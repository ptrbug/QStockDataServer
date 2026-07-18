"""QStockDataServer service, scheduler, and market-data update workflow."""

from __future__ import annotations

import contextlib
import logging
import threading
from datetime import timedelta
from typing import Any

from .config import AppConfig
from .data_fetcher import BaostockDataFetcher
from .db_manager import DuckDBManager, SnapshotManager
from .exceptions import (
    ConfigurationError,
    FatalDataError,
    QStockError,
    StorageError,
    TransientSourceError,
)
from .flight_server import StockFlightServer
from .logging_config import (
    FatalMarkerManager,
    log_context,
    new_run_id,
)
from .strategy_launcher import StrategyProgramLauncher
from .validation import validate_daily_pair


LOGGER = logging.getLogger(__name__)


class StockDataService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.database = DuckDBManager(config)
        self.fetcher = BaostockDataFetcher(config)
        self.snapshots = SnapshotManager()
        self.marker = FatalMarkerManager(config.fatal_marker_path)
        self.strategy_launcher = StrategyProgramLauncher(config.strategy_programs)
        self._flight: StockFlightServer | None = None
        self._scheduler: Any | None = None
        self._update_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._state = "starting"
        self._last_error: str | None = None
        self._exit_code = 0

    def _set_state(self, state: str, error: str | None = None) -> None:
        with self._state_lock:
            self._state = state
            self._last_error = error

    def status(self) -> dict[str, Any]:
        with self._state_lock:
            state = self._state
            error = self._last_error
        return {
            "state": state,
            "last_error": error,
            "disk_last_update_trade_date": self.database.get_meta("last_update_trade_date"),
            "snapshot_last_update_trade_date": self.snapshots.version,
            "database_path": str(self.config.database_path),
            "fatal_marker": self.marker.exists(),
        }

    def initialize(self) -> None:
        self.database.initialize_schema()
        with self.fetcher.session():
            if self.database.needs_initial_import():
                self._initial_import()
                # The fixed initial-import target may have become stale while a
                # long import was running. Catch up before exposing any snapshot.
                self._catch_up()
            else:
                self._catch_up()
        snapshot = self.database.build_snapshot()
        self.snapshots.swap(snapshot)
        self._set_state("ready")

    def _initial_import(self) -> None:
        run_id = new_run_id("initial")
        with log_context(run_id=run_id, stage="initial_import"):
            saved_target = self.database.get_meta("initial_import_target_date")
            target = (
                self.fetcher.fetch_last_trading_date()
                if saved_target is None
                else self._parse_meta_date(saved_target, "initial_import_target_date")
            )
            LOGGER.info("首次导入目标交易日：%s", target)
            stock_list = self.fetcher.fetch_stock_list(target)
            self.database.prepare_initial_import(stock_list, target)
            imported = self.database.imported_symbols(target)
            ordered = stock_list.sort_values("symbol", kind="stable")
            total = len(ordered)
            for index, row in enumerate(ordered.itertuples(index=False), start=1):
                if row.symbol in imported:
                    continue
                with log_context(symbol=row.symbol):
                    LOGGER.info("首次导入 %d/%d", index, total)
                    history = self.fetcher.fetch_stock_history(
                        row.symbol, self.config.start_date, target
                    )
                    rows = self.database.import_symbol_history(history, row.board, target)
                    LOGGER.info("首次导入完成，写入 %d 行", rows)
            self.database.complete_initial_import(set(stock_list["symbol"]), target)
            LOGGER.info("首次导入全部完成，目标交易日=%s，股票数=%d", target, total)

    @staticmethod
    def _parse_meta_date(value: str, key: str) -> Any:
        from datetime import date

        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise FatalDataError(f"meta.{key} 非法：{value!r}") from exc

    def _catch_up(self) -> bool:
        last_update = self.database.get_last_update_date()
        if last_update is None:
            raise FatalDataError("非首次导入状态缺少 last_update_trade_date")
        target = self.fetcher.fetch_last_trading_date()
        if last_update > target:
            raise FatalDataError(
                f"本地日期 {last_update} 晚于 Baostock 当前完整交易日 {target}"
            )
        if last_update == target:
            LOGGER.info("行情已经是最新：%s", target)
            return False

        missing_dates = self.fetcher.fetch_trade_dates(last_update + timedelta(days=1), target)
        if not missing_dates or missing_dates[-1] != target:
            raise FatalDataError(
                f"缺失交易日计算异常：last_update={last_update} target={target} dates={missing_dates}"
            )
        LOGGER.info("需要补齐 %d 个交易日：%s .. %s", len(missing_dates), missing_dates[0], target)
        latest_stock_list = self.fetcher.fetch_stock_list(target)
        known_symbols = self.database.get_stock_symbols()
        new_stocks = latest_stock_list.loc[
            ~latest_stock_list["symbol"].isin(known_symbols)
        ].sort_values("symbol", kind="stable")
        if not new_stocks.empty:
            LOGGER.info("发现 %d 只新增股票，开始逐股历史回补", len(new_stocks))
        new_symbol_histories: list[tuple[Any, Any]] = []
        for row in new_stocks.itertuples(index=False):
            with log_context(stage="new_stock_backfill", symbol=row.symbol):
                history = self.fetcher.fetch_stock_history(
                    row.symbol, self.config.start_date, target
                )
                stock = latest_stock_list.loc[
                    latest_stock_list["symbol"].eq(row.symbol)
                ].copy()
                new_symbol_histories.append((history, stock))
                LOGGER.info("新增股票历史下载并校验完成，行数=%d", len(history))
        for trade_date in missing_dates:
            with log_context(stage="daily_update", trade_date=str(trade_date)):
                daily = self.fetcher.fetch_market_daily(trade_date)
                stock_list = latest_stock_list if trade_date == target else None
                if stock_list is not None:
                    validate_daily_pair(stock_list, daily, trade_date)
                self.database.apply_market_day(
                    stock_list,
                    daily,
                    trade_date,
                    new_symbol_histories=(
                        new_symbol_histories if trade_date == target else None
                    ),
                )
                LOGGER.info("交易日更新完成，行数=%d", len(daily))
        return True

    def _handle_runtime_error(self, error: BaseException, run_id: str) -> None:
        exit_code = error.exit_code if isinstance(error, QStockError) else 5
        self._exit_code = exit_code
        self._set_state("fatal", str(error))
        LOGGER.critical("服务因错误即将终止：%s", error, exc_info=error)
        if exit_code in {4, 5}:
            try:
                self.marker.write(
                    error,
                    exit_code=exit_code,
                    stage="runtime_update",
                    run_id=run_id,
                    log_path=self.config.error_log_path,
                    details={
                        "disk_version": self.database.get_meta("last_update_trade_date"),
                        "snapshot_version": self.snapshots.version,
                    },
                )
            except Exception:
                LOGGER.exception("写入致命错误标记失败")
        if self._scheduler is not None:
            with contextlib.suppress(Exception):
                self._scheduler.shutdown(wait=False)
        if self._flight is not None:
            threading.Thread(
                target=self._flight.shutdown,
                name="fatal-flight-shutdown",
                daemon=True,
            ).start()

    def _update_worker(self, run_id: str, *, lock_already_held: bool = False) -> None:
        acquired = lock_already_held or self._update_lock.acquire(blocking=False)
        if not acquired:
            LOGGER.warning("更新任务正在运行，本次触发跳过")
            return
        try:
            self._set_state("updating")
            with log_context(run_id=run_id, stage="scheduled_update"):
                with self.fetcher.session():
                    changed = self._catch_up()
                if changed:
                    snapshot = self.database.build_snapshot()
                    self.snapshots.swap(snapshot)
                    LOGGER.info("内存快照切换完成，版本=%s", snapshot.version)
                self._set_state("ready")
                if changed:
                    self._launch_strategy_programs(snapshot.version)
        except BaseException as exc:
            if not isinstance(exc, QStockError):
                exc = StorageError(f"未处理的更新异常：{exc}")
            self._handle_runtime_error(exc, run_id)
        finally:
            self._update_lock.release()

    def scheduled_update(self) -> None:
        self._update_worker(new_run_id("schedule"))

    def trigger_update(self) -> bool:
        if not self._update_lock.acquire(blocking=False):
            return False
        run_id = new_run_id("manual")
        thread = threading.Thread(
            target=self._update_worker,
            kwargs={"run_id": run_id, "lock_already_held": True},
            name="manual-stock-update",
            daemon=True,
        )
        thread.start()
        return True

    def _launch_strategy_programs(self, snapshot_version: str | None) -> None:
        if snapshot_version is None or self._flight is None:
            return
        self.strategy_launcher.launch_all(
            flight_port=self._flight.port,
            snapshot_version=snapshot_version,
        )

    def serve(self) -> int:
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
        except ImportError as exc:
            raise ConfigurationError("缺少 APScheduler，请先安装 requirements.txt") from exc

        self._flight = StockFlightServer(
            self.config.flight_host,
            self.config.flight_port,
            self.snapshots,
            max_sql_length=self.config.query_max_sql_length,
            status_provider=self.status,
            trigger_update=self.trigger_update,
        )
        self._launch_strategy_programs(self.snapshots.version)
        self._scheduler = BackgroundScheduler(timezone=self.config.timezone)
        self._scheduler.add_job(
            self.scheduled_update,
            "cron",
            hour=self.config.update_time.hour,
            minute=self.config.update_time.minute,
            second=self.config.update_time.second,
            id="daily_market_update",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        self._scheduler.start()
        LOGGER.info(
            "Arrow Flight 已启动 grpc://%s:%d，快照版本=%s",
            self.config.flight_host,
            self._flight.port,
            self.snapshots.version,
        )
        try:
            self._flight.serve()
        except KeyboardInterrupt:
            LOGGER.info("收到键盘中断，正在停止服务")
            self._exit_code = 0
        finally:
            self.stop()
        return self._exit_code

    def stop(self) -> None:
        self._set_state("stopped", self._last_error)
        if self._scheduler is not None:
            with contextlib.suppress(Exception):
                self._scheduler.shutdown(wait=False)
            self._scheduler = None
        if self._flight is not None:
            with contextlib.suppress(Exception):
                self._flight.shutdown()
            with contextlib.suppress(Exception):
                self._flight.close()
            self._flight = None
        self.snapshots.close()
        self.fetcher.logout()
