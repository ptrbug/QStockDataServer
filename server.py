"""QStockDataServer service, scheduler, initial import, and administrative CLI."""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import sys
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any

from config import AppConfig, load_config
from data_fetcher import BaostockDataFetcher
from db_manager import DuckDBManager, SnapshotManager
from exceptions import (
    ConfigurationError,
    FatalDataError,
    QStockError,
    StorageError,
    TransientSourceError,
)
from flight_server import StockFlightServer
from logging_config import (
    FatalMarkerManager,
    log_context,
    new_run_id,
    setup_logging,
    shutdown_logging,
)
from validation import validate_daily_pair


LOGGER = logging.getLogger(__name__)


class ProcessLock:
    """Cross-platform non-blocking single-process lock."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._stream: Any | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        stream = self.path.open("a+b")
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            stream.close()
            raise ConfigurationError(f"已有 QStockDataServer 进程持有锁：{self.path}") from exc
        stream.seek(0)
        stream.truncate()
        stream.write(str(os.getpid()).encode("ascii"))
        stream.flush()
        self._stream = stream

    def release(self) -> None:
        if self._stream is None:
            return
        stream = self._stream
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()
            self._stream = None

    def __enter__(self) -> "ProcessLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()


class StockDataService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.database = DuckDBManager(config)
        self.fetcher = BaostockDataFetcher(config)
        self.snapshots = SnapshotManager()
        self.marker = FatalMarkerManager(config.fatal_marker_path)
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
        for trade_date in missing_dates:
            with log_context(stage="daily_update", trade_date=str(trade_date)):
                stock_list = self.fetcher.fetch_stock_list(trade_date)
                daily = self.fetcher.fetch_market_daily(trade_date)
                validate_daily_pair(stock_list, daily, trade_date)
                self.database.apply_market_day(stock_list, daily, trade_date)
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="QStockDataServer")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command, help_text in (
        ("serve", "启动常驻数据服务"),
        ("doctor", "只读检查数据库完整性"),
        ("clear-fatal", "检查通过后清除致命错误标记"),
    ):
        sub = subparsers.add_parser(command, help=help_text)
        sub.add_argument("--config", default="config.yaml", help="配置文件路径")
        if command == "clear-fatal":
            sub.add_argument("--confirm", action="store_true", help="确认清除标记")
    return parser


def _print_existing_marker(marker: FatalMarkerManager) -> int:
    payload = marker.read()
    print("检测到致命错误标记，服务拒绝启动：", file=sys.stderr)
    print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
    print(
        "请先运行 doctor，确认后执行 clear-fatal --confirm。",
        file=sys.stderr,
    )
    try:
        return int(payload.get("exit_code", 4))
    except (TypeError, ValueError):
        return 4


def run_command(args: argparse.Namespace, config: AppConfig) -> int:
    marker = FatalMarkerManager(config.fatal_marker_path)
    database = DuckDBManager(config)
    if args.command == "doctor":
        report = database.doctor()
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 0
    if args.command == "clear-fatal":
        if not args.confirm:
            raise ConfigurationError("clear-fatal 必须显式提供 --confirm")
        report = database.doctor()
        marker.clear()
        print(json.dumps({"fatal_marker_cleared": True, "doctor": report}, ensure_ascii=False, indent=2, default=str))
        return 0
    if marker.exists():
        return _print_existing_marker(marker)

    with ProcessLock(config.process_lock_path):
        service = StockDataService(config)
        try:
            service.initialize()
            return service.serve()
        except BaseException:
            service.stop()
            raise


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
    except QStockError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
        return exc.exit_code

    setup_logging(config)
    run_id = new_run_id(args.command)
    try:
        with log_context(run_id=run_id, stage=args.command):
            return run_command(args, config)
    except QStockError as exc:
        LOGGER.critical("命令失败：%s", exc, exc_info=True)
        if args.command == "serve" and exc.exit_code in {4, 5}:
            marker = FatalMarkerManager(config.fatal_marker_path)
            if not marker.exists():
                with contextlib.suppress(Exception):
                    marker.write(
                        exc,
                        exit_code=exc.exit_code,
                        stage="startup",
                        run_id=run_id,
                        log_path=config.error_log_path,
                    )
        return exc.exit_code
    except KeyboardInterrupt:
        LOGGER.info("用户中止")
        return 0
    except Exception as exc:
        wrapped = StorageError(f"未处理异常：{exc}")
        LOGGER.critical("未处理异常", exc_info=True)
        if args.command == "serve":
            with contextlib.suppress(Exception):
                FatalMarkerManager(config.fatal_marker_path).write(
                    wrapped,
                    exit_code=wrapped.exit_code,
                    stage="startup",
                    run_id=run_id,
                    log_path=config.error_log_path,
                )
        return wrapped.exit_code
    finally:
        shutdown_logging()


if __name__ == "__main__":
    raise SystemExit(main())
