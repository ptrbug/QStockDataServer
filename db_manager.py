"""DuckDB persistence, qfq factor transactions, snapshots, and diagnostics."""

from __future__ import annotations

import contextlib
import json
import logging
import math
import threading
import uuid
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterator

import duckdb
import pandas as pd

from config import AppConfig
from exceptions import ConfigurationError, FatalDataError, StorageError
from validation import calculate_qfq_factors


LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = "1"
DAILY_TABLES = {"main": "main_board_daily", "gem": "gem_board_daily"}
STOCK_TABLES = {"main": "main_board_stock_list", "gem": "gem_board_stock_list"}
SNAPSHOT_TABLES = [
    "main_board_daily",
    "gem_board_daily",
    "main_board_stock_list",
    "gem_board_stock_list",
    "adjustment_events",
    "meta",
]
DAILY_SNAPSHOT_COLUMNS = (
    "symbol, date, open, high, low, close, preclose, volume, amount, "
    "trade_status, qfq_factor"
)


DDL = """
CREATE TABLE IF NOT EXISTS main_board_daily (
    symbol         VARCHAR NOT NULL,
    date           DATE NOT NULL,
    open           DOUBLE NOT NULL,
    high           DOUBLE NOT NULL,
    low            DOUBLE NOT NULL,
    close          DOUBLE NOT NULL,
    preclose       DOUBLE NOT NULL,
    volume         BIGINT NOT NULL,
    amount         DOUBLE NOT NULL,
    trade_status   TINYINT NOT NULL,
    qfq_factor     DOUBLE NOT NULL DEFAULT 1.0,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS gem_board_daily (
    symbol         VARCHAR NOT NULL,
    date           DATE NOT NULL,
    open           DOUBLE NOT NULL,
    high           DOUBLE NOT NULL,
    low            DOUBLE NOT NULL,
    close          DOUBLE NOT NULL,
    preclose       DOUBLE NOT NULL,
    volume         BIGINT NOT NULL,
    amount         DOUBLE NOT NULL,
    trade_status   TINYINT NOT NULL,
    qfq_factor     DOUBLE NOT NULL DEFAULT 1.0,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS main_board_stock_list (
    symbol                 VARCHAR PRIMARY KEY,
    name                   VARCHAR NOT NULL,
    ipo_date               DATE,
    out_date               DATE,
    listing_status         VARCHAR NOT NULL,
    trade_status           TINYINT NOT NULL,
    first_seen_trade_date  DATE NOT NULL,
    last_seen_trade_date   DATE NOT NULL,
    updated_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gem_board_stock_list (
    symbol                 VARCHAR PRIMARY KEY,
    name                   VARCHAR NOT NULL,
    ipo_date               DATE,
    out_date               DATE,
    listing_status         VARCHAR NOT NULL,
    trade_status           TINYINT NOT NULL,
    first_seen_trade_date  DATE NOT NULL,
    last_seen_trade_date   DATE NOT NULL,
    updated_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS adjustment_events (
    symbol          VARCHAR NOT NULL,
    ex_date         DATE NOT NULL,
    previous_close  DOUBLE NOT NULL,
    preclose        DOUBLE NOT NULL,
    event_factor    DOUBLE NOT NULL,
    detected_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, ex_date)
);

CREATE TABLE IF NOT EXISTS meta (
    key         VARCHAR PRIMARY KEY,
    value       VARCHAR,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS initial_import_progress (
    symbol             VARCHAR PRIMARY KEY,
    board              VARCHAR NOT NULL,
    target_trade_date  DATE NOT NULL,
    status             VARCHAR NOT NULL,
    row_count          BIGINT NOT NULL,
    updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


@dataclass(slots=True)
class MemorySnapshot:
    connection: duckdb.DuckDBPyConnection
    version: str
    max_rows: int
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _closed: bool = False

    def query(self, sql: str) -> Any:
        with self._lock:
            if self._closed:
                raise StorageError("内存快照已经关闭")
            try:
                table = self.connection.execute(sql).to_arrow_table()
            except Exception as exc:
                raise ValueError(f"SQL 执行失败：{exc}") from exc
            if table.num_rows > self.max_rows:
                raise ValueError(
                    f"查询返回 {table.num_rows} 行，超过 query_max_rows={self.max_rows}"
                )
            return table

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self.connection.close()
                self._closed = True


class SnapshotManager:
    """Reference-counted atomic snapshot swap."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._current: MemorySnapshot | None = None
        self._active: dict[int, int] = {}
        self._retired: dict[int, MemorySnapshot] = {}

    @contextlib.contextmanager
    def acquire(self) -> Iterator[MemorySnapshot]:
        with self._condition:
            snapshot = self._current
            if snapshot is None:
                raise StorageError("查询快照尚未就绪")
            key = id(snapshot)
            self._active[key] = self._active.get(key, 0) + 1
        try:
            yield snapshot
        finally:
            close_snapshot: MemorySnapshot | None = None
            with self._condition:
                self._active[key] -= 1
                if self._active[key] == 0:
                    del self._active[key]
                    close_snapshot = self._retired.pop(key, None)
                self._condition.notify_all()
            if close_snapshot is not None:
                close_snapshot.close()

    def swap(self, snapshot: MemorySnapshot) -> None:
        close_snapshot: MemorySnapshot | None = None
        with self._condition:
            old = self._current
            self._current = snapshot
            if old is not None:
                old_key = id(old)
                if self._active.get(old_key, 0) == 0:
                    close_snapshot = old
                else:
                    self._retired[old_key] = old
        if close_snapshot is not None:
            close_snapshot.close()

    @property
    def version(self) -> str | None:
        with self._condition:
            return self._current.version if self._current else None

    def close(self) -> None:
        with self._condition:
            snapshots = [value for value in [self._current, *self._retired.values()] if value]
            self._current = None
            self._retired.clear()
        for snapshot in snapshots:
            snapshot.close()


class DuckDBManager:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.path = config.database_path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        try:
            return duckdb.connect(
                str(self.path),
                read_only=read_only,
                config={"threads": str(self.config.duckdb_threads)},
            )
        except Exception as exc:
            raise StorageError(f"无法打开 DuckDB {self.path}: {exc}") from exc

    def initialize_schema(self) -> None:
        with self.connect() as connection:
            try:
                connection.execute("BEGIN TRANSACTION")
                existing_tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='main'"
                    ).fetchall()
                }
                if existing_tables:
                    if "meta" not in existing_tables:
                        raise ConfigurationError(
                            "数据库已包含表但缺少 meta，拒绝覆盖未知 schema"
                        )
                    version_row = connection.execute(
                        "SELECT value FROM meta WHERE key='schema_version'"
                    ).fetchone()
                    if not version_row:
                        raise ConfigurationError(
                            "已有数据库缺少 meta.schema_version，拒绝自动迁移"
                        )
                    if version_row[0] != SCHEMA_VERSION:
                        raise ConfigurationError(
                            f"不支持的数据库 schema_version={version_row[0]!r}，"
                            f"程序要求 {SCHEMA_VERSION!r}"
                        )
                connection.execute(DDL)
                self._set_meta(connection, "schema_version", SCHEMA_VERSION)
                self._set_meta(connection, "price_mode", "raw")
                self._set_meta(connection, "qfq_algorithm", "preclose_ratio")
                connection.execute("COMMIT")
            except Exception as exc:
                with contextlib.suppress(Exception):
                    connection.execute("ROLLBACK")
                if isinstance(exc, (FatalDataError, ConfigurationError, StorageError)):
                    raise
                raise StorageError(f"初始化 DuckDB schema 失败：{exc}") from exc

    @staticmethod
    def _set_meta(connection: duckdb.DuckDBPyConnection, key: str, value: str) -> None:
        connection.execute(
            """
            INSERT INTO meta(key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE
            SET value = excluded.value, updated_at = now()
            """,
            [key, value],
        )

    def get_meta(self, key: str) -> str | None:
        with self.connect(read_only=True) as connection:
            try:
                row = connection.execute("SELECT value FROM meta WHERE key = ?", [key]).fetchone()
            except Exception as exc:
                raise StorageError(f"读取 meta.{key} 失败：{exc}") from exc
        return None if row is None else row[0]

    def get_last_update_date(self) -> date | None:
        value = self.get_meta("last_update_trade_date")
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise FatalDataError(f"meta.last_update_trade_date 非法：{value!r}") from exc

    def needs_initial_import(self) -> bool:
        completed = self.get_meta("initial_import_completed") == "true"
        if not completed:
            return True
        with self.connect(read_only=True) as connection:
            count = connection.execute(
                "SELECT (SELECT count(*) FROM main_board_daily) + "
                "(SELECT count(*) FROM gem_board_daily)"
            ).fetchone()[0]
        if count == 0:
            raise FatalDataError("initial_import_completed=true，但日线正式表为空")
        return False

    @contextlib.contextmanager
    def _registered(
        self, connection: duckdb.DuckDBPyConnection, frame: pd.DataFrame
    ) -> Iterator[str]:
        name = f"stage_{uuid.uuid4().hex}"
        connection.register(name, frame)
        try:
            yield name
        finally:
            with contextlib.suppress(Exception):
                connection.unregister(name)

    def _upsert_stock_list(
        self, connection: duckdb.DuckDBPyConnection, frame: pd.DataFrame, trade_date: date
    ) -> None:
        for board, table in STOCK_TABLES.items():
            subset = frame.loc[frame["board"].eq(board), ["symbol", "name", "trade_status"]].copy()
            if subset.empty:
                continue
            with self._registered(connection, subset) as stage:
                connection.execute(
                    f"""
                    INSERT INTO {table} AS target (
                        symbol, name, ipo_date, out_date, listing_status,
                        trade_status, first_seen_trade_date, last_seen_trade_date, updated_at
                    )
                    SELECT symbol, name, NULL, NULL, 'active', trade_status,
                           CAST(? AS DATE), CAST(? AS DATE), CURRENT_TIMESTAMP
                    FROM {stage}
                    ON CONFLICT(symbol) DO UPDATE SET
                        name = excluded.name,
                        listing_status = 'active',
                        trade_status = excluded.trade_status,
                        last_seen_trade_date = excluded.last_seen_trade_date,
                        updated_at = now()
                    """,
                    [trade_date, trade_date],
                )

    def prepare_initial_import(self, stock_list: pd.DataFrame, target_date: date) -> None:
        with self.connect() as connection:
            try:
                connection.execute("BEGIN TRANSACTION")
                self._upsert_stock_list(connection, stock_list, target_date)
                existing_target = connection.execute(
                    "SELECT value FROM meta WHERE key='initial_import_target_date'"
                ).fetchone()
                if existing_target and existing_target[0] != target_date.isoformat():
                    raise FatalDataError(
                        "未完成的首次导入目标日期与本次不一致："
                        f"{existing_target[0]} != {target_date}"
                    )
                self._set_meta(connection, "initial_import_target_date", target_date.isoformat())
                self._set_meta(connection, "initial_import_completed", "false")
                connection.execute("COMMIT")
            except Exception as exc:
                with contextlib.suppress(Exception):
                    connection.execute("ROLLBACK")
                if isinstance(exc, FatalDataError):
                    raise
                raise StorageError(f"准备首次导入失败：{exc}") from exc

    def imported_symbols(self, target_date: date) -> set[str]:
        with self.connect(read_only=True) as connection:
            rows = connection.execute(
                """
                SELECT symbol FROM initial_import_progress
                WHERE target_trade_date = ? AND status = 'completed'
                """,
                [target_date],
            ).fetchall()
        return {row[0] for row in rows}

    def get_stock_symbols(self) -> set[str]:
        with self.connect(read_only=True) as connection:
            rows = connection.execute(
                "SELECT symbol FROM main_board_stock_list "
                "UNION SELECT symbol FROM gem_board_stock_list"
            ).fetchall()
        return {row[0] for row in rows}

    def import_symbol_history(
        self, frame: pd.DataFrame, board: str, target_date: date
    ) -> int:
        if board not in DAILY_TABLES:
            raise ConfigurationError(f"未知板块：{board}")
        adjusted, events = calculate_qfq_factors(frame, self.config.factor_epsilon)
        symbol = str(adjusted.iloc[0]["symbol"])
        table = DAILY_TABLES[board]
        daily_columns = [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "trade_status",
            "qfq_factor",
        ]
        with self.connect() as connection:
            try:
                connection.execute("BEGIN TRANSACTION")
                completed = connection.execute(
                    """
                    SELECT 1 FROM initial_import_progress
                    WHERE symbol=? AND target_trade_date=? AND status='completed'
                    """,
                    [symbol, target_date],
                ).fetchone()
                if completed:
                    connection.execute("ROLLBACK")
                    return 0
                connection.execute(f"DELETE FROM {table} WHERE symbol=?", [symbol])
                connection.execute("DELETE FROM adjustment_events WHERE symbol=?", [symbol])
                with self._registered(connection, adjusted[daily_columns]) as stage:
                    connection.execute(
                        f"""
                        INSERT INTO {table}({', '.join(daily_columns)})
                        SELECT {', '.join(daily_columns)} FROM {stage}
                        """
                    )
                if not events.empty:
                    with self._registered(connection, events) as event_stage:
                        connection.execute(
                            f"""
                            INSERT INTO adjustment_events(
                                symbol, ex_date, previous_close, preclose, event_factor
                            )
                            SELECT symbol, ex_date, previous_close, preclose, event_factor
                            FROM {event_stage}
                            """
                        )
                connection.execute(
                    """
                    INSERT INTO initial_import_progress(
                        symbol, board, target_trade_date, status, row_count, updated_at
                    ) VALUES (?, ?, ?, 'completed', ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(symbol) DO UPDATE SET
                        board=excluded.board,
                        target_trade_date=excluded.target_trade_date,
                        status='completed',
                        row_count=excluded.row_count,
                        updated_at=now()
                    """,
                    [symbol, board, target_date, len(adjusted)],
                )
                connection.execute("COMMIT")
                return len(adjusted)
            except Exception as exc:
                with contextlib.suppress(Exception):
                    connection.execute("ROLLBACK")
                if isinstance(exc, FatalDataError):
                    raise
                raise StorageError(f"导入 {symbol} 历史行情失败：{exc}") from exc

    def _replace_new_symbol_history(
        self,
        connection: duckdb.DuckDBPyConnection,
        frame: pd.DataFrame,
        stock: pd.DataFrame,
        target_date: date,
    ) -> int:
        if len(stock) != 1:
            raise FatalDataError("新增股票回补必须且只能提供一条证券信息")
        symbol = str(stock.iloc[0]["symbol"])
        board = str(stock.iloc[0]["board"])
        if board not in DAILY_TABLES:
            raise ConfigurationError(f"未知板块：{board}")
        adjusted, events = calculate_qfq_factors(frame, self.config.factor_epsilon)
        if not adjusted["symbol"].eq(symbol).all():
            raise FatalDataError(f"新增股票 {symbol} 的证券信息与历史行情代码不一致")
        table = DAILY_TABLES[board]
        daily_columns = [
            "symbol", "date", "open", "high", "low", "close", "preclose",
            "volume", "amount", "trade_status", "qfq_factor",
        ]
        connection.execute("DELETE FROM main_board_daily WHERE symbol=?", [symbol])
        connection.execute("DELETE FROM gem_board_daily WHERE symbol=?", [symbol])
        connection.execute("DELETE FROM adjustment_events WHERE symbol=?", [symbol])
        self._upsert_stock_list(connection, stock, target_date)
        with self._registered(connection, adjusted[daily_columns]) as stage:
            connection.execute(
                f"INSERT INTO {table}({', '.join(daily_columns)}) "
                f"SELECT {', '.join(daily_columns)} FROM {stage}"
            )
        if not events.empty:
            with self._registered(connection, events) as event_stage:
                connection.execute(
                    """
                    INSERT INTO adjustment_events(
                        symbol, ex_date, previous_close, preclose, event_factor
                    )
                    SELECT symbol, ex_date, previous_close, preclose, event_factor
                    FROM {event_stage}
                    """.format(event_stage=event_stage)
                )
        return len(adjusted)

    def complete_initial_import(self, expected_symbols: set[str], target_date: date) -> None:
        with self.connect() as connection:
            try:
                connection.execute("BEGIN TRANSACTION")
                completed = {
                    row[0]
                    for row in connection.execute(
                        """
                        SELECT symbol FROM initial_import_progress
                        WHERE target_trade_date=? AND status='completed'
                        """,
                        [target_date],
                    ).fetchall()
                }
                if completed != expected_symbols:
                    raise FatalDataError(
                        "首次导入股票集合不完整，"
                        f"缺少={sorted(expected_symbols-completed)[:20]}，"
                        f"多出={sorted(completed-expected_symbols)[:20]}"
                    )
                self._set_meta(connection, "last_update_trade_date", target_date.isoformat())
                self._set_meta(connection, "initial_import_completed", "true")
                self._set_meta(connection, "stock_list_last_update_date", target_date.isoformat())
                connection.execute("COMMIT")
            except Exception as exc:
                with contextlib.suppress(Exception):
                    connection.execute("ROLLBACK")
                if isinstance(exc, FatalDataError):
                    raise
                raise StorageError(f"完成首次导入失败：{exc}") from exc

    def _previous_prices(
        self, connection: duckdb.DuckDBPyConnection, trade_date: date
    ) -> pd.DataFrame:
        return connection.execute(
            """
            SELECT symbol, arg_max(close, date) AS previous_close
            FROM (
                SELECT symbol, date, close FROM main_board_daily WHERE date < ?
                UNION ALL
                SELECT symbol, date, close FROM gem_board_daily WHERE date < ?
            ) history
            GROUP BY symbol
            """,
            [trade_date, trade_date],
        ).fetchdf()

    def _existing_daily_symbols(
        self,
        connection: duckdb.DuckDBPyConnection,
        daily: pd.DataFrame,
        trade_date: date,
    ) -> set[str]:
        existing = connection.execute(
            """
            SELECT symbol, date, open, high, low, close, preclose, volume, amount,
                   trade_status, 'main' AS board
            FROM main_board_daily WHERE date=?
            UNION ALL
            SELECT symbol, date, open, high, low, close, preclose, volume, amount,
                   trade_status, 'gem' AS board
            FROM gem_board_daily WHERE date=?
            """,
            [trade_date, trade_date],
        ).fetchdf()
        expected_symbols = set(daily["symbol"])
        existing = existing.loc[existing["symbol"].isin(expected_symbols)].copy()
        if existing.empty:
            return set()
        if existing["symbol"].duplicated().any():
            raise FatalDataError(f"{trade_date} 已有日线出现跨表或表内重复代码")
        incoming = daily.set_index("symbol")
        for row in existing.itertuples(index=False):
            source = incoming.loc[row.symbol]
            for column in ("open", "high", "low", "close", "preclose", "amount"):
                if not math.isclose(
                    float(getattr(row, column)),
                    float(source[column]),
                    rel_tol=self.config.factor_epsilon,
                    abs_tol=self.config.factor_epsilon,
                ):
                    raise FatalDataError(
                        f"{row.symbol} {trade_date} 已回补日线的 {column} 与全市场日线不一致"
                    )
            for column in ("volume", "trade_status", "board"):
                if getattr(row, column) != source[column]:
                    raise FatalDataError(
                        f"{row.symbol} {trade_date} 已回补日线的 {column} 与全市场日线不一致"
                    )
        return set(existing["symbol"])

    def apply_market_day(
        self,
        stock_list: pd.DataFrame | None,
        daily: pd.DataFrame,
        trade_date: date,
        *,
        new_symbol_histories: list[tuple[pd.DataFrame, pd.DataFrame]] | None = None,
    ) -> None:
        if not daily["date"].eq(trade_date).all():
            raise FatalDataError("apply_market_day 收到其他日期行情")
        with self.connect() as connection:
            try:
                connection.execute("BEGIN TRANSACTION")
                last_row = connection.execute(
                    "SELECT value FROM meta WHERE key='last_update_trade_date'"
                ).fetchone()
                if last_row and date.fromisoformat(last_row[0]) >= trade_date:
                    raise FatalDataError(
                        f"拒绝乱序或重复推进：last_update={last_row[0]} target={trade_date}"
                    )
                if new_symbol_histories and stock_list is None:
                    raise FatalDataError("新增股票历史只能随最新证券列表在目标日提交")
                for history, stock in new_symbol_histories or []:
                    self._replace_new_symbol_history(
                        connection, history, stock, trade_date
                    )
                existing_symbols = self._existing_daily_symbols(
                    connection, daily, trade_date
                )
                pending = daily.loc[~daily["symbol"].isin(existing_symbols)].copy()
                previous = self._previous_prices(connection, trade_date)
                work = pending.merge(previous, on="symbol", how="left", validate="one_to_one")
                if stock_list is not None:
                    self._upsert_stock_list(connection, stock_list, trade_date)

                events: list[dict[str, Any]] = []
                for row in work.itertuples(index=False):
                    previous_close = getattr(row, "previous_close")
                    if pd.isna(previous_close):
                        # A symbol first appearing inside a long catch-up window has no
                        # local prior close. Its first downloaded row starts at factor 1.
                        continue
                    ratio = float(row.preclose) / float(previous_close)
                    if math.isclose(
                        ratio,
                        1.0,
                        rel_tol=self.config.factor_epsilon,
                        abs_tol=self.config.factor_epsilon,
                    ):
                        continue
                    existing = connection.execute(
                        """
                        SELECT previous_close, preclose, event_factor
                        FROM adjustment_events WHERE symbol=? AND ex_date=?
                        """,
                        [row.symbol, trade_date],
                    ).fetchone()
                    if existing is not None:
                        raise FatalDataError(
                            f"{row.symbol} {trade_date} 调整事件已存在但当日日线缺失"
                        )
                    events.append(
                        {
                            "symbol": row.symbol,
                            "board": row.board,
                            "previous_close": float(previous_close),
                            "preclose": float(row.preclose),
                            "event_factor": ratio,
                        }
                    )

                for event in events:
                    table = DAILY_TABLES[event["board"]]
                    connection.execute(
                        f"UPDATE {table} SET qfq_factor=qfq_factor*? "
                        "WHERE symbol=? AND date<?",
                        [event["event_factor"], event["symbol"], trade_date],
                    )
                    connection.execute(
                        """
                        INSERT INTO adjustment_events(
                            symbol, ex_date, previous_close, preclose, event_factor
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        [
                            event["symbol"],
                            trade_date,
                            event["previous_close"],
                            event["preclose"],
                            event["event_factor"],
                        ],
                    )

                insert_columns = [
                    "symbol",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "preclose",
                    "volume",
                    "amount",
                    "trade_status",
                    "qfq_factor",
                ]
                for board, table in DAILY_TABLES.items():
                    subset = pending.loc[
                        pending["board"].eq(board),
                        [
                            "symbol",
                            "date",
                            "open",
                            "high",
                            "low",
                            "close",
                            "preclose",
                            "volume",
                            "amount",
                            "trade_status",
                        ],
                    ].copy()
                    if subset.empty:
                        continue
                    subset["qfq_factor"] = 1.0
                    with self._registered(connection, subset[insert_columns]) as stage:
                        connection.execute(
                            f"INSERT INTO {table}({', '.join(insert_columns)}) "
                            f"SELECT {', '.join(insert_columns)} FROM {stage}"
                        )

                stored_symbols = self._existing_daily_symbols(connection, daily, trade_date)
                if stored_symbols != set(daily["symbol"]):
                    raise FatalDataError(
                        f"{trade_date} 正式表覆盖不完整，"
                        f"缺少={sorted(set(daily['symbol'])-stored_symbols)[:20]}"
                    )

                # Verify adjusted close continuity after all factor updates.
                continuity = connection.execute(
                    """
                    WITH history AS (
                        SELECT symbol, date, close, preclose, qfq_factor FROM main_board_daily
                        UNION ALL
                        SELECT symbol, date, close, preclose, qfq_factor FROM gem_board_daily
                    ), latest AS (
                        SELECT symbol, arg_max(close*qfq_factor, date) AS adjusted_previous
                        FROM history WHERE date < ? GROUP BY symbol
                    )
                    SELECT h.symbol, l.adjusted_previous, h.preclose*h.qfq_factor AS adjusted_preclose
                    FROM history h JOIN latest l USING(symbol)
                    WHERE h.date=?
                    """,
                    [trade_date, trade_date],
                ).fetchall()
                tolerance = max(self.config.factor_epsilon * 10, 1.0e-9)
                for symbol, adjusted_previous, adjusted_preclose in continuity:
                    scale = max(abs(adjusted_previous), abs(adjusted_preclose), 1.0)
                    if abs(adjusted_previous - adjusted_preclose) > tolerance * scale:
                        raise FatalDataError(f"{symbol} {trade_date} 前复权连续性校验失败")

                self._set_meta(connection, "last_update_trade_date", trade_date.isoformat())
                if stock_list is not None:
                    self._set_meta(
                        connection, "stock_list_last_update_date", trade_date.isoformat()
                    )
                connection.execute("COMMIT")
            except Exception as exc:
                with contextlib.suppress(Exception):
                    connection.execute("ROLLBACK")
                if isinstance(exc, FatalDataError):
                    raise
                raise StorageError(f"提交 {trade_date} 增量行情失败：{exc}") from exc

    def build_snapshot(self) -> MemorySnapshot:
        try:
            import pyarrow  # noqa: F401
        except ImportError as exc:
            raise ConfigurationError("缺少 pyarrow，无法构建内存快照") from exc

        memory: duckdb.DuckDBPyConnection | None = None
        try:
            with self.connect(read_only=True) as source:
                tables = {
                    name: source.execute(f"SELECT * FROM {name}").to_arrow_table()
                    for name in SNAPSHOT_TABLES
                }
            memory = duckdb.connect(
                ":memory:",
                config={
                    "threads": str(self.config.duckdb_threads),
                    "enable_external_access": "false",
                },
            )
            for name, table in tables.items():
                stage = f"source_{name}"
                memory.register(stage, table)
                if name in DAILY_TABLES.values():
                    storage = f"_{name}_snapshot"
                    memory.execute(
                        f"""
                        CREATE TABLE {storage} AS
                        SELECT *,
                               open*qfq_factor AS _qfq_open,
                               high*qfq_factor AS _qfq_high,
                               low*qfq_factor AS _qfq_low,
                               close*qfq_factor AS _qfq_close
                        FROM {stage}
                        """
                    )
                    memory.execute(
                        f"CREATE VIEW {name} AS "
                        f"SELECT {DAILY_SNAPSHOT_COLUMNS} FROM {storage}"
                    )
                else:
                    memory.execute(f"CREATE TABLE {name} AS SELECT * FROM {stage}")
                memory.unregister(stage)
            memory.execute(
                """
                CREATE VIEW main_board_daily_qfq AS
                SELECT symbol, date,
                       _qfq_open AS open, _qfq_high AS high,
                       _qfq_low AS low, _qfq_close AS close,
                       volume, amount, trade_status
                FROM _main_board_daily_snapshot
                """
            )
            memory.execute(
                """
                CREATE VIEW gem_board_daily_qfq AS
                SELECT symbol, date,
                       _qfq_open AS open, _qfq_high AS high,
                       _qfq_low AS low, _qfq_close AS close,
                       volume, amount, trade_status
                FROM _gem_board_daily_snapshot
                """
            )
            meta_rows = memory.execute("SELECT key, value FROM meta").fetchall()
            meta = dict(meta_rows)
            version = meta.get("last_update_trade_date")
            if not version:
                raise FatalDataError("内存快照缺少 last_update_trade_date")
            maximum = memory.execute(
                """
                SELECT max(date) FROM (
                    SELECT date FROM main_board_daily
                    UNION ALL SELECT date FROM gem_board_daily
                )
                """
            ).fetchone()[0]
            if maximum != date.fromisoformat(version):
                raise FatalDataError(f"快照最大日期 {maximum} 与 meta {version} 不一致")
            return MemorySnapshot(memory, version, self.config.query_max_rows)
        except (FatalDataError, ConfigurationError):
            if memory is not None:
                memory.close()
            raise
        except Exception as exc:
            if memory is not None:
                memory.close()
            raise StorageError(f"构建内存快照失败：{exc}") from exc

    def doctor(self) -> dict[str, Any]:
        report: dict[str, Any] = {"database": str(self.path), "checks": {}}
        try:
            with self.connect(read_only=True) as connection:
                present = {
                    row[0]
                    for row in connection.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                    ).fetchall()
                }
                required = set(SNAPSHOT_TABLES) | {"initial_import_progress"}
                missing = sorted(required - present)
                if missing:
                    raise FatalDataError(f"DuckDB 缺少表：{missing}")
                report["checks"]["schema"] = "ok"

                invalid = connection.execute(
                    """
                    SELECT count(*) FROM (
                        SELECT * FROM main_board_daily
                        UNION ALL SELECT * FROM gem_board_daily
                    ) d
                    WHERE symbol IS NULL OR date IS NULL OR open<=0 OR high<=0 OR low<=0
                       OR close<=0 OR preclose<=0 OR volume<0 OR amount<0
                       OR qfq_factor<=0 OR trade_status NOT IN (0,1)
                       OR high < greatest(open, low, close)
                       OR low > least(open, high, close)
                    """
                ).fetchone()[0]
                if invalid:
                    raise FatalDataError(f"正式表存在 {invalid} 条非法行情")
                report["checks"]["values"] = "ok"

                continuity_errors = connection.execute(
                    """
                    WITH history AS (
                        SELECT symbol, date, close*qfq_factor AS adjusted_close,
                               preclose*qfq_factor AS adjusted_preclose
                        FROM main_board_daily
                        UNION ALL
                        SELECT symbol, date, close*qfq_factor AS adjusted_close,
                               preclose*qfq_factor AS adjusted_preclose
                        FROM gem_board_daily
                    ), linked AS (
                        SELECT *, lag(adjusted_close) OVER(PARTITION BY symbol ORDER BY date) AS prior
                        FROM history
                    )
                    SELECT count(*) FROM linked
                    WHERE prior IS NOT NULL
                      AND abs(prior-adjusted_preclose) >
                          ? * greatest(abs(prior), abs(adjusted_preclose), 1.0)
                    """,
                    [max(self.config.factor_epsilon * 10, 1.0e-9)],
                ).fetchone()[0]
                if continuity_errors:
                    raise FatalDataError(f"存在 {continuity_errors} 条前复权连续性错误")
                report["checks"]["qfq_continuity"] = "ok"

                initial_completed_row = connection.execute(
                    "SELECT value FROM meta WHERE key='initial_import_completed'"
                ).fetchone()
                initial_completed = bool(
                    initial_completed_row and initial_completed_row[0] == "true"
                )
                maximum = connection.execute(
                    """
                    SELECT max(date) FROM (
                        SELECT date FROM main_board_daily
                        UNION ALL SELECT date FROM gem_board_daily
                    )
                    """
                ).fetchone()[0]
                if initial_completed:
                    last_update_row = connection.execute(
                        "SELECT value FROM meta WHERE key='last_update_trade_date'"
                    ).fetchone()
                    if not last_update_row:
                        raise FatalDataError("meta 缺少 last_update_trade_date")
                    if maximum != date.fromisoformat(last_update_row[0]):
                        raise FatalDataError(
                            f"正式表最大日期 {maximum} 与 meta {last_update_row[0]} 不一致"
                        )
                    report["checks"]["last_update_trade_date"] = last_update_row[0]
                else:
                    report["checks"]["initial_import"] = "incomplete_but_consistent"
                    report["completed_import_symbols"] = connection.execute(
                        "SELECT count(*) FROM initial_import_progress WHERE status='completed'"
                    ).fetchone()[0]
                report["row_count"] = connection.execute(
                    "SELECT (SELECT count(*) FROM main_board_daily) + "
                    "(SELECT count(*) FROM gem_board_daily)"
                ).fetchone()[0]
        except FatalDataError:
            raise
        except Exception as exc:
            raise StorageError(f"doctor 检查失败：{exc}") from exc
        report["status"] = "ok"
        return report

    def dump_status(self) -> str:
        payload = {
            "database_path": str(self.path),
            "last_update_trade_date": self.get_meta("last_update_trade_date"),
            "initial_import_completed": self.get_meta("initial_import_completed"),
            "schema_version": self.get_meta("schema_version"),
        }
        return json.dumps(payload, ensure_ascii=False)
