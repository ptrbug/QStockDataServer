"""Read-only custom Arrow Flight protocol for SQL queries and service actions."""

from __future__ import annotations

import json
import logging
import re
import threading
from collections.abc import Callable, Iterator
from typing import Any

import duckdb

from .db_manager import SnapshotManager
from .exceptions import ConfigurationError, StorageError


LOGGER = logging.getLogger(__name__)
PROTOCOL_VERSION = 1
FORBIDDEN_SELECT_TOKENS = re.compile(
    r"\b(?:read_csv|read_json|read_parquet|parquet_scan|csv_scan|json_scan|"
    r"sqlite_scan|postgres_scan|glob|httpfs|delta_scan|iceberg_scan)\s*\(",
    re.IGNORECASE,
)


def _flight_module() -> Any:
    try:
        import pyarrow.flight as flight
    except ImportError as exc:  # pragma: no cover - dependency path
        raise ConfigurationError("缺少 pyarrow，无法启动 Arrow Flight") from exc
    return flight


class ReadOnlySQLGuard:
    def __init__(self, max_length: int) -> None:
        self.max_length = max_length
        self._connection = duckdb.connect(
            ":memory:", config={"enable_external_access": "false", "threads": "1"}
        )
        self._lock = threading.Lock()

    def validate(self, sql: str) -> str:
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("SQL 不能为空")
        if len(sql) > self.max_length:
            raise ValueError(f"SQL 长度超过限制 {self.max_length}")
        if FORBIDDEN_SELECT_TOKENS.search(sql):
            raise ValueError("禁止通过 SQL 访问外部文件、网络或扩展表函数")
        with self._lock:
            try:
                statements = self._connection.extract_statements(sql)
            except Exception as exc:
                raise ValueError(f"SQL 解析失败：{exc}") from exc
        if len(statements) != 1:
            raise ValueError("每次请求只允许一条 SQL")
        statement_type = getattr(statements[0], "type", None)
        if getattr(statement_type, "name", str(statement_type)) != "SELECT":
            raise ValueError("只允许只读 SELECT/WITH 查询")
        return sql.strip()

    def close(self) -> None:
        self._connection.close()


class StockFlightServer:
    """Thin wrapper that delays importing PyArrow until runtime."""

    def __init__(
        self,
        host: str,
        port: int,
        snapshots: SnapshotManager,
        *,
        max_sql_length: int,
        status_provider: Callable[[], dict[str, Any]],
        trigger_update: Callable[[], bool],
    ) -> None:
        flight = _flight_module()
        guard = ReadOnlySQLGuard(max_sql_length)
        location = flight.Location.for_grpc_tcp(host, port)

        class Implementation(flight.FlightServerBase):
            def __init__(self) -> None:
                super().__init__(location)

            def do_get(self, context: Any, ticket: Any) -> Any:
                try:
                    if len(ticket.ticket) > max_sql_length * 2:
                        raise ValueError("Flight Ticket 超过长度限制")
                    payload = json.loads(ticket.ticket.decode("utf-8"))
                    if not isinstance(payload, dict) or payload.get("v") != PROTOCOL_VERSION:
                        raise ValueError("不支持的 Flight 协议版本")
                    sql = guard.validate(payload.get("sql"))
                    with snapshots.acquire() as snapshot:
                        table = snapshot.query(sql)
                    return flight.RecordBatchStream(table)
                except (ValueError, json.JSONDecodeError) as exc:
                    raise flight.FlightServerError(f"invalid request: {exc}") from exc
                except StorageError as exc:
                    raise flight.FlightUnavailableError(str(exc)) from exc
                except Exception as exc:
                    LOGGER.exception("Flight 查询失败")
                    raise flight.FlightInternalError(str(exc)) from exc

            def list_actions(self, context: Any) -> list[tuple[str, str]]:
                return [
                    ("status", "Return service status as JSON"),
                    ("trigger_update", "Start an update if none is running"),
                ]

            def do_action(self, context: Any, action: Any) -> Iterator[Any]:
                if action.type == "status":
                    yield flight.Result(
                        json.dumps(status_provider(), ensure_ascii=False).encode("utf-8")
                    )
                    return
                if action.type == "trigger_update":
                    accepted = trigger_update()
                    yield flight.Result(
                        json.dumps({"accepted": accepted}, ensure_ascii=False).encode("utf-8")
                    )
                    return
                raise flight.FlightServerError(f"invalid action: {action.type}")

        self._guard = guard
        self._implementation = Implementation()

    @property
    def port(self) -> int:
        return int(self._implementation.port)

    def serve(self) -> None:
        self._implementation.serve()

    def shutdown(self) -> None:
        self._implementation.shutdown()

    def close(self) -> None:
        self._guard.close()
