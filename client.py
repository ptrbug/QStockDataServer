"""Client for the QStockDataServer custom Arrow Flight protocol."""

from __future__ import annotations

import json
from typing import Any


class StockDataClientError(RuntimeError):
    pass


class StockDataClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8815,
        *,
        timeout_seconds: float = 60.0,
    ) -> None:
        try:
            import pyarrow.flight as flight
        except ImportError as exc:  # pragma: no cover - dependency path
            raise StockDataClientError("缺少 pyarrow，请先安装 requirements.txt") from exc
        self._flight = flight
        self._timeout = timeout_seconds
        try:
            self._client = flight.connect(f"grpc://{host}:{port}")
        except Exception as exc:
            raise StockDataClientError(f"无法连接股票数据服务 {host}:{port}: {exc}") from exc

    def _options(self) -> Any:
        return self._flight.FlightCallOptions(timeout=self._timeout)

    def query_arrow(self, sql: str) -> Any:
        payload = json.dumps({"v": 1, "sql": sql}, ensure_ascii=False).encode("utf-8")
        try:
            reader = self._client.do_get(
                self._flight.Ticket(payload), options=self._options()
            )
            return reader.read_all()
        except Exception as exc:
            raise StockDataClientError(f"查询失败：{exc}") from exc

    def query(self, sql: str) -> Any:
        return self.query_arrow(sql).to_pandas()

    def _action(self, action_type: str) -> dict[str, Any]:
        try:
            results = self._client.do_action(
                self._flight.Action(action_type, b""), options=self._options()
            )
            values = list(results)
            if len(values) != 1:
                raise StockDataClientError(f"{action_type} 返回数量异常：{len(values)}")
            payload = json.loads(values[0].body.to_pybytes().decode("utf-8"))
            if not isinstance(payload, dict):
                raise StockDataClientError(f"{action_type} 返回格式错误")
            return payload
        except StockDataClientError:
            raise
        except Exception as exc:
            raise StockDataClientError(f"{action_type} 失败：{exc}") from exc

    def status(self) -> dict[str, Any]:
        return self._action("status")

    def trigger_update(self) -> bool:
        return bool(self._action("trigger_update").get("accepted"))

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "StockDataClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
