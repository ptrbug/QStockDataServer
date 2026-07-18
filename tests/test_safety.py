from __future__ import annotations

import threading

import duckdb

from qstockdataserver.client import StockDataClient, StockDataClientError
from qstockdataserver.db_manager import MemorySnapshot, SnapshotManager
from qstockdataserver.exceptions import FatalDataError
from qstockdataserver.flight_server import ReadOnlySQLGuard, StockFlightServer
from qstockdataserver.logging_config import FatalMarkerManager


def test_read_only_sql_guard() -> None:
    guard = ReadOnlySQLGuard(1000)
    try:
        assert guard.validate("WITH x AS (SELECT 1 AS a) SELECT * FROM x")
        for sql in (
            "DELETE FROM daily_qfq",
            "SELECT 1; SELECT 2",
            "SELECT * FROM read_csv('secret.csv')",
        ):
            try:
                guard.validate(sql)
            except ValueError:
                pass
            else:
                raise AssertionError(f"unsafe SQL was accepted: {sql}")
    finally:
        guard.close()


def test_fatal_marker_is_written_and_cleared(app_config) -> None:
    marker = FatalMarkerManager(app_config.fatal_marker_path)
    error = FatalDataError("wrong date")
    payload = marker.write(
        error,
        exit_code=4,
        stage="test",
        run_id="test-run",
        log_path=app_config.error_log_path,
        trade_date="2024-01-02",
    )
    assert marker.exists()
    assert marker.read()["message"] == "wrong date"
    assert payload["exit_code"] == 4
    marker.clear()
    assert not marker.exists()


def test_flight_query_and_read_only_rejection() -> None:
    connection = duckdb.connect(":memory:", config={"enable_external_access": "false"})
    connection.execute("CREATE TABLE prices(symbol VARCHAR, close DOUBLE)")
    connection.execute("INSERT INTO prices VALUES ('sh.600000', 10.5)")
    snapshots = SnapshotManager()
    snapshots.swap(MemorySnapshot(connection, "2024-01-02", 100))
    server = StockFlightServer(
        "127.0.0.1",
        0,
        snapshots,
        max_sql_length=1000,
        status_provider=lambda: {"state": "ready"},
        trigger_update=lambda: True,
    )
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    try:
        with StockDataClient("127.0.0.1", server.port, timeout_seconds=5) as client:
            result = client.query("SELECT * FROM prices")
            assert result.to_dict("records") == [{"symbol": "sh.600000", "close": 10.5}]
            assert client.status()["state"] == "ready"
            try:
                client.query("DELETE FROM prices")
            except StockDataClientError:
                pass
            else:
                raise AssertionError("write SQL was accepted")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.close()
        snapshots.close()
