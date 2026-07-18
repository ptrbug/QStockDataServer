from __future__ import annotations

import contextlib
import sys
import types
from datetime import date

import pandas as pd

from server import StockDataService, _build_parser


def test_cli_uses_default_config_path() -> None:
    args = _build_parser().parse_args(["serve"])

    assert args.config == "config.yaml"


def _daily(trade_date: date) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "sh.600000", "date": trade_date, "open": 10.0,
                "high": 10.0, "low": 10.0, "close": 10.0, "preclose": 10.0,
                "volume": 100, "amount": 1000.0, "adjustflag": 3,
                "trade_status": 1, "turn": 1.0, "pct_chg": 0.0,
                "is_st": 0, "board": "zb",
            },
            {
                "symbol": "sz.300001", "date": trade_date, "open": 20.0,
                "high": 20.0, "low": 20.0, "close": 20.0, "preclose": 20.0,
                "volume": 100, "amount": 2000.0, "adjustflag": 3,
                "trade_status": 1, "turn": 1.0, "pct_chg": 0.0,
                "is_st": 0, "board": "cyb",
            },
        ]
    )


class FakeFetcher:
    def __init__(self) -> None:
        self.stock_list_dates: list[date] = []
        self.daily_dates: list[date] = []
        self.history_calls: list[tuple[str, date, date]] = []

    def fetch_last_trading_date(self) -> date:
        return date(2024, 1, 5)

    def fetch_trade_dates(self, start: date, end: date) -> list[date]:
        assert start == date(2024, 1, 3)
        assert end == date(2024, 1, 5)
        return [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]

    def fetch_stock_list(self, trade_date: date) -> pd.DataFrame:
        self.stock_list_dates.append(trade_date)
        return pd.DataFrame(
            [
                {"symbol": "sh.600000", "name": "浦发银行", "trade_status": 1, "board": "zb"},
                {"symbol": "sz.300001", "name": "新增股票", "trade_status": 1, "board": "cyb"},
            ]
        )

    def fetch_stock_history(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        self.history_calls.append((symbol, start, end))
        return _daily(end).loc[lambda frame: frame["symbol"].eq(symbol)].copy()

    def fetch_market_daily(self, trade_date: date) -> pd.DataFrame:
        self.daily_dates.append(trade_date)
        return _daily(trade_date)


class FakeDatabase:
    def __init__(self) -> None:
        self.applied: list[tuple[date, bool, int]] = []

    def get_last_update_date(self) -> date:
        return date(2024, 1, 2)

    def get_stock_symbols(self) -> set[str]:
        return {"sh.600000"}

    def apply_market_day(
        self,
        stock_list: pd.DataFrame | None,
        daily: pd.DataFrame,
        trade_date: date,
        *,
        new_symbol_histories: list[tuple[pd.DataFrame, pd.DataFrame]] | None = None,
    ) -> None:
        self.applied.append(
            (trade_date, stock_list is not None, len(new_symbol_histories or []))
        )


def test_catch_up_queries_latest_stock_list_once_and_backfills_new_stock(app_config) -> None:
    service = StockDataService(app_config)
    fetcher = FakeFetcher()
    database = FakeDatabase()
    service.fetcher = fetcher  # type: ignore[assignment]
    service.database = database  # type: ignore[assignment]

    assert service._catch_up() is True
    assert fetcher.stock_list_dates == [date(2024, 1, 5)]
    assert fetcher.daily_dates == [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]
    assert fetcher.history_calls == [
        ("sz.300001", app_config.start_date, date(2024, 1, 5))
    ]
    assert database.applied == [
        (date(2024, 1, 3), False, 0),
        (date(2024, 1, 4), False, 0),
        (date(2024, 1, 5), True, 1),
    ]


def test_initialize_catches_up_after_fixed_target_initial_import(
    app_config, monkeypatch
) -> None:
    service = StockDataService(app_config)
    calls: list[str] = []

    class Database:
        def initialize_schema(self) -> None:
            calls.append("schema")

        def needs_initial_import(self) -> bool:
            return True

        def build_snapshot(self) -> object:
            calls.append("snapshot")
            return object()

    class Fetcher:
        @contextlib.contextmanager
        def session(self):
            calls.append("login")
            yield self
            calls.append("logout")

    class Snapshots:
        def swap(self, snapshot: object) -> None:
            calls.append("swap")

    service.database = Database()  # type: ignore[assignment]
    service.fetcher = Fetcher()  # type: ignore[assignment]
    service.snapshots = Snapshots()  # type: ignore[assignment]
    monkeypatch.setattr(service, "_initial_import", lambda: calls.append("initial"))
    monkeypatch.setattr(service, "_catch_up", lambda: calls.append("catch_up"))

    service.initialize()

    assert calls == [
        "schema", "login", "initial", "catch_up", "logout", "snapshot", "swap"
    ]


def test_update_worker_launches_strategy_programs_after_snapshot_swap(
    app_config, monkeypatch
) -> None:
    service = StockDataService(app_config)
    calls: list[tuple[str, object]] = []

    class Fetcher:
        @contextlib.contextmanager
        def session(self):
            yield self

    class Snapshot:
        version = "2024-01-05"

    class Database:
        def build_snapshot(self) -> Snapshot:
            calls.append(("build_snapshot", None))
            return Snapshot()

    class Snapshots:
        def swap(self, snapshot: Snapshot) -> None:
            calls.append(("swap", snapshot.version))

    class Flight:
        port = 18815

    class Launcher:
        def launch_all(self, *, flight_port: int, snapshot_version: str) -> None:
            calls.append(("launch", (flight_port, snapshot_version)))

    service.fetcher = Fetcher()  # type: ignore[assignment]
    service.database = Database()  # type: ignore[assignment]
    service.snapshots = Snapshots()  # type: ignore[assignment]
    service._flight = Flight()  # type: ignore[assignment]
    service.strategy_launcher = Launcher()  # type: ignore[assignment]
    monkeypatch.setattr(service, "_catch_up", lambda: True)

    service._update_worker("test-run")

    assert calls == [
        ("build_snapshot", None),
        ("swap", "2024-01-05"),
        ("launch", (18815, "2024-01-05")),
    ]


def test_serve_launches_strategy_programs_for_existing_startup_snapshot(
    app_config, monkeypatch
) -> None:
    service = StockDataService(app_config)
    calls: list[tuple[str, object]] = []

    class Snapshots:
        version = "2024-01-05"

        def close(self) -> None:
            calls.append(("snapshots_close", None))

    class Fetcher:
        def logout(self) -> None:
            calls.append(("logout", None))

    class Flight:
        port = 19999

        def __init__(self, *args, **kwargs) -> None:
            calls.append(("flight_init", None))

        def serve(self) -> None:
            raise KeyboardInterrupt

        def shutdown(self) -> None:
            calls.append(("flight_shutdown", None))

        def close(self) -> None:
            calls.append(("flight_close", None))

    class Scheduler:
        def __init__(self, *, timezone) -> None:
            calls.append(("scheduler_init", timezone))

        def add_job(self, *args, **kwargs) -> None:
            calls.append(("add_job", kwargs["id"]))

        def start(self) -> None:
            calls.append(("scheduler_start", None))

        def shutdown(self, *, wait: bool) -> None:
            calls.append(("scheduler_shutdown", wait))

    class Launcher:
        def launch_all(self, *, flight_port: int, snapshot_version: str) -> None:
            calls.append(("launch", (flight_port, snapshot_version)))

    apscheduler = types.ModuleType("apscheduler")
    schedulers = types.ModuleType("apscheduler.schedulers")
    background = types.ModuleType("apscheduler.schedulers.background")
    background.BackgroundScheduler = Scheduler
    monkeypatch.setitem(sys.modules, "apscheduler", apscheduler)
    monkeypatch.setitem(sys.modules, "apscheduler.schedulers", schedulers)
    monkeypatch.setitem(sys.modules, "apscheduler.schedulers.background", background)
    monkeypatch.setattr("qstockdataserver.service.StockFlightServer", Flight)
    service.snapshots = Snapshots()  # type: ignore[assignment]
    service.fetcher = Fetcher()  # type: ignore[assignment]
    service.strategy_launcher = Launcher()  # type: ignore[assignment]

    assert service.serve() == 0
    assert ("launch", (19999, "2024-01-05")) in calls
    assert calls.index(("flight_init", None)) < calls.index(
        ("launch", (19999, "2024-01-05"))
    )
