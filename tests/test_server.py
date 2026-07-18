from __future__ import annotations

import contextlib
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
