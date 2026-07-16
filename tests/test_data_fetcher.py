from __future__ import annotations

from datetime import date

import pytest

from data_fetcher import BaostockDataFetcher, classify_board
from exceptions import FatalDataError


class Response:
    def __init__(self, fields: list[str], rows: list[list[str]], error_code: str = "0") -> None:
        self.fields = fields
        self.rows = rows
        self.error_code = error_code
        self.error_msg = "success" if error_code == "0" else "temporary"
        self._index = -1

    def next(self) -> bool:
        self._index += 1
        return self._index < len(self.rows)

    def get_row_data(self) -> list[str]:
        return self.rows[self._index]


class FakeBS:
    def login(self) -> Response:
        return Response(["ok"], [])

    def logout(self) -> Response:
        return Response(["ok"], [])

    def query_all_stock(self, day: str) -> Response:
        return Response(
            ["code", "tradeStatus", "code_name"],
            [
                ["sh.000001", "1", "上证指数"],
                ["sh.600000", "1", "浦发银行"],
                ["sz.300001", "0", "特锐德"],
                ["sh.688001", "1", "科创股票"],
            ],
        )

    def query_daily_history_k_AStock(self, date: str) -> Response:
        fields = [
            "date",
            "code",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "adjustflag",
            "turn",
            "tradestatus",
            "pctChg",
            "isST",
        ]
        return Response(
            fields,
            [
                [date, "sh.600000", "10", "11", "9", "10.5", "10", "100", "1000", "3", "1", "1", "5", "0"],
                [date, "sz.300001", "20", "20", "20", "20", "20", "0", "0", "3", "", "0", "0", "0"],
                [date, "sh.688001", "30", "31", "29", "30", "30", "100", "1000", "3", "1", "1", "0", "0"],
            ],
        )


def test_board_classification() -> None:
    assert classify_board("sh.600000") == "main"
    assert classify_board("sz.002001") == "main"
    assert classify_board("sz.301001") == "gem"
    assert classify_board("sh.688001") is None
    assert classify_board("sh.000001") is None


def test_daily_and_stock_list_are_filtered_and_typed(app_config) -> None:
    fetcher = BaostockDataFetcher(app_config, FakeBS())
    stocks = fetcher.fetch_stock_list(date(2024, 1, 2))
    daily = fetcher.fetch_market_daily(date(2024, 1, 2))
    assert stocks["symbol"].tolist() == ["sh.600000", "sz.300001"]
    assert daily["symbol"].tolist() == ["sh.600000", "sz.300001"]
    assert daily.loc[daily["symbol"].eq("sz.300001"), "volume"].item() == 0


def test_wrong_adjustflag_is_fatal(app_config) -> None:
    fake = FakeBS()
    original = fake.query_daily_history_k_AStock

    def wrong(date: str) -> Response:
        result = original(date)
        result.rows[0][9] = "2"
        return result

    fake.query_daily_history_k_AStock = wrong  # type: ignore[method-assign]
    fetcher = BaostockDataFetcher(app_config, fake)
    with pytest.raises(FatalDataError, match="非不复权"):
        fetcher.fetch_market_daily(date(2024, 1, 2))


def test_blank_suspended_volume_and_amount_are_canonicalized(app_config) -> None:
    fake = FakeBS()
    original = fake.query_daily_history_k_AStock

    def blank_suspended(date: str) -> Response:
        result = original(date)
        result.rows[1][7] = ""
        result.rows[1][8] = ""
        return result

    fake.query_daily_history_k_AStock = blank_suspended  # type: ignore[method-assign]
    daily = BaostockDataFetcher(app_config, fake).fetch_market_daily(date(2024, 1, 2))
    row = daily.loc[daily["symbol"].eq("sz.300001")].iloc[0]
    assert row["volume"] == 0
    assert row["amount"] == 0


def test_blank_active_volume_is_fatal(app_config) -> None:
    fake = FakeBS()
    original = fake.query_daily_history_k_AStock

    def blank_active(date: str) -> Response:
        result = original(date)
        result.rows[0][7] = ""
        return result

    fake.query_daily_history_k_AStock = blank_active  # type: ignore[method-assign]
    with pytest.raises(FatalDataError, match="正常交易股票.*volume 为空"):
        BaostockDataFetcher(app_config, fake).fetch_market_daily(date(2024, 1, 2))
