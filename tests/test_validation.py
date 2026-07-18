from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from qstockdataserver.exceptions import FatalDataError
from qstockdataserver.validation import (
    calculate_qfq_factors,
    validate_daily_frame,
    validate_daily_pair,
)


def daily_frame(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "adjustflag": 3,
        "trade_status": 1,
        "board": "zb",
        "volume": 100,
        "amount": 1000.0,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def test_preclose_qfq_factor_calculation() -> None:
    frame = daily_frame(
        [
            {
                "symbol": "sh.600000",
                "date": date(2024, 1, 2),
                "open": 10.0,
                "high": 10.2,
                "low": 9.8,
                "close": 10.0,
                "preclose": 10.0,
            },
            {
                "symbol": "sh.600000",
                "date": date(2024, 1, 3),
                "open": 8.0,
                "high": 8.6,
                "low": 7.9,
                "close": 8.5,
                "preclose": 8.0,
            },
        ]
    )
    validate_daily_frame(frame)
    adjusted, events = calculate_qfq_factors(frame, 1.0e-10)
    assert adjusted["qfq_factor"].tolist() == pytest.approx([0.8, 1.0])
    assert events.to_dict("records") == [
        {
            "symbol": "sh.600000",
            "ex_date": date(2024, 1, 3),
            "previous_close": 10.0,
            "preclose": 8.0,
            "event_factor": 0.8,
        }
    ]


def test_invalid_required_price_is_fatal() -> None:
    frame = daily_frame(
        [
            {
                "symbol": "sh.600000",
                "date": date(2024, 1, 2),
                "open": 10.0,
                "high": 9.0,
                "low": 9.8,
                "close": 10.0,
                "preclose": 10.0,
            }
        ]
    )
    with pytest.raises(FatalDataError, match="OHLC"):
        validate_daily_frame(frame)


def test_suspended_row_and_pair_validation() -> None:
    trade_date = date(2024, 1, 2)
    stocks = pd.DataFrame(
        [
            {"symbol": "sh.600000", "name": "浦发银行", "trade_status": 0, "board": "zb"}
        ]
    )
    daily = daily_frame(
        [
            {
                "symbol": "sh.600000",
                "date": trade_date,
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "preclose": 10.0,
                "volume": 0,
                "amount": 0.0,
                "trade_status": 0,
            }
        ]
    )
    validate_daily_frame(daily, expected_date=trade_date)
    validate_daily_pair(stocks, daily, trade_date)
    stocks.loc[0, "trade_status"] = 1
    with pytest.raises(FatalDataError, match="交易状态"):
        validate_daily_pair(stocks, daily, trade_date)
