from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd
import pytest

from db_manager import DuckDBManager
from exceptions import ConfigurationError, FatalDataError
from validation import validate_daily_frame, validate_daily_pair, validate_stock_list


def make_daily(symbol: str, board: str, rows: list[tuple]) -> pd.DataFrame:
    frame = pd.DataFrame(
        rows,
        columns=["date", "open", "high", "low", "close", "preclose", "volume", "amount", "trade_status"],
    )
    frame.insert(0, "symbol", symbol)
    frame["adjustflag"] = 3
    frame["board"] = board
    frame["turn"] = pd.NA
    frame["pct_chg"] = pd.NA
    frame["is_st"] = 0
    validate_daily_frame(frame)
    return frame


def stock_list(trade_date: date) -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {"symbol": "sh.600000", "name": "浦发银行", "trade_status": 1, "board": "main"},
            {"symbol": "sz.300001", "name": "特锐德", "trade_status": 1, "board": "gem"},
        ]
    )
    validate_stock_list(frame, trade_date)
    return frame


def test_initial_import_incremental_factor_and_snapshot(app_config) -> None:
    manager = DuckDBManager(app_config)
    manager.initialize_schema()
    target = date(2024, 1, 3)
    stocks = stock_list(target)
    manager.prepare_initial_import(stocks, target)

    main = make_daily(
        "sh.600000",
        "main",
        [
            (date(2024, 1, 2), 10.0, 10.2, 9.8, 10.0, 10.0, 100, 1000.0, 1),
            (date(2024, 1, 3), 8.0, 8.6, 7.9, 8.5, 8.0, 100, 850.0, 1),
        ],
    )
    gem = make_daily(
        "sz.300001",
        "gem",
        [
            (date(2024, 1, 2), 20.0, 20.5, 19.8, 20.0, 20.0, 100, 2000.0, 1),
            (date(2024, 1, 3), 20.0, 20.8, 19.9, 20.5, 20.0, 100, 2050.0, 1),
        ],
    )
    manager.import_symbol_history(main, "main", target)
    manager.import_symbol_history(gem, "gem", target)
    manager.complete_initial_import(set(stocks["symbol"]), target)

    with manager.connect(read_only=True) as connection:
        factors = connection.execute(
            "SELECT date, qfq_factor FROM main_board_daily ORDER BY date"
        ).fetchall()
    assert factors == [(date(2024, 1, 2), pytest.approx(0.8)), (date(2024, 1, 3), pytest.approx(1.0))]

    next_date = date(2024, 1, 4)
    next_daily = pd.concat(
        [
            make_daily(
                "sh.600000",
                "main",
                [(next_date, 8.5, 8.8, 8.4, 8.6, 8.5, 100, 860.0, 1)],
            ),
            make_daily(
                "sz.300001",
                "gem",
                [(next_date, 20.5, 21.0, 20.4, 20.8, 20.5, 100, 2080.0, 1)],
            ),
        ],
        ignore_index=True,
    )
    next_stocks = stock_list(next_date)
    validate_daily_pair(next_stocks, next_daily, next_date)
    manager.apply_market_day(next_stocks, next_daily, next_date)
    assert manager.get_last_update_date() == next_date

    ex_date = date(2024, 1, 5)
    ex_daily = pd.concat(
        [
            make_daily(
                "sh.600000",
                "main",
                [(ex_date, 6.8, 7.0, 6.7, 6.9, 6.88, 100, 690.0, 1)],
            ),
            make_daily(
                "sz.300001",
                "gem",
                [(ex_date, 20.8, 21.2, 20.7, 21.0, 20.8, 100, 2100.0, 1)],
            ),
        ],
        ignore_index=True,
    )
    ex_stocks = stock_list(ex_date)
    manager.apply_market_day(ex_stocks, ex_daily, ex_date)
    with manager.connect(read_only=True) as connection:
        old_factor = connection.execute(
            "SELECT qfq_factor FROM main_board_daily WHERE date='2024-01-04'"
        ).fetchone()[0]
        event = connection.execute(
            "SELECT event_factor FROM adjustment_events WHERE symbol='sh.600000' AND ex_date='2024-01-05'"
        ).fetchone()[0]
    assert event == pytest.approx(0.8)
    assert old_factor == pytest.approx(0.8)

    report = manager.doctor()
    assert report["status"] == "ok"
    snapshot = manager.build_snapshot()
    try:
        result = snapshot.query("SELECT max(date) AS max_date FROM main_board_daily")
        assert result.column("max_date")[0].as_py() == ex_date
    finally:
        snapshot.close()


def test_duplicate_day_rolls_back_without_meta_advance(app_config) -> None:
    manager = DuckDBManager(app_config)
    manager.initialize_schema()
    target = date(2024, 1, 2)
    stocks = stock_list(target).iloc[[0]].copy()
    manager.prepare_initial_import(stocks, target)
    history = make_daily(
        "sh.600000",
        "main",
        [(target, 10.0, 10.2, 9.8, 10.0, 10.0, 100, 1000.0, 1)],
    )
    manager.import_symbol_history(history, "main", target)
    manager.complete_initial_import({"sh.600000"}, target)
    with pytest.raises(FatalDataError, match="乱序或重复"):
        manager.apply_market_day(stocks, history, target)
    assert manager.get_last_update_date() == target


def test_doctor_accepts_consistent_partial_initial_import(app_config) -> None:
    manager = DuckDBManager(app_config)
    manager.initialize_schema()
    target = date(2024, 1, 2)
    stocks = stock_list(target)
    manager.prepare_initial_import(stocks, target)
    history = make_daily(
        "sh.600000",
        "main",
        [(target, 10.0, 10.2, 9.8, 10.0, 10.0, 100, 1000.0, 1)],
    )
    manager.import_symbol_history(history, "main", target)

    report = manager.doctor()

    assert report["status"] == "ok"
    assert report["checks"]["initial_import"] == "incomplete_but_consistent"
    assert report["completed_import_symbols"] == 1


def test_unknown_existing_schema_is_never_overwritten(app_config) -> None:
    app_config.database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(app_config.database_path)) as connection:
        connection.execute("CREATE TABLE unrelated(value INTEGER)")

    with pytest.raises(ConfigurationError, match="拒绝覆盖未知 schema"):
        DuckDBManager(app_config).initialize_schema()

    with duckdb.connect(str(app_config.database_path), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM unrelated").fetchone()[0] == 0
