from __future__ import annotations

from dataclasses import replace
from datetime import date

import duckdb
import pandas as pd
import pytest

from qstockdataserver.db_manager import DuckDBManager
from qstockdataserver.exceptions import ConfigurationError, FatalDataError
from qstockdataserver.validation import (
    validate_daily_frame,
    validate_daily_pair,
    validate_stock_list,
)


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
            {"symbol": "sh.600000", "name": "浦发银行", "trade_status": 1, "board": "zb"},
            {"symbol": "sz.300001", "name": "特锐德", "trade_status": 1, "board": "cyb"},
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
        "zb",
        [
            (date(2024, 1, 2), 10.0, 10.2, 9.8, 10.0, 10.0, 100, 1000.0, 1),
            (date(2024, 1, 3), 8.0, 8.6, 7.9, 8.5, 8.0, 100, 850.0, 1),
        ],
    )
    gem = make_daily(
        "sz.300001",
        "cyb",
        [
            (date(2024, 1, 2), 20.0, 20.5, 19.8, 20.0, 20.0, 100, 2000.0, 1),
            (date(2024, 1, 3), 20.0, 20.8, 19.9, 20.5, 20.0, 100, 2050.0, 1),
        ],
    )
    manager.import_symbol_history(main, "zb", target)
    manager.import_symbol_history(gem, "cyb", target)
    manager.complete_initial_import(set(stocks["symbol"]), target)

    with manager.connect(read_only=True) as connection:
        factors = connection.execute(
            "SELECT date, qfq_factor FROM daily WHERE symbol='sh.600000' ORDER BY date"
        ).fetchall()
    assert factors == [(date(2024, 1, 2), pytest.approx(0.8)), (date(2024, 1, 3), pytest.approx(1.0))]

    next_date = date(2024, 1, 4)
    next_daily = pd.concat(
        [
            make_daily(
                "sh.600000",
                "zb",
                [(next_date, 8.5, 8.8, 8.4, 8.6, 8.5, 100, 860.0, 1)],
            ),
            make_daily(
                "sz.300001",
                "cyb",
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
                "zb",
                [(ex_date, 6.8, 7.0, 6.7, 6.9, 6.88, 100, 690.0, 1)],
            ),
            make_daily(
                "sz.300001",
                "cyb",
                [(ex_date, 20.8, 21.2, 20.7, 21.0, 20.8, 100, 2100.0, 1)],
            ),
        ],
        ignore_index=True,
    )
    ex_stocks = stock_list(ex_date)
    manager.apply_market_day(ex_stocks, ex_daily, ex_date)
    with manager.connect(read_only=True) as connection:
        old_factor = connection.execute(
            "SELECT qfq_factor FROM daily WHERE symbol='sh.600000' AND date='2024-01-04'"
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
        result = snapshot.query("SELECT max(date) AS max_date FROM daily_qfq")
        assert result.column("max_date")[0].as_py() == ex_date
        qfq_columns = snapshot.connection.execute(
            "DESCRIBE zb_daily_qfq"
        ).fetchdf()["column_name"].tolist()
        assert qfq_columns == [
            "symbol", "date", "open", "high", "low", "close", "pct_chg",
            "volume", "amount", "trade_status",
        ]
        view_sql = snapshot.connection.execute(
            "SELECT sql FROM duckdb_views() "
            "WHERE view_name='zb_daily_qfq'"
        ).fetchone()[0]
        assert "qfq_factor" not in view_sql
        assert "daily_qfq" in view_sql
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
        "zb",
        [(target, 10.0, 10.2, 9.8, 10.0, 10.0, 100, 1000.0, 1)],
    )
    manager.import_symbol_history(history, "zb", target)
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
        "zb",
        [(target, 10.0, 10.2, 9.8, 10.0, 10.0, 100, 1000.0, 1)],
    )
    manager.import_symbol_history(history, "zb", target)

    report = manager.doctor()

    assert report["status"] == "ok"
    assert report["checks"]["initial_import"] == "incomplete_but_consistent"
    assert report["completed_import_symbols"] == 1


def test_unknown_existing_schema_is_never_overwritten(app_config) -> None:
    app_config.database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(app_config.database_path)) as connection:
        connection.execute("CREATE TABLE unrelated(value INTEGER)")

    with pytest.raises(ConfigurationError, match="数据库不是当前结构"):
        DuckDBManager(app_config).initialize_schema()

    with duckdb.connect(str(app_config.database_path), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM unrelated").fetchone()[0] == 0


def test_kcb_query_objects_are_created_when_enabled(app_config) -> None:
    config = replace(app_config, boards=("zb", "cyb", "kcb"))
    manager = DuckDBManager(config)
    manager.initialize_schema()
    target = date(2024, 1, 2)
    stocks = pd.DataFrame(
        [
            {
                "symbol": "sh.688001",
                "name": "科创股票",
                "trade_status": 1,
                "board": "kcb",
            }
        ]
    )
    validate_stock_list(stocks, target)
    manager.prepare_initial_import(stocks, target)
    history = make_daily(
        "sh.688001",
        "kcb",
        [(target, 8.0, 10.0, 8.0, 8.123, 8.0, 100, 1000.0, 1)],
    )
    manager.import_symbol_history(history, "kcb", target)
    manager.complete_initial_import({"sh.688001"}, target)

    snapshot = manager.build_snapshot()
    try:
        assert snapshot.query("SELECT count(*) AS n FROM kcb_daily_qfq")["n"][0].as_py() == 1
        pct_chg = snapshot.query("SELECT pct_chg FROM kcb_daily_qfq")["pct_chg"][0].as_py()
        assert pct_chg == pytest.approx(1.54)
        assert snapshot.query("SELECT count(*) AS n FROM kcb_stock_list")["n"][0].as_py() == 1
    finally:
        snapshot.close()


def test_new_symbol_history_is_committed_atomically_on_latest_day(app_config) -> None:
    manager = DuckDBManager(app_config)
    manager.initialize_schema()
    initial_date = date(2024, 1, 2)
    initial_stocks = stock_list(initial_date).iloc[[0]].copy()
    manager.prepare_initial_import(initial_stocks, initial_date)
    manager.import_symbol_history(
        make_daily(
            "sh.600000",
            "zb",
            [(initial_date, 10.0, 10.0, 10.0, 10.0, 10.0, 100, 1000.0, 1)],
        ),
        "zb",
        initial_date,
    )
    manager.complete_initial_import({"sh.600000"}, initial_date)

    day3 = date(2024, 1, 3)
    daily3 = pd.concat(
        [
            make_daily("sh.600000", "zb", [(day3, 10, 10, 10, 10, 10, 100, 1000, 1)]),
            make_daily("sz.300001", "cyb", [(day3, 20, 20, 20, 20, 20, 100, 2000, 1)]),
        ],
        ignore_index=True,
    )
    manager.apply_market_day(None, daily3, day3)

    day4 = date(2024, 1, 4)
    latest_stocks = stock_list(day4)
    daily4 = pd.concat(
        [
            make_daily("sh.600000", "zb", [(day4, 10, 10, 10, 10, 10, 100, 1000, 1)]),
            make_daily("sz.300001", "cyb", [(day4, 20, 20, 20, 20, 20, 100, 2000, 1)]),
        ],
        ignore_index=True,
    )
    gem_history = pd.concat(
        [
            daily3.loc[daily3["symbol"].eq("sz.300001")],
            daily4.loc[daily4["symbol"].eq("sz.300001")],
        ],
        ignore_index=True,
    )
    gem_stock = latest_stocks.loc[latest_stocks["symbol"].eq("sz.300001")].copy()
    validate_daily_pair(latest_stocks, daily4, day4)

    conflicting_history = gem_history.copy()
    conflicting_history.loc[
        conflicting_history["date"].eq(day4), "close"
    ] = 21.0
    with pytest.raises(FatalDataError, match="已回补日线的 close.*不一致"):
        manager.apply_market_day(
            latest_stocks,
            daily4,
            day4,
            new_symbol_histories=[(conflicting_history, gem_stock)],
        )
    assert manager.get_last_update_date() == day3
    with manager.connect(read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM daily WHERE symbol='sz.300001'"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT count(*) FROM stock_list WHERE symbol='sz.300001'"
        ).fetchone()[0] == 0

    manager.apply_market_day(
        latest_stocks,
        daily4,
        day4,
        new_symbol_histories=[(gem_history, gem_stock)],
    )

    assert manager.get_last_update_date() == day4
    assert manager.get_meta("stock_list_last_update_date") == day4.isoformat()
    with manager.connect(read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM daily WHERE symbol='sz.300001'"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT count(*) FROM stock_list WHERE symbol='sz.300001'"
        ).fetchone()[0] == 1
