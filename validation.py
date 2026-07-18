"""Strict market-data validation and deterministic qfq factor calculation."""

from __future__ import annotations

import math
import re
from datetime import date
from typing import Iterable

import numpy as np
import pandas as pd

from exceptions import FatalDataError


DAILY_COLUMNS = {
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "trade_status",
    "board",
}
STOCK_COLUMNS = {"symbol", "name", "trade_status", "board"}
SYMBOL_RE = re.compile(r"^(?:sh|sz)\.\d{6}$")
SUPPORTED_BOARDS = {"zb", "cyb", "kcb"}


def _missing_columns(frame: pd.DataFrame, required: Iterable[str]) -> list[str]:
    return sorted(set(required) - set(frame.columns))


def _row_label(row: pd.Series) -> str:
    return f"symbol={row.get('symbol', '?')} date={row.get('date', '?')}"


def validate_stock_list(frame: pd.DataFrame, requested_date: date) -> None:
    missing = _missing_columns(frame, STOCK_COLUMNS)
    if missing:
        raise FatalDataError(f"证券列表缺少字段：{missing}")
    if frame.empty:
        raise FatalDataError(f"{requested_date} 的目标板块证券列表为空")
    if frame["symbol"].duplicated().any():
        values = frame.loc[frame["symbol"].duplicated(False), "symbol"].head(10).tolist()
        raise FatalDataError(f"证券列表代码重复：{values}")
    bad_symbols = frame.loc[~frame["symbol"].astype(str).str.match(SYMBOL_RE), "symbol"]
    if not bad_symbols.empty:
        raise FatalDataError(f"证券代码格式错误：{bad_symbols.head(10).tolist()}")
    if frame["name"].isna().any() or frame["name"].astype(str).str.strip().eq("").any():
        raise FatalDataError("证券列表存在空名称")
    if not frame["trade_status"].isin([0, 1]).all():
        raise FatalDataError("证券列表 trade_status 必须是 0 或 1")
    if not frame["board"].isin(SUPPORTED_BOARDS).all():
        raise FatalDataError("证券列表包含无法识别的目标板块")


def validate_daily_frame(
    frame: pd.DataFrame,
    *,
    expected_date: date | None = None,
    expected_symbol: str | None = None,
    epsilon: float = 1.0e-10,
) -> None:
    missing = _missing_columns(frame, DAILY_COLUMNS)
    if missing:
        raise FatalDataError(f"日线数据缺少字段：{missing}")
    if frame.empty:
        target = expected_symbol or expected_date or "请求"
        raise FatalDataError(f"{target} 的日线数据为空")
    if frame[["symbol", "date"]].duplicated().any():
        rows = frame.loc[frame[["symbol", "date"]].duplicated(False), ["symbol", "date"]]
        raise FatalDataError(f"日线唯一键重复：{rows.head(10).to_dict('records')}")
    if expected_date is not None and not frame["date"].eq(expected_date).all():
        values = sorted(str(value) for value in frame.loc[frame["date"] != expected_date, "date"].unique())
        raise FatalDataError(f"请求日期 {expected_date}，响应混入其他日期：{values[:10]}")
    if expected_symbol is not None and not frame["symbol"].eq(expected_symbol).all():
        values = frame.loc[frame["symbol"] != expected_symbol, "symbol"].unique().tolist()
        raise FatalDataError(f"请求股票 {expected_symbol}，响应混入其他代码：{values[:10]}")
    if not frame["adjustflag"].eq(3).all():
        raise FatalDataError(
            f"发现非不复权行情，adjustflag={sorted(frame['adjustflag'].unique().tolist())}"
        )
    if not frame["trade_status"].isin([0, 1]).all():
        raise FatalDataError("日线 trade_status 必须是 0 或 1")
    if not frame["board"].isin(SUPPORTED_BOARDS).all():
        raise FatalDataError("日线包含无法识别的目标板块")

    price_columns = ["open", "high", "low", "close", "preclose"]
    numeric_columns = price_columns + ["volume", "amount"]
    for column in numeric_columns:
        values = frame[column].to_numpy(dtype=float, na_value=np.nan)
        if not np.isfinite(values).all():
            row = frame.loc[~np.isfinite(values)].iloc[0]
            raise FatalDataError(f"{_row_label(row)} 的 {column} 为空或不是有限数")
    for column in price_columns:
        if (frame[column] <= 0).any():
            row = frame.loc[frame[column] <= 0].iloc[0]
            raise FatalDataError(f"{_row_label(row)} 的 {column} 必须大于 0")
    if (frame["volume"] < 0).any() or (frame["amount"] < 0).any():
        raise FatalDataError("成交量和成交额不能为负数")

    tolerance = max(epsilon, 1.0e-12)
    invalid_high = frame["high"] + tolerance < frame[["open", "low", "close"]].max(axis=1)
    invalid_low = frame["low"] - tolerance > frame[["open", "high", "close"]].min(axis=1)
    if invalid_high.any() or invalid_low.any():
        row = frame.loc[invalid_high | invalid_low].iloc[0]
        raise FatalDataError(f"{_row_label(row)} 的 OHLC 高低关系非法")

    suspended = frame["trade_status"].eq(0)
    if suspended.any():
        stopped = frame.loc[suspended]
        spread = stopped[["open", "high", "low", "close"]].max(axis=1) - stopped[
            ["open", "high", "low", "close"]
        ].min(axis=1)
        price_scale = stopped[["open", "high", "low", "close"]].abs().max(axis=1).clip(lower=1.0)
        invalid = (spread > tolerance * price_scale) | stopped["volume"].ne(0) | stopped[
            "amount"
        ].abs().gt(tolerance)
        if invalid.any():
            row = stopped.loc[invalid].iloc[0]
            raise FatalDataError(f"{_row_label(row)} 的停牌行情不符合价格相等且量额为 0")


def validate_daily_pair(stock_list: pd.DataFrame, daily: pd.DataFrame, trade_date: date) -> None:
    validate_stock_list(stock_list, trade_date)
    expected = set(stock_list["symbol"])
    actual = set(daily["symbol"])
    if expected != actual:
        missing = sorted(expected - actual)[:20]
        unexpected = sorted(actual - expected)[:20]
        raise FatalDataError(
            f"{trade_date} 证券列表与日线集合不一致，缺少={missing}，多出={unexpected}"
        )
    left = stock_list.set_index("symbol")["trade_status"].astype(int).sort_index()
    right = daily.set_index("symbol")["trade_status"].astype(int).sort_index()
    mismatch = left.ne(right)
    if mismatch.any():
        symbols = mismatch[mismatch].index[:20].tolist()
        raise FatalDataError(f"{trade_date} 两个接口交易状态不一致：{symbols}")


def calculate_qfq_factors(
    frame: pd.DataFrame, epsilon: float
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate deterministic qfq factors for one symbol's full raw history."""

    if frame.empty:
        raise FatalDataError("无法为空行情计算前复权因子")
    symbols = frame["symbol"].unique().tolist()
    if len(symbols) != 1:
        raise FatalDataError(f"单股因子计算混入多个代码：{symbols[:10]}")
    ordered = frame.sort_values("date", kind="stable").reset_index(drop=True).copy()
    factors = np.ones(len(ordered), dtype=float)
    events: list[dict[str, object]] = []

    for index in range(len(ordered) - 1, 0, -1):
        current = ordered.iloc[index]
        previous = ordered.iloc[index - 1]
        previous_close = float(previous["close"])
        preclose = float(current["preclose"])
        if not math.isfinite(previous_close) or previous_close <= 0:
            raise FatalDataError(f"{_row_label(previous)} 无法作为复权计算的上一收盘价")
        if not math.isfinite(preclose) or preclose <= 0:
            raise FatalDataError(f"{_row_label(current)} 的 preclose 无法计算复权因子")
        ratio = preclose / previous_close
        if math.isclose(ratio, 1.0, rel_tol=epsilon, abs_tol=epsilon):
            ratio = 1.0
        else:
            events.append(
                {
                    "symbol": current["symbol"],
                    "ex_date": current["date"],
                    "previous_close": previous_close,
                    "preclose": preclose,
                    "event_factor": ratio,
                }
            )
        factors[index - 1] = factors[index] * ratio

    if not np.isfinite(factors).all() or (factors <= 0).any():
        raise FatalDataError(f"{symbols[0]} 计算出非法前复权因子")
    ordered["qfq_factor"] = factors

    # The adjusted previous close and current preclose must join continuously.
    if len(ordered) > 1:
        left = ordered["close"].to_numpy(dtype=float)[:-1] * factors[:-1]
        right = ordered["preclose"].to_numpy(dtype=float)[1:] * factors[1:]
        scale = np.maximum.reduce([np.abs(left), np.abs(right), np.ones_like(left)])
        if (np.abs(left - right) > max(epsilon * 10, 1.0e-9) * scale).any():
            raise FatalDataError(f"{symbols[0]} 前复权收益连续性校验失败")

    event_frame = pd.DataFrame(
        events,
        columns=["symbol", "ex_date", "previous_close", "preclose", "event_factor"],
    )
    return ordered, event_frame
