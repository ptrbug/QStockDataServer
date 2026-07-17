"""Baostock-only access layer with strict normalization and retries."""

from __future__ import annotations

import contextlib
import logging
import time as time_module
from collections.abc import Callable, Iterator, Sequence
from datetime import date, datetime, timedelta
from typing import Any, TypeVar

import pandas as pd

from config import AppConfig
from exceptions import (
    ConfigurationError,
    FatalDataError,
    TemporarySourceAttemptError,
    TransientSourceError,
)
from validation import validate_daily_frame, validate_stock_list


LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


DAILY_FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,"
    "turn,tradestatus,pctChg,isST"
)


def classify_board(symbol: str) -> str | None:
    """Return the supported board for an A-share symbol, otherwise None."""

    value = str(symbol).lower()
    if value.startswith("sh.") and len(value) == 9:
        code = value[3:]
        if code.startswith(("600", "601", "603", "605", "609")):
            return "main"
    if value.startswith("sz.") and len(value) == 9:
        code = value[3:]
        if code.startswith(("000", "001", "002", "003")):
            return "main"
        if code.startswith(("300", "301")):
            return "gem"
    return None


class BaostockDataFetcher:
    def __init__(self, config: AppConfig, bs_module: Any | None = None) -> None:
        self.config = config
        if bs_module is None:
            try:
                import baostock as bs_module  # type: ignore[no-redef]
            except ImportError as exc:
                raise ConfigurationError("缺少 baostock，请先安装 requirements.txt") from exc
        self.bs = bs_module
        self._logged_in = False
        self._session_started_at: float | None = None

    def _retry_delay(self, attempt: int) -> int:
        if attempt <= len(self.config.retry_delays_seconds):
            return self.config.retry_delays_seconds[attempt - 1]
        return self.config.retry_delays_seconds[-1]

    def _session_rotation_due(self) -> str | None:
        if not self._logged_in or self._session_started_at is None:
            return None
        age_seconds = time_module.monotonic() - self._session_started_at
        if age_seconds >= self.config.session_max_minutes * 60:
            return f"会话已使用 {age_seconds / 60:.1f} 分钟"
        return None

    def _login_once(self) -> None:
        try:
            response = self.bs.login()
        except Exception as exc:
            raise TemporarySourceAttemptError(f"Baostock 登录异常：{exc}") from exc
        if str(getattr(response, "error_code", "")) != "0":
            raise TemporarySourceAttemptError(
                f"Baostock 登录失败 error_code={getattr(response, 'error_code', None)} "
                f"error_msg={getattr(response, 'error_msg', None)}"
            )
        self._logged_in = True
        self._session_started_at = time_module.monotonic()
        LOGGER.info("Baostock 登录成功")

    def _retry(
        self,
        operation: str,
        function: Callable[[], T],
        *,
        requires_session: bool = False,
    ) -> T:
        last_error: BaseException | None = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                if requires_session:
                    rotation_reason = self._session_rotation_due()
                    if rotation_reason:
                        LOGGER.info("%s，下一请求前主动轮换 Baostock 会话", rotation_reason)
                        self.logout()
                    if not self._logged_in:
                        self._login_once()
                return function()
            except TemporarySourceAttemptError as exc:
                last_error = exc
                if requires_session:
                    # A transport/protocol failure makes the current session
                    # untrustworthy. Discard it immediately; login is delayed
                    # until immediately before the next request.
                    self.logout()
                delay = self._retry_delay(attempt)
                LOGGER.warning(
                    "%s 第 %d/%d 次尝试失败：%s；%d 秒后重试",
                    operation,
                    attempt,
                    self.config.max_retries,
                    exc,
                    delay,
                )
                if attempt < self.config.max_retries:
                    time_module.sleep(delay)
        raise TransientSourceError(
            f"{operation} 重试 {self.config.max_retries} 次后仍失败：{last_error}"
        ) from last_error

    def login(self) -> None:
        if self._logged_in:
            return
        self._retry("Baostock 登录", self._login_once)

    def logout(self) -> None:
        if not self._logged_in:
            return
        try:
            response = self.bs.logout()
            if response is not None and str(getattr(response, "error_code", "0")) != "0":
                LOGGER.error(
                    "Baostock 登出失败 error_code=%s error_msg=%s",
                    getattr(response, "error_code", None),
                    getattr(response, "error_msg", None),
                )
        except Exception:
            LOGGER.exception("Baostock 登出异常")
        finally:
            self._logged_in = False
            self._session_started_at = None

    @contextlib.contextmanager
    def session(self) -> Iterator["BaostockDataFetcher"]:
        self.login()
        try:
            yield self
        finally:
            self.logout()

    @staticmethod
    def _consume_result(result: Any, operation: str) -> pd.DataFrame:
        error_code = str(getattr(result, "error_code", ""))
        if error_code != "0":
            raise TemporarySourceAttemptError(
                f"{operation} error_code={error_code} error_msg={getattr(result, 'error_msg', None)}"
            )
        fields = list(getattr(result, "fields", []) or [])
        if not fields:
            raise FatalDataError(f"{operation} 返回成功但没有 fields")
        rows: list[list[Any]] = []
        try:
            while str(getattr(result, "error_code", "")) == "0" and result.next():
                row = list(result.get_row_data())
                if len(row) != len(fields):
                    raise FatalDataError(
                        f"{operation} 返回行列数 {len(row)}，预期 {len(fields)}"
                    )
                rows.append(row)
        except FatalDataError:
            raise
        except Exception as exc:
            raise TemporarySourceAttemptError(f"{operation} 读取响应失败：{exc}") from exc
        if str(getattr(result, "error_code", "")) != "0":
            raise TemporarySourceAttemptError(
                f"{operation} 迭代失败 error_code={getattr(result, 'error_code', None)} "
                f"error_msg={getattr(result, 'error_msg', None)}"
            )
        return pd.DataFrame(rows, columns=fields)

    def _query(self, operation: str, call: Callable[[], Any]) -> pd.DataFrame:
        def attempt() -> pd.DataFrame:
            try:
                result = call()
            except (FatalDataError, ConfigurationError):
                raise
            except Exception as exc:
                raise TemporarySourceAttemptError(f"{operation} 调用异常：{exc}") from exc
            return self._consume_result(result, operation)

        return self._retry(operation, attempt, requires_session=True)

    @staticmethod
    def _require_raw_columns(frame: pd.DataFrame, required: Sequence[str], operation: str) -> None:
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            raise FatalDataError(f"{operation} 缺少字段：{missing}")

    @staticmethod
    def _nonempty(frame: pd.DataFrame, columns: Sequence[str], operation: str) -> None:
        for column in columns:
            values = frame[column]
            if values.isna().any() or values.astype(str).str.strip().eq("").any():
                raise FatalDataError(f"{operation} 的必需字段 {column} 存在空值")

    def _normalize_daily(self, raw: pd.DataFrame, operation: str) -> pd.DataFrame:
        required = [
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
            "tradestatus",
        ]
        self._require_raw_columns(raw, required, operation)
        if raw.empty:
            return pd.DataFrame(
                columns=[
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
                    "turn",
                    "pct_chg",
                    "is_st",
                    "board",
                ]
            )
        frame = raw.copy()
        frame.rename(
            columns={
                "code": "symbol",
                "tradestatus": "trade_status",
                "pctChg": "pct_chg",
                "isST": "is_st",
            },
            inplace=True,
        )
        frame["symbol"] = frame["symbol"].astype(str).str.lower().str.strip()
        frame["board"] = frame["symbol"].map(classify_board)
        frame = frame.loc[frame["board"].notna()].reset_index(drop=True)
        self._nonempty(
            frame,
            [
                "date",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "preclose",
                "adjustflag",
                "trade_status",
            ],
            operation,
        )
        try:
            trade_status = pd.to_numeric(frame["trade_status"], errors="raise").astype(int)
            for column in ("volume", "amount"):
                blank = frame[column].isna() | frame[column].astype(str).str.strip().eq("")
                invalid_blank = blank & trade_status.ne(0)
                if invalid_blank.any():
                    symbol = frame.loc[invalid_blank, "symbol"].iloc[0]
                    raise FatalDataError(
                        f"{operation} 的正常交易股票 {symbol} 的 {column} 为空"
                    )
                if blank.any():
                    LOGGER.warning(
                        "%s 有 %d 条停牌行情的 %s 为空，按交易状态规范化为 0",
                        operation,
                        int(blank.sum()),
                        column,
                    )
                    frame.loc[blank, column] = "0"
            frame["date"] = pd.to_datetime(frame["date"], format="%Y-%m-%d", errors="raise").dt.date
            for column in ["open", "high", "low", "close", "preclose", "volume", "amount"]:
                frame[column] = pd.to_numeric(frame[column], errors="raise")
            frame["adjustflag"] = pd.to_numeric(frame["adjustflag"], errors="raise").astype(int)
            frame["trade_status"] = trade_status
        except FatalDataError:
            raise
        except (TypeError, ValueError) as exc:
            raise FatalDataError(f"{operation} 必需字段类型转换失败：{exc}") from exc

        volume = frame["volume"].astype(float)
        if not np_all_integral(volume):
            raise FatalDataError(f"{operation} volume 包含非整数")
        frame["volume"] = volume.astype("int64")
        suspended = frame["trade_status"].eq(0)
        equal_prices = frame[["open", "high", "low", "close"]].max(axis=1).eq(
            frame[["open", "high", "low", "close"]].min(axis=1)
        )
        stale_suspended_volume = (
            suspended
            & equal_prices
            & frame["amount"].eq(0)
            & frame["volume"].gt(0)
        )
        if stale_suspended_volume.any():
            LOGGER.warning(
                "%s 有 %d 条停牌行情残留了非零 volume，但 OHLC 相等且 amount=0，"
                "按交易状态规范化为 0",
                operation,
                int(stale_suspended_volume.sum()),
            )
            frame.loc[stale_suspended_volume, "volume"] = 0
        for optional in ("turn", "pct_chg", "is_st"):
            if optional not in frame.columns:
                frame[optional] = pd.NA
            frame[optional] = pd.to_numeric(
                frame[optional].replace(r"^\s*$", pd.NA, regex=True), errors="coerce"
            )
        return frame[
            [
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
                "turn",
                "pct_chg",
                "is_st",
                "board",
            ]
        ]

    def fetch_trade_dates(self, start_date: str | date, end_date: str | date) -> list[date]:
        start_text = str(start_date)
        end_text = str(end_date)
        raw = self._query(
            f"query_trade_dates({start_text},{end_text})",
            lambda: self.bs.query_trade_dates(start_date=start_text, end_date=end_text),
        )
        self._require_raw_columns(raw, ["calendar_date", "is_trading_day"], "query_trade_dates")
        if raw.empty:
            raise FatalDataError(f"交易日历 {start_text}..{end_text} 返回为空")
        try:
            parsed = pd.to_datetime(raw["calendar_date"], format="%Y-%m-%d", errors="raise").dt.date
            flags = pd.to_numeric(raw["is_trading_day"], errors="raise").astype(int)
        except (TypeError, ValueError) as exc:
            raise FatalDataError(f"交易日历字段非法：{exc}") from exc
        if not flags.isin([0, 1]).all():
            raise FatalDataError("交易日历 is_trading_day 必须是 0 或 1")
        return sorted(parsed.loc[flags.eq(1)].tolist())

    def fetch_last_trading_date(self, now: datetime | None = None) -> date:
        local_now = now.astimezone(self.config.timezone) if now else datetime.now(self.config.timezone)
        eligible = local_now.date()
        if local_now.timetz().replace(tzinfo=None) < self.config.update_time:
            eligible -= timedelta(days=1)
        dates = self.fetch_trade_dates(eligible - timedelta(days=45), eligible)
        if not dates:
            raise FatalDataError(f"{eligible} 之前 45 天没有交易日")
        return dates[-1]

    def fetch_stock_list(self, trade_date: str | date, board: str | None = None) -> pd.DataFrame:
        if board not in {None, "main", "gem"}:
            raise ConfigurationError(f"不支持的板块：{board}")
        requested = date.fromisoformat(str(trade_date))
        raw = self._query(
            f"query_all_stock({requested})",
            lambda: self.bs.query_all_stock(day=requested.isoformat()),
        )
        self._require_raw_columns(raw, ["code", "tradeStatus", "code_name"], "query_all_stock")
        frame = raw.rename(
            columns={"code": "symbol", "tradeStatus": "trade_status", "code_name": "name"}
        ).copy()
        frame["symbol"] = frame["symbol"].astype(str).str.lower().str.strip()
        frame["board"] = frame["symbol"].map(classify_board)
        frame = frame.loc[frame["board"].notna()].copy()
        self._nonempty(frame, ["symbol", "trade_status", "name"], "query_all_stock")
        frame["name"] = frame["name"].astype(str).str.strip()
        try:
            frame["trade_status"] = pd.to_numeric(
                frame["trade_status"], errors="raise"
            ).astype(int)
        except (TypeError, ValueError) as exc:
            raise FatalDataError(f"query_all_stock tradeStatus 非法：{exc}") from exc
        frame = frame.loc[:, ["symbol", "name", "trade_status", "board"]]
        if board is not None:
            frame = frame.loc[frame["board"].eq(board)]
        frame = frame.reset_index(drop=True)
        validate_stock_list(frame, requested)
        return frame

    def fetch_stock_history(
        self, symbol: str, start_date: str | date, end_date: str | date
    ) -> pd.DataFrame:
        start_text = str(start_date)
        end_text = str(end_date)
        raw = self._query(
            f"query_history_k_data_plus({symbol},{start_text},{end_text})",
            lambda: self.bs.query_history_k_data_plus(
                symbol,
                DAILY_FIELDS,
                start_date=start_text,
                end_date=end_text,
                frequency="d",
                adjustflag="3",
            ),
        )
        frame = self._normalize_daily(raw, "query_history_k_data_plus")
        validate_daily_frame(
            frame,
            expected_symbol=symbol,
            epsilon=self.config.factor_epsilon,
        )
        if frame["date"].min() < date.fromisoformat(start_text) or frame["date"].max() > date.fromisoformat(
            end_text
        ):
            raise FatalDataError(f"{symbol} 历史行情返回请求区间外日期")
        return frame

    def fetch_market_daily(self, trade_date: str | date) -> pd.DataFrame:
        requested = date.fromisoformat(str(trade_date))
        method = getattr(self.bs, "query_daily_history_k_AStock", None)
        if method is None:
            raise ConfigurationError(
                "当前 baostock 版本不支持 query_daily_history_k_AStock()"
            )
        raw = self._query(
            f"query_daily_history_k_AStock({requested})",
            lambda: method(date=requested.isoformat()),
        )
        frame = self._normalize_daily(raw, "query_daily_history_k_AStock")
        validate_daily_frame(
            frame,
            expected_date=requested,
            epsilon=self.config.factor_epsilon,
        )
        return frame

    def fetch_market_daily_dates(self, trade_dates: Sequence[str | date]) -> pd.DataFrame:
        frames = [self.fetch_market_daily(value) for value in trade_dates]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


def np_all_integral(values: pd.Series) -> bool:
    array = values.to_numpy(dtype=float)
    return bool(((array % 1) == 0).all())
