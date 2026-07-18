"""Typed YAML configuration with deterministic path resolution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .exceptions import ConfigurationError


SUPPORTED_BOARDS = ("zb", "cyb", "kcb")


def _parse_date(value: Any, key: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{key} 必须是 YYYY-MM-DD，实际为 {value!r}") from exc


def _parse_time(value: Any, key: str) -> time:
    try:
        return time.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{key} 必须是 HH:MM 或 HH:MM:SS，实际为 {value!r}") from exc


def _positive_int(value: Any, key: str, *, allow_zero: bool = False) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{key} 必须是整数") from exc
    minimum = 0 if allow_zero else 1
    if parsed < minimum:
        raise ConfigurationError(f"{key} 必须大于等于 {minimum}")
    return parsed


def _resolve(base_dir: Path, value: Any, key: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{key} 必须是非空路径")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


@dataclass(frozen=True, slots=True)
class AppConfig:
    config_path: Path
    database_path: Path
    boards: tuple[str, ...]
    start_date: date
    update_time: time
    timezone: ZoneInfo
    retry_delays_seconds: tuple[int, ...]
    max_retries: int
    session_max_minutes: int
    factor_epsilon: float
    flight_host: str
    flight_port: int
    runtime_dir: Path
    log_path: Path
    error_log_path: Path
    log_level: str
    log_max_bytes: int
    log_backup_count: int
    query_max_rows: int
    query_max_sql_length: int
    duckdb_threads: int

    @property
    def fatal_marker_path(self) -> Path:
        return self.runtime_dir / "FATAL_ERROR.json"

    @property
    def process_lock_path(self) -> Path:
        return self.runtime_dir / "qstockdataserver.lock"


DEFAULTS: dict[str, Any] = {
    "database_path": "data/stock_daily.duckdb",
    "boards": ["zb", "cyb"],
    "start_date": "2018-01-01",
    "update_time": "18:30",
    "timezone": "Asia/Shanghai",
    "retry_delays_seconds": [3, 30, 120, 300],
    "max_retries": 12,
    "session_max_minutes": 30,
    "factor_epsilon": 1.0e-10,
    "flight_host": "127.0.0.1",
    "flight_port": 8815,
    "runtime_dir": "runtime",
    "log_path": "logs/qstockdataserver.log",
    "error_log_path": "logs/qstockdataserver.error.log",
    "log_level": "INFO",
    "log_max_bytes": 10 * 1024 * 1024,
    "log_backup_count": 10,
    "query_max_rows": 5_000_000,
    "query_max_sql_length": 100_000,
    "duckdb_threads": 4,
}


def load_config(path: str | Path) -> AppConfig:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - dependency error path
        raise ConfigurationError("缺少 PyYAML，请先安装 requirements.txt") from exc

    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise ConfigurationError(f"配置文件不存在：{config_path}")
    try:
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigurationError(f"无法读取配置文件 {config_path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ConfigurationError("config.yaml 顶层必须是 key/value 映射")

    values = {**DEFAULTS, **loaded}
    base_dir = config_path.parent
    try:
        timezone = ZoneInfo(str(values["timezone"]))
    except ZoneInfoNotFoundError as exc:
        raise ConfigurationError(f"未知时区：{values['timezone']!r}") from exc

    try:
        factor_epsilon = float(values["factor_epsilon"])
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("factor_epsilon 必须是浮点数") from exc
    if not 0 < factor_epsilon < 0.01:
        raise ConfigurationError("factor_epsilon 必须在 0 与 0.01 之间")

    host = str(values["flight_host"]).strip()
    if not host:
        raise ConfigurationError("flight_host 不能为空")
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ConfigurationError(
            "Flight 服务未实现身份认证，flight_host 只能绑定本机回环地址"
        )
    port = _positive_int(values["flight_port"], "flight_port")
    if port > 65535:
        raise ConfigurationError("flight_port 必须小于等于 65535")

    level = str(values["log_level"]).upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigurationError(f"不支持的 log_level：{level}")

    raw_delays = values["retry_delays_seconds"]
    if not isinstance(raw_delays, list) or not raw_delays:
        raise ConfigurationError("retry_delays_seconds 必须是非空整数列表")
    retry_delays = tuple(
        _positive_int(value, "retry_delays_seconds", allow_zero=True)
        for value in raw_delays
    )

    raw_boards = values["boards"]
    if not isinstance(raw_boards, list) or not raw_boards:
        raise ConfigurationError("boards 必须是非空列表")
    boards = tuple(str(value).strip().lower() for value in raw_boards)
    if any(not value for value in boards):
        raise ConfigurationError("boards 不能包含空值")
    if len(set(boards)) != len(boards):
        raise ConfigurationError("boards 不能包含重复项")
    unsupported = sorted(set(boards) - set(SUPPORTED_BOARDS))
    if unsupported:
        raise ConfigurationError(
            f"boards 包含不支持的板块：{unsupported}；可选值为 {list(SUPPORTED_BOARDS)}"
        )

    return AppConfig(
        config_path=config_path,
        database_path=_resolve(base_dir, values["database_path"], "database_path"),
        boards=boards,
        start_date=_parse_date(values["start_date"], "start_date"),
        update_time=_parse_time(values["update_time"], "update_time"),
        timezone=timezone,
        retry_delays_seconds=retry_delays,
        max_retries=_positive_int(values["max_retries"], "max_retries"),
        session_max_minutes=_positive_int(
            values["session_max_minutes"], "session_max_minutes"
        ),
        factor_epsilon=factor_epsilon,
        flight_host=host,
        flight_port=port,
        runtime_dir=_resolve(base_dir, values["runtime_dir"], "runtime_dir"),
        log_path=_resolve(base_dir, values["log_path"], "log_path"),
        error_log_path=_resolve(base_dir, values["error_log_path"], "error_log_path"),
        log_level=level,
        log_max_bytes=_positive_int(values["log_max_bytes"], "log_max_bytes"),
        log_backup_count=_positive_int(
            values["log_backup_count"], "log_backup_count", allow_zero=True
        ),
        query_max_rows=_positive_int(values["query_max_rows"], "query_max_rows"),
        query_max_sql_length=_positive_int(
            values["query_max_sql_length"], "query_max_sql_length"
        ),
        duckdb_threads=_positive_int(values["duckdb_threads"], "duckdb_threads"),
    )
