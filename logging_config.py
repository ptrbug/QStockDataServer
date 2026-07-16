"""Central logging, contextual fields, and fatal marker management."""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import logging.handlers
import os
import sys
import tempfile
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from config import AppConfig


_run_id = contextvars.ContextVar("run_id", default="-")
_stage = contextvars.ContextVar("stage", default="-")
_trade_date = contextvars.ContextVar("trade_date", default="-")
_symbol = contextvars.ContextVar("symbol", default="-")


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _run_id.get()
        record.stage = _stage.get()
        record.trade_date = _trade_date.get()
        record.symbol = _symbol.get()
        return True


class MaxLevelFilter(logging.Filter):
    def __init__(self, maximum: int) -> None:
        super().__init__()
        self.maximum = maximum

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.maximum


@contextlib.contextmanager
def log_context(
    *,
    run_id: str | None = None,
    stage: str | None = None,
    trade_date: str | None = None,
    symbol: str | None = None,
) -> Iterator[None]:
    tokens: list[tuple[contextvars.ContextVar[str], contextvars.Token[str]]] = []
    for variable, value in (
        (_run_id, run_id),
        (_stage, stage),
        (_trade_date, trade_date),
        (_symbol, symbol),
    ):
        if value is not None:
            tokens.append((variable, variable.set(value)))
    try:
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def setup_logging(config: AppConfig) -> logging.Logger:
    config.log_path.parent.mkdir(parents=True, exist_ok=True)
    config.error_log_path.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, config.log_level))
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s run=%(run_id)s stage=%(stage)s "
        "date=%(trade_date)s symbol=%(symbol)s %(name)s: %(message)s"
    )
    context_filter = ContextFilter()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(getattr(logging, config.log_level))
    stdout_handler.addFilter(context_filter)
    stdout_handler.addFilter(MaxLevelFilter(logging.WARNING))
    stdout_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.addFilter(context_filter)
    stderr_handler.setFormatter(formatter)

    file_handler = logging.handlers.RotatingFileHandler(
        config.log_path,
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(getattr(logging, config.log_level))
    file_handler.addFilter(context_filter)
    file_handler.setFormatter(formatter)

    error_handler = logging.handlers.RotatingFileHandler(
        config.error_log_path,
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.addFilter(context_filter)
    error_handler.setFormatter(formatter)

    root.addHandler(stdout_handler)
    root.addHandler(stderr_handler)
    root.addHandler(file_handler)
    root.addHandler(error_handler)
    return logging.getLogger("qstock")


def shutdown_logging() -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        try:
            handler.flush()
        finally:
            handler.close()
            root.removeHandler(handler)
    logging.shutdown()


class FatalMarkerManager:
    def __init__(self, marker_path: Path) -> None:
        self.path = marker_path

    def exists(self) -> bool:
        return self.path.is_file()

    def read(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return {"message": f"致命错误标记无法读取：{exc}", "path": str(self.path)}
        return value if isinstance(value, dict) else {"message": str(value)}

    def write(
        self,
        error: BaseException,
        *,
        exit_code: int,
        stage: str,
        run_id: str,
        log_path: Path,
        trade_date: str | None = None,
        symbol: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "exit_code": exit_code,
            "error_type": type(error).__name__,
            "message": str(error),
            "stage": stage,
            "run_id": run_id,
            "trade_date": trade_date,
            "symbol": symbol,
            "log_path": str(log_path),
            "traceback": "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            ),
        }
        if details:
            payload["details"] = details

        encoded = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(temporary)
            raise
        return payload

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)
