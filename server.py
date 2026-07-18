"""QStockDataServer command-line entry point."""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from qstockdataserver.config import AppConfig, load_config
from qstockdataserver.db_manager import DuckDBManager
from qstockdataserver.exceptions import ConfigurationError, QStockError, StorageError
from qstockdataserver.logging_config import (
    FatalMarkerManager,
    log_context,
    new_run_id,
    setup_logging,
    shutdown_logging,
)
from qstockdataserver.service import StockDataService


LOGGER = logging.getLogger(__name__)


class ProcessLock:
    """Cross-platform non-blocking single-process lock."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._stream: Any | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        stream = self.path.open("a+b")
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            stream.close()
            raise ConfigurationError(
                f"已有 QStockDataServer 进程持有锁：{self.path}"
            ) from exc
        stream.seek(0)
        stream.truncate()
        stream.write(str(os.getpid()).encode("ascii"))
        stream.flush()
        self._stream = stream

    def release(self) -> None:
        if self._stream is None:
            return
        stream = self._stream
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()
            self._stream = None

    def __enter__(self) -> "ProcessLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="QStockDataServer")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command, help_text in (
        ("serve", "启动常驻数据服务"),
        ("doctor", "只读检查数据库完整性"),
        ("clear-fatal", "检查通过后清除致命错误标记"),
    ):
        sub = subparsers.add_parser(command, help=help_text)
        sub.add_argument("--config", default="config.yaml", help="配置文件路径")
        if command == "clear-fatal":
            sub.add_argument("--confirm", action="store_true", help="确认清除标记")
    return parser


def _print_existing_marker(marker: FatalMarkerManager) -> int:
    payload = marker.read()
    print("检测到致命错误标记，服务拒绝启动：", file=sys.stderr)
    print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
    print("请先运行 doctor，确认后执行 clear-fatal --confirm。", file=sys.stderr)
    try:
        return int(payload.get("exit_code", 4))
    except (TypeError, ValueError):
        return 4


def run_command(args: argparse.Namespace, config: AppConfig) -> int:
    marker = FatalMarkerManager(config.fatal_marker_path)
    database = DuckDBManager(config)
    if args.command == "doctor":
        report = database.doctor()
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 0
    if args.command == "clear-fatal":
        if not args.confirm:
            raise ConfigurationError("clear-fatal 必须显式提供 --confirm")
        report = database.doctor()
        marker.clear()
        print(
            json.dumps(
                {"fatal_marker_cleared": True, "doctor": report},
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
        return 0
    if marker.exists():
        return _print_existing_marker(marker)

    with ProcessLock(config.process_lock_path):
        service = StockDataService(config)
        try:
            service.initialize()
            return service.serve()
        except BaseException:
            service.stop()
            raise


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
    except QStockError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
        return exc.exit_code

    setup_logging(config)
    run_id = new_run_id(args.command)
    try:
        with log_context(run_id=run_id, stage=args.command):
            return run_command(args, config)
    except QStockError as exc:
        LOGGER.critical("命令失败：%s", exc, exc_info=True)
        if args.command == "serve" and exc.exit_code in {4, 5}:
            marker = FatalMarkerManager(config.fatal_marker_path)
            if not marker.exists():
                with contextlib.suppress(Exception):
                    marker.write(
                        exc,
                        exit_code=exc.exit_code,
                        stage="startup",
                        run_id=run_id,
                        log_path=config.error_log_path,
                    )
        return exc.exit_code
    except KeyboardInterrupt:
        LOGGER.info("用户中止")
        return 0
    except Exception as exc:
        wrapped = StorageError(f"未处理异常：{exc}")
        LOGGER.critical("未处理异常", exc_info=True)
        if args.command == "serve":
            with contextlib.suppress(Exception):
                FatalMarkerManager(config.fatal_marker_path).write(
                    wrapped,
                    exit_code=wrapped.exit_code,
                    stage="startup",
                    run_id=run_id,
                    log_path=config.error_log_path,
                )
        return wrapped.exit_code
    finally:
        shutdown_logging()


if __name__ == "__main__":
    raise SystemExit(main())
