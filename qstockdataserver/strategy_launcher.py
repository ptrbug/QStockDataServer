"""Fire-and-forget strategy program launcher."""

from __future__ import annotations

import logging
import subprocess

from .config import StrategyProgramsConfig


LOGGER = logging.getLogger(__name__)
LOCAL_FLIGHT_HOST = "127.0.0.1"


class StrategyProgramLauncher:
    def __init__(self, config: StrategyProgramsConfig) -> None:
        self.config = config

    def launch_all(self, *, flight_port: int, snapshot_version: str) -> None:
        if not self.config.enabled:
            return
        server = f"grpc://{LOCAL_FLIGHT_HOST}:{flight_port}"
        extra_args = ["--server", server, "--version", snapshot_version]
        for item in self.config.items:
            argv = [*item.command, *extra_args]
            try:
                process = subprocess.Popen(
                    argv,
                    cwd=str(item.cwd) if item.cwd is not None else None,
                    close_fds=True,
                )
            except Exception:
                LOGGER.exception(
                    "启动策略程序失败：name=%s version=%s command=%s",
                    item.name,
                    snapshot_version,
                    item.command,
                )
                continue
            LOGGER.info(
                "已启动策略程序：name=%s pid=%s version=%s server=%s",
                item.name,
                process.pid,
                snapshot_version,
                server,
            )
