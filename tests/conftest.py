from __future__ import annotations

from pathlib import Path

import pytest

from config import AppConfig, load_config


@pytest.fixture
def app_config(tmp_path: Path) -> AppConfig:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                'database_path: "data/test.duckdb"',
                'start_date: "2018-01-01"',
                'update_time: "18:30"',
                'timezone: "Asia/Shanghai"',
                "retry_interval_minutes: 0",
                "max_retries: 2",
                "initial_import_batch_size: 10",
                "factor_epsilon: 1.0e-10",
                'flight_host: "127.0.0.1"',
                "flight_port: 18815",
                'runtime_dir: "runtime"',
                'log_path: "logs/main.log"',
                'error_log_path: "logs/error.log"',
                'log_level: "DEBUG"',
                "log_max_bytes: 100000",
                "log_backup_count: 2",
                "query_max_rows: 100000",
                "query_max_sql_length: 10000",
                "duckdb_threads: 2",
            ]
        ),
        encoding="utf-8",
    )
    return load_config(config_path)
