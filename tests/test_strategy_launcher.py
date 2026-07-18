from __future__ import annotations

from pathlib import Path

import pytest

from qstockdataserver.config import (
    StrategyProgramConfig,
    StrategyProgramsConfig,
    load_config,
)
from qstockdataserver.exceptions import ConfigurationError
from qstockdataserver.strategy_launcher import StrategyProgramLauncher


def test_strategy_launcher_appends_default_flight_arguments(monkeypatch) -> None:
    calls: list[tuple[list[str], str | None, bool]] = []

    class Process:
        pid = 12345

    def fake_popen(argv, *, cwd=None, close_fds=False):
        calls.append((list(argv), cwd, close_fds))
        return Process()

    monkeypatch.setattr("qstockdataserver.strategy_launcher.subprocess.Popen", fake_popen)
    cwd = Path("strategies/momentum")
    launcher = StrategyProgramLauncher(
        StrategyProgramsConfig(
            enabled=True,
            items=(
                StrategyProgramConfig(
                    name="momentum",
                    command=("python", "D:/strategies/momentum/main.py"),
                    cwd=cwd,
                ),
            ),
        )
    )

    launcher.launch_all(flight_port=8815, snapshot_version="2024-01-05")

    assert calls == [
        (
            [
                "python",
                "D:/strategies/momentum/main.py",
                "--server",
                "grpc://127.0.0.1:8815",
                "--version",
                "2024-01-05",
            ],
            str(cwd),
            True,
        )
    ]


def test_strategy_launcher_disabled_does_not_start_process(monkeypatch) -> None:
    calls: list[object] = []
    monkeypatch.setattr(
        "qstockdataserver.strategy_launcher.subprocess.Popen",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    launcher = StrategyProgramLauncher(
        StrategyProgramsConfig(
            enabled=False,
            items=(StrategyProgramConfig(name="momentum", command=("strategy",)),),
        )
    )

    launcher.launch_all(flight_port=8815, snapshot_version="2024-01-05")

    assert calls == []


def test_strategy_programs_config_is_loaded_from_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    strategy_path = tmp_path / "strategy.yaml"
    config_path.write_text("", encoding="utf-8")
    strategy_path.write_text(
        "\n".join(
            [
                "strategy_programs:",
                "  enabled: true",
                "  items:",
                "    - name: momentum",
                "      command:",
                "        - python",
                "        - strategies/momentum/main.py",
                "      cwd: strategies/momentum",
            ]
        ),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.strategy_config_path == strategy_path
    assert config.strategy_programs.enabled is True
    assert len(config.strategy_programs.items) == 1
    item = config.strategy_programs.items[0]
    assert item.name == "momentum"
    assert item.command == ("python", "strategies/momentum/main.py")
    assert item.cwd == (tmp_path / "strategies" / "momentum").resolve()


def test_strategy_programs_enabled_must_be_boolean(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    strategy_path = tmp_path / "strategy.yaml"
    config_path.write_text("", encoding="utf-8")
    strategy_path.write_text(
        "\n".join(["strategy_programs:", '  enabled: "false"', "  items: []"]),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="strategy_programs.enabled"):
        load_config(config_path)


def test_missing_strategy_yaml_disables_strategy_programs(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("", encoding="utf-8")

    config = load_config(config_path)

    assert config.strategy_config_path == tmp_path / "strategy.yaml"
    assert config.strategy_programs.enabled is False
    assert config.strategy_programs.items == ()
