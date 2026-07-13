"""Safe defaults for machine-local paths and secrets (no hardcoded home dirs)."""

from __future__ import annotations

import os

import pytest


def test_default_ipc_file_uses_stockodile_home(monkeypatch: pytest.MonkeyPatch) -> None:
    from stockodile.exchanges.base_onchain import connector as base_onchain

    monkeypatch.delenv("CUSTOM_POOLS_IPC_FILE", raising=False)
    monkeypatch.delenv("STOCKODILE_HOME", raising=False)

    path = base_onchain._default_ipc_file()
    assert path.endswith(os.path.join(".stockodile", "custom_pools_ipc.json"))
    # Must not embed a machine-specific project checkout path (home expand is fine).
    assert not path.startswith("/Users/nazmi/Stockodile/")
    assert not path.startswith("/Users/nazmi/Desktop/Stockodile/")

    monkeypatch.setenv("STOCKODILE_HOME", "/tmp/stockodile-home")
    path = base_onchain._default_ipc_file()
    assert path == os.path.join("/tmp/stockodile-home", "custom_pools_ipc.json")


def test_get_ipc_file_prefers_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from stockodile.exchanges.base_onchain import connector as base_onchain

    monkeypatch.setenv("CUSTOM_POOLS_IPC_FILE", "/tmp/custom_ipc.json")
    assert base_onchain._get_ipc_file() == "/tmp/custom_ipc.json"

    monkeypatch.delenv("CUSTOM_POOLS_IPC_FILE", raising=False)
    monkeypatch.setenv("STOCKODILE_HOME", "/tmp/stockodile-home")
    assert base_onchain._get_ipc_file() == os.path.join(
        "/tmp/stockodile-home", "custom_pools_ipc.json"
    )


def test_no_machine_home_hardcodes_in_src() -> None:
    """Regression: production source under src/stockodile must not embed /Users/nazmi paths."""
    root = os.path.join(os.path.dirname(__file__), "..", "src", "stockodile")
    root = os.path.abspath(root)
    offenders: list[str] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if not name.endswith(".py"):
                continue
            path = os.path.join(dirpath, name)
            with open(path, encoding="utf-8") as f:
                text = f.read()
            if "/Users/nazmi/" in text:
                offenders.append(path)
    assert offenders == []
