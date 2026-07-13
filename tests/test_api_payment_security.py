"""Unit tests for API payment gate, admin/metrics auth, and payments DB hardening."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest
from eth_account import Account
from eth_account.messages import encode_defunct
from fastapi.testclient import TestClient

# api_server import pulls heavy deps; allow headroom under pytest-timeout thread mode
pytestmark = pytest.mark.timeout(120)


@pytest.fixture()
def payments_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "payments_db.json"
    monkeypatch.setenv("PAYMENTS_FILE", str(path))
    monkeypatch.setenv("ALLOW_SIMULATION", "true")
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("METRICS_TOKEN", raising=False)
    return path


@pytest.fixture()
def client(payments_file: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    # Import (or re-bind) after env is set so get_payments_file() picks up tmp path.
    import stockodile.api_server as api

    monkeypatch.setattr(api, "PAYMENTS_DB", api.PersistentDict())
    # Avoid bleeding rate-limit state across tests
    api.rate_limiter.requests.clear()
    return TestClient(api.app)


def _sign_payment_id(payment_id: str, key: str = "0x" + "1" * 64) -> tuple[str, str]:
    account = Account.from_key(key)
    msg = encode_defunct(text=payment_id)
    sig = account.sign_message(msg).signature.hex()
    if not sig.startswith("0x"):
        sig = "0x" + sig
    return sig, account.address


def test_allow_simulation_default_is_false() -> None:
    """Default env value for ALLOW_SIMULATION must be false (not true)."""
    import inspect

    import stockodile.api_server as api

    # Prefer testing the helper if present; also assert source default.
    assert hasattr(api, "_allow_simulation")
    src = inspect.getsource(api._allow_simulation)
    assert '"false"' in src or "'false'" in src
    assert "true" not in src.split("ALLOW_SIMULATION")[1].split(")")[0] or (
        'os.getenv("ALLOW_SIMULATION", "false")' in src
        or "os.getenv('ALLOW_SIMULATION', 'false')" in src
    )


def test_allow_simulation_false_without_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    import stockodile.api_server as api

    monkeypatch.delenv("ALLOW_SIMULATION", raising=False)
    # Simulate production: no pytest module and no env override
    modules_without_pytest = {k: v for k, v in sys.modules.items() if k != "pytest"}
    monkeypatch.setattr(api.sys, "modules", modules_without_pytest)
    assert api._allow_simulation() is False

    monkeypatch.setenv("ALLOW_SIMULATION", "true")
    assert api._allow_simulation() is True


def test_admin_payments_hidden_without_token(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    resp = client.get("/api/v1/admin/payments")
    assert resp.status_code == 404


def test_admin_payments_requires_correct_token(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ADMIN_TOKEN", "secret-admin")
    resp = client.get("/api/v1/admin/payments")
    assert resp.status_code == 401

    resp = client.get("/api/v1/admin/payments", headers={"X-Admin-Token": "wrong"})
    assert resp.status_code == 401

    resp = client.get("/api/v1/admin/payments", headers={"X-Admin-Token": "secret-admin"})
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


def test_simulate_payment_rejects_spent(client: TestClient) -> None:
    import stockodile.api_server as api

    # Create pending payment via market-data gate
    r = client.get("/api/v1/market-data", params={"symbol": "cbBTC-USDC"})
    assert r.status_code == 402
    pid = r.json()["payment_id"]
    sig, _ = _sign_payment_id(pid)
    payload = {"payment_id": pid, "tx_hash": "0xsim1", "signature": sig}

    r = client.post("/api/v1/simulate-payment", json=payload)
    assert r.status_code == 200

    # Manually mark spent and attempt re-simulate
    rec = api.PAYMENTS_DB[pid]
    rec["status"] = "spent"
    api.PAYMENTS_DB[pid] = rec

    r = client.post(
        "/api/v1/simulate-payment",
        json={"payment_id": pid, "tx_hash": "0xsim2", "signature": sig},
    )
    assert r.status_code == 400
    assert "spent" in r.json()["detail"].lower() or "already" in r.json()["detail"].lower()


def test_simulate_payment_rejects_paid(client: TestClient) -> None:
    r = client.get("/api/v1/market-data", params={"symbol": "ETH-USDC"})
    assert r.status_code == 402
    pid = r.json()["payment_id"]
    sig, _ = _sign_payment_id(pid)
    payload = {"payment_id": pid, "tx_hash": "0xpaid1", "signature": sig}

    r = client.post("/api/v1/simulate-payment", json=payload)
    assert r.status_code == 200

    r = client.post(
        "/api/v1/simulate-payment",
        json={"payment_id": pid, "tx_hash": "0xpaid2", "signature": sig},
    )
    assert r.status_code == 400
    detail = r.json()["detail"].lower()
    assert "paid" in detail or "already" in detail or "processed" in detail


def test_symbol_binding_on_redeem(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    import stockodile.api_server as api

    r = client.get("/api/v1/market-data", params={"symbol": "cbBTC-USDC"})
    assert r.status_code == 402
    pid = r.json()["payment_id"]
    sig, _ = _sign_payment_id(pid)
    payload = {"payment_id": pid, "tx_hash": "0xbind1", "signature": sig}
    assert client.post("/api/v1/simulate-payment", json=payload).status_code == 200

    async def fake_price(symbol: str, rpc_url: str | None = None) -> dict[str, Any]:
        return {"symbol": symbol, "price": 1.0}

    monkeypatch.setattr(api, "get_onchain_price", fake_price)

    # Wrong symbol must be rejected
    r = client.get(
        "/api/v1/market-data",
        params={"symbol": "OTHER-USDC"},
        headers={"Payment-Signature": json.dumps(payload)},
    )
    assert r.status_code == 400
    assert "symbol" in r.json()["detail"].lower()

    # Correct symbol should succeed (mocked price)
    r = client.get(
        "/api/v1/market-data",
        params={"symbol": "cbBTC-USDC"},
        headers={"Payment-Signature": json.dumps(payload)},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "success"

    # Payment is spent — cannot reuse
    r = client.get(
        "/api/v1/market-data",
        params={"symbol": "cbBTC-USDC"},
        headers={"Payment-Signature": json.dumps(payload)},
    )
    assert r.status_code == 400
    assert "spent" in r.json()["detail"].lower()


def test_two_phase_refund_on_data_failure(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    import stockodile.api_server as api

    r = client.get("/api/v1/market-data", params={"symbol": "cbBTC-USDC"})
    pid = r.json()["payment_id"]
    sig, _ = _sign_payment_id(pid)
    payload = {"payment_id": pid, "tx_hash": "0xrefund1", "signature": sig}
    assert client.post("/api/v1/simulate-payment", json=payload).status_code == 200

    async def boom(symbol: str, rpc_url: str | None = None) -> dict[str, Any]:
        return {"error": "rpc down"}

    monkeypatch.setattr(api, "get_onchain_price", boom)

    r = client.get(
        "/api/v1/market-data",
        params={"symbol": "cbBTC-USDC"},
        headers={"Payment-Signature": json.dumps(payload)},
    )
    assert r.status_code == 500

    rec = api.PAYMENTS_DB[pid]
    # Should be refunded to paid (not spent) so client can retry
    assert rec["status"] == "paid"


def test_metrics_token_enforced(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("METRICS_TOKEN", "m-secret")
    r = client.get("/metrics")
    assert r.status_code == 401

    r = client.get("/metrics", headers={"X-Metrics-Token": "m-secret"})
    assert r.status_code == 200
    assert "stockodile_uptime_seconds" in r.text

    r = client.get("/metrics", headers={"Authorization": "Bearer m-secret"})
    assert r.status_code == 200


def test_metrics_open_without_token(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("METRICS_TOKEN", raising=False)
    r = client.get("/metrics")
    assert r.status_code == 200


def test_atomic_save_uses_replace(payments_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import stockodile.api_server as api

    monkeypatch.setenv("PAYMENTS_FILE", str(payments_file))
    data = {"abc": {"status": "pending", "symbol": "X"}}
    api._save_db_file(data)
    assert payments_file.exists()
    assert not Path(str(payments_file) + ".tmp").exists()
    loaded = json.loads(payments_file.read_text())
    assert loaded == data


def test_default_payments_file_path(monkeypatch: pytest.MonkeyPatch) -> None:
    import stockodile.api_server as api

    monkeypatch.delenv("STOCKODILE_HOME", raising=False)
    path = api._default_payments_file()
    assert path.endswith(os.path.join(".stockodile", "payments_db.json"))
    assert not path.startswith("/Users/nazmi/Stockodile/")

    monkeypatch.setenv("STOCKODILE_HOME", "/tmp/stockodile-home")
    path = api._default_payments_file()
    assert path == os.path.join("/tmp/stockodile-home", "payments_db.json")
