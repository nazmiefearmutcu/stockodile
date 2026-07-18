"""Hermetic tests for the GET /api/v1/depth/{symbol} endpoint (no network).

The facade `select_depth_source` is monkeypatched to yield a canned
`DepthProfile`, so these tests never hit live Yahoo/Alpaca.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# api_server needs web3 + fastapi (stockodile[full]); TestClient needs httpx.
pytest.importorskip("web3")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from stockodile.schema.records import DepthProfile

pytestmark = pytest.mark.timeout(120)


def _profile(*, is_synthetic: bool) -> DepthProfile:
    return DepthProfile(
        provider="synthetic-yahoo" if is_synthetic else "alpaca",
        symbol="AAPL",
        symbol_raw="AAPL",
        local_ts=1_700_000_000_000_000_000,
        bids=[(189.5, 120.0), (189.4, 80.0)],
        asks=[(190.5, 110.0), (190.6, 60.0)],
        reference_price=190.0,
        basis="vap" if is_synthetic else "l1",
        is_synthetic=is_synthetic,
        depth=2,
        source_ts=None,
    )


class _FakeSource:
    def __init__(self, profile: DepthProfile) -> None:
        self._profile = profile

    async def snapshot(self, symbol: str) -> DepthProfile:
        return self._profile


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("PAYMENTS_FILE", str(tmp_path / "payments_db.json"))
    import stockodile.api_server as api

    monkeypatch.setattr(api, "PAYMENTS_DB", api.PersistentDict())
    api.rate_limiter.requests.clear()
    return TestClient(api.app)


def _patch_facade(monkeypatch: pytest.MonkeyPatch, profile: DepthProfile) -> None:
    import stockodile.depth as depth_pkg

    def _fake_select(*, bins: int = 40, top_n: int = 10, method: str = "uniform") -> _FakeSource:
        return _FakeSource(profile)

    monkeypatch.setattr(depth_pkg, "select_depth_source", _fake_select)


def test_depth_synthetic_includes_warning(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_facade(monkeypatch, _profile(is_synthetic=True))
    resp = client.get("/api/v1/depth/AAPL")
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["basis"] == "vap"
    assert body["is_synthetic"] is True
    assert body["reference_price"] == 190.0
    assert body["depth"] == 2
    assert body["bids"] == [[189.5, 120.0], [189.4, 80.0]]
    assert body["asks"] == [[190.5, 110.0], [190.6, 60.0]]
    assert body["warning"] == (
        "SYNTHETIC — relative volume-at-price from Yahoo 1m bars, "
        "not real resting liquidity."
    )


def test_depth_real_has_no_warning(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_facade(monkeypatch, _profile(is_synthetic=False))
    resp = client.get("/api/v1/depth/AAPL")
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_synthetic"] is False
    assert "warning" not in body


def test_depth_provider_error_maps_to_400(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    import stockodile.depth as depth_pkg

    class _BoomSource:
        async def snapshot(self, symbol: str) -> DepthProfile:
            raise ValueError("unknown symbol")

    def _fake_select(*, bins: int = 40, top_n: int = 10, method: str = "uniform") -> _BoomSource:
        return _BoomSource()

    monkeypatch.setattr(depth_pkg, "select_depth_source", _fake_select)
    resp = client.get("/api/v1/depth/BADSYM")
    assert resp.status_code == 400
