import asyncio

import pytest

from stockodile.depth.alpaca_l1 import AlpacaL1DepthSource
from stockodile.depth.select import select_depth_source
from stockodile.depth.synthetic import SyntheticYahooDepthSource
from stockodile.depth.synthetic import SyntheticYahooDepthSource as _Synth
from stockodile.schema.records import Bar, DepthProfile


def _bar(c, v, ts):
    return Bar(provider="yahoo", symbol="yahoo:AAPL", symbol_raw="AAPL", local_ts=ts,
               interval="1m", open=c, high=c + 1, low=c - 1, close=c, volume=v)


class _FakeYahoo:
    async def fetch_intraday_bars(self, symbol, interval, start=None, end=None):
        return [_bar(100, 500, 1), _bar(101, 700, 2), _bar(99, 300, 3)]


def test_synthetic_source_builds_labeled_profile():
    src = SyntheticYahooDepthSource(client=_FakeYahoo(), bins=20, top_n=5)
    prof = asyncio.run(src.snapshot("AAPL"))
    assert isinstance(prof, DepthProfile)
    assert prof.is_synthetic is True
    assert prof.basis == "yahoo_1m_vap"
    assert prof.provider == "synth"
    assert prof.reference_price == 99.0  # last close
    assert prof.depth == len(prof.bids) + len(prof.asks)


def test_select_returns_synthetic_without_key(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    src = select_depth_source()
    assert isinstance(src, _Synth)


def test_select_returns_alpaca_with_key(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_API_SECRET", "s")
    src = select_depth_source()
    assert isinstance(src, AlpacaL1DepthSource)


def test_alpaca_l1_parses_quote(monkeypatch):
    payload = {"quote": {"bp": 100.1, "bs": 3, "ap": 100.2, "as": 4, "t": "2026-07-16T15:00:00Z"}}

    class _FakeResp:
        status = 200
        async def json(self): return payload
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    class _FakeSession:
        def __init__(self, *a, **k): pass
        def get(self, *a, **k): return _FakeResp()
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    monkeypatch.setattr("stockodile.depth.alpaca_l1.aiohttp.ClientSession", _FakeSession)
    src = AlpacaL1DepthSource(key="k", secret="s")
    prof = asyncio.run(src.snapshot("AAPL"))
    assert prof.is_synthetic is False
    assert prof.basis == "alpaca_l1"
    assert prof.bids == [(100.1, 3.0)]
    assert prof.asks == [(100.2, 4.0)]
    assert prof.reference_price == pytest.approx((100.1 + 100.2) / 2)


def test_alpaca_l1_auth_failure_surfaces_not_silent(monkeypatch):
    # A 401 MUST raise (surface the failure). It must NOT silently degrade to synthetic —
    # that would hide a real auth problem and serve fake data as if it were real L1.
    class _FakeResp:
        status = 401
        async def json(self): return {}
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    class _FakeSession:
        def __init__(self, *a, **k): pass
        def get(self, *a, **k): return _FakeResp()
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    monkeypatch.setattr("stockodile.depth.alpaca_l1.aiohttp.ClientSession", _FakeSession)
    src = AlpacaL1DepthSource(key="k", secret="s")
    with pytest.raises(ValueError, match="auth failed"):
        asyncio.run(src.snapshot("AAPL"))


def test_alpaca_l1_sends_auth_headers_and_upper_symbol(monkeypatch):
    captured: dict = {}

    class _FakeResp:
        status = 200
        async def json(self):
            return {"quote": {"bp": 1.0, "bs": 1, "ap": 2.0, "as": 1, "t": "2026-07-16T15:00:00Z"}}
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    class _FakeSession:
        def __init__(self, *a, **k): captured["headers"] = k.get("headers")
        def get(self, url, params=None, **k):
            captured["url"] = url
            captured["params"] = params
            return _FakeResp()
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    monkeypatch.setattr("stockodile.depth.alpaca_l1.aiohttp.ClientSession", _FakeSession)
    src = AlpacaL1DepthSource(key="mykey", secret="mysecret", feed="iex")
    asyncio.run(src.snapshot("aapl"))
    assert captured["headers"]["APCA-API-KEY-ID"] == "mykey"
    assert captured["headers"]["APCA-API-SECRET-KEY"] == "mysecret"
    assert captured["params"]["feed"] == "iex"
    assert captured["url"].endswith("/stocks/AAPL/quotes/latest")  # symbol upper-cased in path
