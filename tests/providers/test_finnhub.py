from __future__ import annotations

import json
import os

import pytest

from stockodile.providers.finnhub.connector import FinnhubProvider
from stockodile.reference.registry import InstrumentRegistry
from stockodile.schema.records import Trade
from stockodile.sink.base import MemorySink


class MockTransport:
    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.closed = False

    async def connect(self) -> None:
        pass

    def __aiter__(self) -> MockTransport:
        return self

    async def __anext__(self) -> bytes:
        raise StopAsyncIteration

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        self.closed = True


def test_finnhub_provider_init_credentials() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()

    # Missing credentials in env and params
    if "FINNHUB_API_KEY" in os.environ:
        del os.environ["FINNHUB_API_KEY"]

    with pytest.raises(ValueError, match="Finnhub API key missing"):
        FinnhubProvider(
            symbols=["AAPL"],
            channels=["trade"],
            out=sink,
            registry=registry,
        )

    # Valid credentials via params
    provider = FinnhubProvider(
        symbols=["AAPL"],
        channels=["trade"],
        out=sink,
        registry=registry,
        token="test_token",
    )
    assert provider.token == "test_token"
    assert provider.ws_url == "wss://ws.finnhub.io?token=test_token"


def test_finnhub_provider_symbol_cap() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    symbols = [f"SYM{i}" for i in range(55)]

    with pytest.raises(ValueError, match="Finnhub free tier WebSocket limits"):
        FinnhubProvider(
            symbols=symbols,
            channels=["trade"],
            out=sink,
            registry=registry,
            token="test_token",
        )


@pytest.mark.asyncio
async def test_finnhub_provider_subscribe() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = FinnhubProvider(
        symbols=["AAPL", "MSFT"],
        channels=["trade"],
        out=sink,
        registry=registry,
        token="test_token",
    )

    transport = MockTransport()
    await provider._subscribe(transport)

    assert len(transport.sent) == 2
    sub1 = json.loads(transport.sent[0])
    sub2 = json.loads(transport.sent[1])

    assert sub1 == {"type": "subscribe", "symbol": "AAPL"}
    assert sub2 == {"type": "subscribe", "symbol": "MSFT"}


def test_finnhub_normalize_trade() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = FinnhubProvider(
        symbols=["AAPL"],
        channels=["trade"],
        out=sink,
        registry=registry,
        token="test_token",
    )

    raw_msg = {
        "type": "trade",
        "data": [
            {
                "s": "AAPL",
                "p": 150.25,
                "v": 100,
                "t": 1770000000123,  # Milliseconds since epoch
                "c": ["1", "12"],
            }
        ],
    }

    records = list(provider.normalize(raw_msg, local_ts=999))
    assert len(records) == 1
    trade = records[0]
    assert isinstance(trade, Trade)
    assert trade.provider == "finnhub"
    assert trade.symbol == "AAPL"
    assert trade.symbol_raw == "AAPL"
    assert trade.price == 150.25
    assert trade.size == 100.0
    assert trade.source_ts == 1770000000123 * 1_000_000
    assert trade.local_ts == 999
    assert trade.venue is None
    assert trade.conditions == ["1", "12"]
    assert trade.id == ""
    assert trade.tape is None


@pytest.mark.asyncio
async def test_finnhub_run_connection_limit() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = FinnhubProvider(
        symbols=["AAPL"],
        channels=["trade"],
        out=sink,
        registry=registry,
        token="test_token",
    )

    # Mark as running manually and try to run
    provider._running = True
    with pytest.raises(RuntimeError, match="FinnhubProvider is already running"):
        await provider.run()


def test_finnhub_normalize_item_error_recovery() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = FinnhubProvider(
        symbols=["AAPL"],
        channels=["trade"],
        out=sink,
        registry=registry,
        token="test_token",
    )

    # First item is malformed (missing 'p' key), second is valid
    raw_msg = {
        "type": "trade",
        "data": [
            {
                "s": "AAPL",
                "v": 100,
                "t": 1770000000123,
                # missing "p"
            },
            {
                "s": "AAPL",
                "p": 150.25,
                "v": 100,
                "t": 1770000000123,
            },
        ],
    }

    records = list(provider.normalize(raw_msg, local_ts=999))
    assert len(records) == 1
    trade = records[0]
    assert isinstance(trade, Trade)
    assert trade.price == 150.25


def test_finnhub_fatal_auth_error() -> None:
    from stockodile.providers.finnhub.connector import FatalProviderError

    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = FinnhubProvider(
        symbols=["AAPL"],
        channels=["trade"],
        out=sink,
        registry=registry,
        token="test_token",
    )

    # Error message from Finnhub indicating invalid token
    raw_msg = {"type": "error", "msg": "Invalid API key/token"}

    with pytest.raises(FatalProviderError, match="Finnhub authentication failed"):
        list(provider.normalize(raw_msg, local_ts=999))


def test_finnhub_mid_session_error() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = FinnhubProvider(
        symbols=["AAPL"],
        channels=["trade"],
        out=sink,
        registry=registry,
        token="test_token",
    )

    # Other WebSocket error
    raw_msg = {"type": "error", "msg": "subscription limit exceeded"}

    with pytest.raises(ValueError, match="subscription limit exceeded"):
        list(provider.normalize(raw_msg, local_ts=999))
