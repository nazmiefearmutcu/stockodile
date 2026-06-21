from __future__ import annotations

import json
import os

import pytest

from stockodile.providers.alpaca.connector import AlpacaProvider
from stockodile.reference.registry import InstrumentRegistry
from stockodile.schema.records import Bar, Quote, Trade
from stockodile.sink.base import MemorySink


class MockTransport:
    def __init__(self, responses: list[bytes]) -> None:
        self.responses = responses
        self.sent: list[bytes] = []
        self.index = 0

    async def connect(self) -> None:
        pass

    def __aiter__(self) -> MockTransport:
        return self

    async def __anext__(self) -> bytes:
        if self.index >= len(self.responses):
            raise StopAsyncIteration
        res = self.responses[self.index]
        self.index += 1
        return res

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        pass


def test_alpaca_provider_init_credentials() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()

    # Missing credentials in env and params
    if "ALPACA_API_KEY" in os.environ:
        del os.environ["ALPACA_API_KEY"]
    if "ALPACA_API_SECRET" in os.environ:
        del os.environ["ALPACA_API_SECRET"]

    with pytest.raises(ValueError, match="Alpaca API credentials missing"):
        AlpacaProvider(
            symbols=["AAPL"],
            channels=["trade"],
            out=sink,
            registry=registry,
        )

    # Valid credentials via params
    provider = AlpacaProvider(
        symbols=["AAPL"],
        channels=["trade"],
        out=sink,
        registry=registry,
        key="test_key",
        secret="test_secret",
    )
    assert provider.key == "test_key"
    assert provider.secret == "test_secret"


def test_alpaca_provider_symbol_cap() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    symbols = [f"SYM{i}" for i in range(35)]

    # Limit exceeded for trade/quote channels
    with pytest.raises(ValueError, match="Alpaca basic plan WebSocket limits"):
        AlpacaProvider(
            symbols=symbols,
            channels=["trade"],
            out=sink,
            registry=registry,
            key="test_key",
            secret="test_secret",
        )

    # Bar channel is uncapped
    provider = AlpacaProvider(
        symbols=symbols,
        channels=["bar"],
        out=sink,
        registry=registry,
        key="test_key",
        secret="test_secret",
    )
    assert len(provider.symbols) == 35


@pytest.mark.asyncio
async def test_alpaca_provider_subscribe() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = AlpacaProvider(
        symbols=["AAPL", "MSFT"],
        channels=["trade", "quote", "bar"],
        out=sink,
        registry=registry,
        key="test_key",
        secret="test_secret",
    )

    # Prepare mocked responses
    greeting = b'[{"T":"success","msg":"connected"}]'
    auth_success = b'[{"T":"success","msg":"authenticated"}]'
    transport = MockTransport([greeting, auth_success])
    provider.transport = transport

    await provider._subscribe(transport)

    assert len(transport.sent) == 2
    auth_sent = json.loads(transport.sent[0])
    sub_sent = json.loads(transport.sent[1])

    assert auth_sent == {
        "action": "auth",
        "key": "test_key",
        "secret": "test_secret",
    }
    assert sub_sent == {
        "action": "subscribe",
        "trades": ["AAPL", "MSFT"],
        "quotes": ["AAPL", "MSFT"],
        "bars": ["AAPL", "MSFT"],
    }


def test_alpaca_normalize_trade() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = AlpacaProvider(
        symbols=["AAPL"],
        channels=["trade"],
        out=sink,
        registry=registry,
        key="test_key",
        secret="test_secret",
    )

    raw_msg = [
        {
            "T": "t",
            "S": "AAPL",
            "p": 150.25,
            "s": 100,
            "t": "2026-06-21T17:48:18.123456789Z",
            "x": "V",
            "c": ["@", "I"],
            "i": 123456,
            "z": "C",
        }
    ]

    records = list(provider.normalize(raw_msg, local_ts=999))
    assert len(records) == 1
    trade = records[0]
    assert isinstance(trade, Trade)
    assert trade.provider == "alpaca"
    assert trade.symbol == "AAPL"
    assert trade.symbol_raw == "AAPL"
    assert trade.price == 150.25
    assert trade.size == 100.0
    assert trade.venue == "V"
    assert trade.conditions == ["@", "I"]
    assert trade.id == "123456"
    assert trade.tape is not None
    assert trade.tape.value == "C"


def test_alpaca_normalize_quote() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = AlpacaProvider(
        symbols=["AAPL"],
        channels=["quote"],
        out=sink,
        registry=registry,
        key="test_key",
        secret="test_secret",
    )

    raw_msg = [
        {
            "T": "q",
            "S": "AAPL",
            "bp": 150.20,
            "bs": 5,
            "ap": 150.30,
            "as": 8,
            "t": "2026-06-21T17:48:18.123456789Z",
            "bx": "V",
            "ax": "V",
            "z": "C",
        }
    ]

    records = list(provider.normalize(raw_msg, local_ts=999))
    assert len(records) == 1
    quote = records[0]
    assert isinstance(quote, Quote)
    assert quote.provider == "alpaca"
    assert quote.symbol == "AAPL"
    assert quote.bid_px == 150.20
    assert quote.bid_sz == 5.0
    assert quote.ask_px == 150.30
    assert quote.ask_sz == 8.0
    assert not quote.is_nbbo
    assert not quote.is_consolidated
    assert quote.tape is not None
    assert quote.tape.value == "C"


def test_alpaca_normalize_bar() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = AlpacaProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
        key="test_key",
        secret="test_secret",
    )

    raw_msg = [
        {
            "T": "b",
            "S": "AAPL",
            "o": 150.0,
            "h": 151.0,
            "l": 149.5,
            "c": 150.5,
            "v": 10000,
            "vw": 150.25,
            "n": 50,
            "t": "2026-06-21T17:48:00Z",
        }
    ]

    records = list(provider.normalize(raw_msg, local_ts=999))
    assert len(records) == 1
    bar = records[0]
    assert isinstance(bar, Bar)
    assert bar.provider == "alpaca"
    assert bar.symbol == "AAPL"
    assert bar.open == 150.0
    assert bar.high == 151.0
    assert bar.low == 149.5
    assert bar.close == 150.5
    assert bar.volume == 10000.0
    assert bar.vwap == 150.25
    assert bar.trade_count == 50


@pytest.mark.asyncio
async def test_alpaca_run_connection_limit() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = AlpacaProvider(
        symbols=["AAPL"],
        channels=["trade"],
        out=sink,
        registry=registry,
        key="test_key",
        secret="test_secret",
    )

    greeting = b'[{"T":"success","msg":"connected"}]'
    auth_success = b'[{"T":"success","msg":"authenticated"}]'
    transport = MockTransport([greeting, auth_success])
    provider.transport = transport

    # Mark as running manually and try to run
    provider._running = True
    with pytest.raises(RuntimeError, match="AlpacaProvider is already running"):
        await provider.run()
