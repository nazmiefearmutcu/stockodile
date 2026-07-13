from __future__ import annotations

import pathlib
from collections.abc import Iterable
from typing import Any

import pytest

from stockodile.ingest.deadletter import DeadLetterQueue
from stockodile.ingest.gap_bridge import (
    BookResyncBridge,
    OrderBookSync,
    SyncResult,
    TradeSeqGap,
)
from stockodile.ingest.transport import FakeTransport
from stockodile.providers.base import Provider, backoff_delays
from stockodile.reference.registry import Instrument, InstrumentRegistry
from stockodile.schema.enums import SecurityType
from stockodile.schema.records import BookDelta, BookSnapshot
from stockodile.sink.base import Sink


class DummySink(Sink):
    def __init__(self) -> None:
        self.records: list[Any] = []

    async def put(self, record: Any) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        pass


class DummyProvider(Provider):
    async def list_instruments(self) -> list[Instrument]:
        return [
            Instrument(
                symbol="AAPL",
                provider=self.name,
                symbol_raw="AAPL",
                security_type=SecurityType.CS,
            )
        ]

    async def _subscribe(self, transport: Any) -> None:
        await transport.send(b'{"action": "subscribe"}')

    def normalize(self, msg: object, local_ts: int) -> Iterable[Any]:
        yield msg


def test_backoff_delays() -> None:
    assert backoff_delays(0, base=1.0, cap=30.0, jitter=0.0) == 1.0
    assert backoff_delays(1, base=1.0, cap=30.0, jitter=0.0) == 2.0
    assert backoff_delays(10, base=1.0, cap=30.0, jitter=0.0) == 30.0


def test_deadletter_queue() -> None:
    dlq = DeadLetterQueue(max_size=3)
    dlq.put(1, b"raw1", "Err1", "tb1")
    dlq.put(2, b"raw2", "Err2", "tb2")
    dlq.put(3, b"raw3", "Err3", "tb3")
    # should evict the first one if we add more
    dlq.put(4, b"raw4", "Err4", "tb4")

    items = dlq.drain()
    assert len(items) == 3
    assert items[0].local_ts == 2
    assert items[1].local_ts == 3
    assert items[2].local_ts == 4

    assert len(dlq.drain()) == 0


def test_dlq_sqlite_honors_max_size(tmp_path: pathlib.Path) -> None:
    import sqlite3

    path = tmp_path / "dlq.db"
    q = DeadLetterQueue(max_size=2, db_path=str(path))
    for i in range(5):
        q.put(i, f"r{i}".encode(), "E", "tb")
    n = sqlite3.connect(path).execute("SELECT COUNT(*) FROM dead_letters").fetchone()[0]
    assert n == 2
    items = q.drain()
    assert len(items) == 2
    n2 = sqlite3.connect(path).execute("SELECT COUNT(*) FROM dead_letters").fetchone()[0]
    assert n2 == 0


def test_dlq_rejects_zero_max_size() -> None:
    with pytest.raises(ValueError):
        DeadLetterQueue(max_size=0)


@pytest.mark.asyncio
async def test_fake_transport() -> None:
    frames = [b"frame1", b"frame2"]
    transport = FakeTransport(frames)

    await transport.connect()
    assert transport._connected is True

    received = []
    async for f in transport:
        received.append(f)
    assert received == frames

    await transport.send(b"hello")  # no-op
    await transport.close()
    assert transport._connected is False


@pytest.mark.asyncio
async def test_provider_run() -> None:
    sink = DummySink()
    registry = InstrumentRegistry()
    provider = DummyProvider(symbols=["AAPL"], channels=["trade"], out=sink, registry=registry)
    provider.name = "dummy"

    frames = [b'{"data": "msg1"}', b'{"data": "msg2"}']
    transport = FakeTransport(frames)
    provider.transport = transport

    await provider.run(max_reconnects=0)
    assert len(sink.records) == 2
    assert sink.records[0] == {"data": "msg1"}
    assert sink.records[1] == {"data": "msg2"}


def test_order_book_sync_spot() -> None:
    sync = OrderBookSync(venue="spot")

    # Pre-snapshot feed -> DROP
    assert sync.feed(U=10, u=15, pu=None) == SyncResult.DROP

    # Set snapshot
    sync.set_snapshot(last_update_id=100)

    # Stale event -> DROP
    assert sync.feed(U=90, u=100, pu=None) == SyncResult.DROP

    # Gap in first event -> RESYNC
    assert sync.feed(U=105, u=110, pu=None) == SyncResult.RESYNC

    # Valid first event: U <= 101, u >= 101
    sync.set_snapshot(last_update_id=100)
    assert sync.feed(U=95, u=105, pu=None) == SyncResult.APPLY

    # Subsequent event: U == prev_u + 1
    assert sync.feed(U=106, u=110, pu=None) == SyncResult.APPLY
    # Gap -> RESYNC
    assert sync.feed(U=112, u=115, pu=None) == SyncResult.RESYNC


def test_order_book_sync_futures() -> None:
    sync = OrderBookSync(venue="futures")
    sync.set_snapshot(last_update_id=100)

    # Stale: u < 100
    assert sync.feed(U=90, u=99, pu=90) == SyncResult.DROP

    # First valid: U <= 100, u >= 100
    assert sync.feed(U=95, u=105, pu=94) == SyncResult.APPLY

    # Subsequent: pu == prev_u (105)
    assert sync.feed(U=106, u=110, pu=105) == SyncResult.APPLY
    # Gap -> RESYNC
    assert sync.feed(U=111, u=115, pu=109) == SyncResult.RESYNC


@pytest.mark.asyncio
async def test_book_resync_bridge() -> None:
    sync = OrderBookSync(venue="spot")

    async def dummy_fetch(symbol: str) -> BookSnapshot:
        return BookSnapshot(
            provider="dummy",
            symbol=symbol,
            symbol_raw=symbol,
            source_ts=None,
            local_ts=1000,
            bids=[],
            asks=[],
            depth=0,
            sequence_id=200,
        )

    bridge = BookResyncBridge(sync=sync, fetch_snapshot=dummy_fetch, symbol="AAPL")

    # Simulate a feed resulting in APPLY
    delta1 = BookDelta(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=500,
        bids=[],
        asks=[],
        seq_id=150,
        prev_seq_id=None,
    )

    # Not resyncing, first needs snapshot to decide
    sync.set_snapshot(200)
    res = sync.feed(U=201, u=202, pu=None)
    assert res == SyncResult.APPLY
    assert bridge.feed_sync_result(res, delta1) == delta1

    # Trigger RESYNC
    res = sync.feed(U=205, u=206, pu=None)
    assert res == SyncResult.RESYNC
    assert bridge.feed_sync_result(res, delta1) is None
    assert bridge.is_resyncing is True

    # Subsequent delta is buffered
    delta2 = BookDelta(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=600,
        bids=[],
        asks=[],
        seq_id=201,
        prev_seq_id=None,
    )
    assert bridge.feed_sync_result(SyncResult.DROP, delta2) is None

    # Complete resync
    # snapshot seq will be 200. spot venue: keep seq_id > 200. delta2 (201) > 200, so it is kept.
    emitted = await bridge.complete_resync()
    assert len(emitted) == 2
    assert isinstance(emitted[0], BookSnapshot)
    assert emitted[0].sequence_id == 200
    assert emitted[1] == delta2
    assert bridge.is_resyncing is False


def test_trade_seq_gap() -> None:
    gap = TradeSeqGap()
    r0 = gap.feed(100)
    assert not r0  # bool(TradeGapResult)
    assert not r0.is_gap
    assert not gap.feed(101)
    # gap detected with expected/got for backfill
    r = gap.feed(103)
    assert r
    assert r.is_gap
    assert r.expected == 102
    assert r.got == 103
    assert r.skipped == 1
    assert r.kind == "forward"
    # reset
    gap.reset()
    assert not gap.feed(200)


def test_trade_seq_gap_backward() -> None:
    gap = TradeSeqGap()
    gap.feed(100)
    r = gap.feed(90)
    assert r
    assert r.kind == "backward"
    assert r.expected == 101
    assert r.got == 90
