import pytest

from stockodile.replay.merge import replay
from stockodile.replay.orderbook import BookGap, OrderBook
from stockodile.schema.records import BookDelta, BookSnapshot, Trade


def test_orderbook_spot_basic() -> None:
    book = OrderBook()
    assert not book._initialized

    # Apply snapshot
    snap = BookSnapshot(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1000,
        local_ts=1000,
        bids=[(100.0, 10.0), (99.0, 20.0)],
        asks=[(101.0, 15.0), (102.0, 25.0)],
        depth=2,
        sequence_id=100,
    )
    book.apply(snap)
    assert book._initialized
    assert book._last_seq_id == 100
    assert book.best_bid() == 100.0
    assert book.best_ask() == 101.0

    # Apply correct spot delta (seq_id == last_seq_id + 1)
    delta1 = BookDelta(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1001,
        local_ts=1001,
        bids=[(100.0, 12.0), (99.0, 0.0)],  # update bid, remove bid
        asks=[(101.5, 5.0)],
        seq_id=101,
        prev_seq_id=None,
    )
    book.apply(delta1)
    assert book._last_seq_id == 101
    assert book.bids == {100.0: 12.0}  # 99.0 was removed
    assert book.asks == {101.0: 15.0, 101.5: 5.0, 102.0: 25.0}

    # Apply spot delta with gap
    delta_gap = BookDelta(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1002,
        local_ts=1002,
        bids=[],
        asks=[],
        seq_id=103,  # Gap: expected 102
        prev_seq_id=None,
    )
    with pytest.raises(BookGap):
        book.apply(delta_gap)


def test_orderbook_futures_first_delta() -> None:
    book = OrderBook()
    snap = BookSnapshot(
        provider="dummy",
        symbol="BTCUSDT",
        symbol_raw="BTCUSDT",
        source_ts=1000,
        local_ts=1000,
        bids=[(50000.0, 1.0)],
        asks=[(50001.0, 2.0)],
        depth=1,
        sequence_id=200,  # lastUpdateId
    )
    book.apply(snap)

    # First futures delta: U <= lastUpdateId <= u
    # which translates to: prev_seq_id < last_seq_id <= seq_id
    # e.g., prev_seq_id=195, seq_id=205
    delta = BookDelta(
        provider="dummy",
        symbol="BTCUSDT",
        symbol_raw="BTCUSDT",
        source_ts=1001,
        local_ts=1001,
        bids=[(50000.0, 1.5)],
        asks=[],
        seq_id=205,
        prev_seq_id=195,
    )
    # This should succeed due to relaxed check
    book.apply(delta)
    assert book._last_seq_id == 205
    assert book.bids[50000.0] == 1.5

    # Subsequent futures delta: prev_seq_id == last_seq_id (205)
    delta2 = BookDelta(
        provider="dummy",
        symbol="BTCUSDT",
        symbol_raw="BTCUSDT",
        source_ts=1002,
        local_ts=1002,
        bids=[],
        asks=[(50001.0, 0.0)],
        seq_id=210,
        prev_seq_id=205,
    )
    book.apply(delta2)
    assert book._last_seq_id == 210

    # Subsequent futures delta with gap
    delta_gap = BookDelta(
        provider="dummy",
        symbol="BTCUSDT",
        symbol_raw="BTCUSDT",
        source_ts=1003,
        local_ts=1003,
        bids=[],
        asks=[],
        seq_id=220,
        prev_seq_id=215,  # Gap: expected 210
    )
    with pytest.raises(BookGap):
        book.apply(delta_gap)


def test_orderbook_duplicate_delta_checks() -> None:
    book = OrderBook()
    snap = BookSnapshot(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1000,
        local_ts=1000,
        bids=[(100.0, 10.0)],
        asks=[(101.0, 10.0)],
        depth=1,
        sequence_id=100,
    )
    book.apply(snap)

    delta = BookDelta(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1001,
        local_ts=1001,
        bids=[(100.0, 11.0)],
        asks=[],
        seq_id=101,
        prev_seq_id=None,
    )
    book.apply(delta)

    # Re-apply identical delta (same seq_id, same bids/asks)
    identical_delta = BookDelta(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1001,
        local_ts=1001,
        bids=[(100.0, 11.0)],
        asks=[],
        seq_id=101,
        prev_seq_id=None,
    )
    book.apply(identical_delta)  # should succeed and do nothing

    # Re-apply non-identical delta (same seq_id, different bids/asks)
    non_identical_delta = BookDelta(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1001,
        local_ts=1001,
        bids=[(100.0, 12.0)],
        asks=[],
        seq_id=101,
        prev_seq_id=None,
    )
    with pytest.raises(BookGap):
        book.apply(non_identical_delta)


def test_orderbook_float_rounding_and_validation() -> None:
    book = OrderBook()
    snap = BookSnapshot(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1000,
        local_ts=1000,
        bids=[(100.10000000000001, 10.0)],
        asks=[(101.0, 10.0)],
        depth=1,
        sequence_id=100,
    )
    book.apply(snap)
    # The price should be rounded to 8 decimals
    assert 100.1 in book.bids

    # Update/Delete level with slightly imprecise float
    delta = BookDelta(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1001,
        local_ts=1001,
        bids=[(100.1, 0.0)],
        asks=[],
        seq_id=101,
    )
    book.apply(delta)
    assert 100.1 not in book.bids

    # Assert validations
    with pytest.raises(AssertionError):
        # negative price
        book._apply_levels([(-10.0, 1.0)], book.bids)

    with pytest.raises(AssertionError):
        # negative size
        book._apply_levels([(100.0, -1.0)], book.bids)


def test_orderbook_apply_batch() -> None:
    book = OrderBook()
    snap = BookSnapshot(
        provider="dummy",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1000,
        local_ts=1000,
        bids=[(100.0, 10.0)],
        asks=[(101.0, 10.0)],
        depth=1,
        sequence_id=100,
    )
    book.apply(snap)

    deltas = [
        BookDelta(
            provider="dummy",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=1001,
            local_ts=1001,
            bids=[(100.0, 11.0)],
            asks=[],
            seq_id=101,
        ),
        BookDelta(
            provider="dummy",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=1001,
            local_ts=1001,
            bids=[],
            asks=[(101.0, 12.0)],
            seq_id=102,
        ),
    ]
    book.apply_batch(deltas)
    assert book._last_seq_id == 102
    assert book.bids[100.0] == 11.0
    assert book.asks[101.0] == 12.0

    # Batch with gap inside
    deltas_gap = [
        BookDelta(
            provider="dummy",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=1002,
            local_ts=1002,
            bids=[(100.0, 15.0)],
            asks=[],
            seq_id=103,
        ),
        BookDelta(
            provider="dummy",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=1002,
            local_ts=1002,
            bids=[],
            asks=[(101.0, 15.0)],
            seq_id=105,  # Gap! Expected 104
        ),
    ]
    with pytest.raises(BookGap):
        book.apply_batch(deltas_gap)


def test_merge_replay_determinism() -> None:
    t1 = Trade(
        provider="provA",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1000,
        local_ts=2000,
        id="1",
        price=150.0,
        size=10.0,
    )
    t2 = Trade(
        provider="provB",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1000,
        local_ts=2000,
        id="2",
        price=150.0,
        size=10.0,
    )
    t3 = Trade(
        provider="provA",
        symbol="MSFT",
        symbol_raw="MSFT",
        source_ts=1000,
        local_ts=2000,
        id="3",
        price=300.0,
        size=5.0,
    )

    res1 = list(replay([iter([t2]), iter([t1]), iter([t3])]))
    assert res1 == [t1, t3, t2]

    res2 = list(replay([iter([t3]), iter([t2]), iter([t1])]))
    assert res2 == [t1, t3, t2]
