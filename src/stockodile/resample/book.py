"""Book-snapshot resampling at fixed wall-clock intervals."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from stockodile.replay.orderbook import OrderBook
from stockodile.schema.records import BookDelta, BookSnapshot

__all__ = ["resample_book_snapshots"]


def resample_book_snapshots(
    records: Iterable[BookSnapshot | BookDelta],
    interval_ns: int,
    top_n: int | None = None,
) -> Iterator[BookSnapshot]:
    """Reconstruct book from a stream of records and emit periodic snapshots.

    Args:
        records:     An iterable of ``BookSnapshot`` and/or ``BookDelta``
                     canonical records, ordered by ``local_ts``.
        interval_ns: Emit interval width in nanoseconds.
                     E.g. ``1_000_000_000`` for 1-second snapshots.
        top_n:       Maximum number of bid and ask levels to include in each
                     emitted snapshot.  ``None`` means include all levels.

    Yields:
        ``BookSnapshot`` records at every interval boundary, capturing the
        reconstructed book state *after* the boundary-crossing record has been
        applied.  Each emitted snapshot's ``local_ts`` is set to the bucket
        boundary timestamp, not the triggering record's ``local_ts``.

    Raises:
        stockodile.replay.orderbook.BookGap: Propagated from the underlying
                 ``OrderBook`` if a sequence continuity break is detected.
        ValueError: If ``interval_ns`` is not a positive integer.
    """
    if interval_ns <= 0:
        raise ValueError(f"interval_ns must be positive; got {interval_ns!r}")

    book = OrderBook()
    next_boundary_ns: int | None = None  # set when first snapshot is applied
    initialized = False  # True once the engine has seen its first BookSnapshot

    for record in records:
        ts = record.local_ts

        # Before the engine is initialized, we cannot emit anything useful.
        # We still forward the record to the engine so it can wait for its
        # first BookSnapshot (the engine silently drops pre-snapshot deltas).
        if not initialized:
            if isinstance(record, BookSnapshot):
                book.apply(record)
                initialized = True
                # Set the first boundary to the end of the interval that
                # contains this snapshot.
                next_boundary_ns = (ts // interval_ns) * interval_ns + interval_ns
            continue

        # Yield snapshots for all boundaries strictly before ts to prevent lookahead bias.
        assert next_boundary_ns is not None
        while ts > next_boundary_ns:
            yield _capture_snapshot(book, record, next_boundary_ns, top_n)
            next_boundary_ns += interval_ns

        # Apply the record to the book (may raise BookGap).
        book.apply(record)

        # If the record is exactly on the boundary, yield the snapshot for it.
        while ts >= next_boundary_ns:
            yield _capture_snapshot(book, record, next_boundary_ns, top_n)
            next_boundary_ns += interval_ns


def _capture_snapshot(
    book: OrderBook,
    trigger_record: BookSnapshot | BookDelta,
    boundary_ns: int,
    top_n: int | None,
) -> BookSnapshot:
    """Build a BookSnapshot from the current ``OrderBook`` state.

    Args:
        book:           The live ``OrderBook`` instance.
        trigger_record: The record whose ``local_ts`` crossed the boundary.
        boundary_ns:    The nanosecond timestamp of the bucket boundary.
        top_n:          Maximum bid/ask levels on each side; ``None`` = all.

    Returns:
        A ``BookSnapshot`` representing the book at ``boundary_ns``.
    """
    bids_sorted = sorted(book.bids.items(), reverse=True)
    asks_sorted = sorted(book.asks.items())

    if top_n is not None:
        bids_sorted = bids_sorted[:top_n]
        asks_sorted = asks_sorted[:top_n]

    bids: list[tuple[float, float]] = [(p, s) for p, s in bids_sorted]
    asks: list[tuple[float, float]] = [(p, s) for p, s in asks_sorted]

    depth = len(bids) + len(asks)

    return BookSnapshot(
        provider=trigger_record.provider,
        symbol=trigger_record.symbol,
        symbol_raw=trigger_record.symbol_raw,
        source_ts=None,
        local_ts=boundary_ns,
        bids=bids,
        asks=asks,
        depth=depth,
        sequence_id=None,
        is_snapshot=True,
    )
