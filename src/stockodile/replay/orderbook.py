"""Order-book reconstruction state machine."""

from __future__ import annotations

from collections.abc import Sequence

from stockodile.schema.records import BookDelta, BookSnapshot


class BookGap(Exception):
    """Raised when a sequence continuity check fails.

    Callers should catch this and trigger a REST resync to rebuild the book
    from a fresh snapshot.
    """


class OrderBook:
    """Snapshot-anchored order-book reconstruction state machine.

    Maintains bids and asks as plain dicts mapping price → size.
    ``best_bid()`` and ``best_ask()`` return the top-of-book prices.
    """

    def __init__(self) -> None:
        # price → size (positive float)
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}

        # True once the first snapshot has been processed
        self._initialized: bool = False

        # Last seq_id applied (from a snapshot or delta); None if not set
        self._last_seq_id: int | None = None
        self._first_delta_after_snapshot: bool = False
        self._last_delta: BookDelta | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def apply(self, record: BookSnapshot | BookDelta) -> None:
        """Apply a single record to the book.

        Args:
            record: A ``BookSnapshot`` (resets the book) or a ``BookDelta``
                    (incremental update, subject to gap detection).

        Raises:
            BookGap: If sequence continuity is violated on a delta.
        """
        if isinstance(record, BookSnapshot):
            self._apply_snapshot(record)
        else:
            self._apply_delta(record)

    def apply_batch(self, deltas: Sequence[BookDelta]) -> None:
        """Apply a batch of deltas that share one ``local_ts`` atomically.

        Args:
            deltas: One or more BookDelta records from the same timestamp.

        Raises:
            BookGap: If any delta's sequence continuity check fails.
        """
        if not deltas:
            return
        for delta in deltas:
            self._apply_delta(delta)

    def best_bid(self) -> float | None:
        """Return the highest bid price, or None if the bids side is empty."""
        return max(self.bids) if self.bids else None

    def best_ask(self) -> float | None:
        """Return the lowest ask price, or None if the asks side is empty."""
        return min(self.asks) if self.asks else None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _apply_snapshot(self, snap: BookSnapshot) -> None:
        """Reset book state from a snapshot record."""
        self.bids = {}
        self.asks = {}
        self._apply_levels(snap.bids, self.bids)
        self._apply_levels(snap.asks, self.asks)
        self._initialized = True
        self._last_seq_id = snap.sequence_id
        self._first_delta_after_snapshot = True
        self._last_delta = None

    def _apply_delta(self, delta: BookDelta) -> None:
        """Apply one delta, performing gap detection first."""
        if not self._initialized:
            # Skip rows before the first snapshot
            return

        self._check_gap(delta)
        self._apply_levels(delta.bids, self.bids)
        self._apply_levels(delta.asks, self.asks)

        if delta.seq_id is not None:
            self._last_seq_id = delta.seq_id
        self._first_delta_after_snapshot = False
        self._last_delta = delta

    def _check_gap(self, delta: BookDelta) -> None:
        """Validate sequence continuity; raise BookGap if broken."""
        if delta.seq_id is not None and self._last_seq_id is not None:
            if delta.seq_id == self._last_seq_id:
                if self._last_delta is not None:
                    if delta.bids != self._last_delta.bids or delta.asks != self._last_delta.asks:
                        raise BookGap(
                            f"Duplicate sequence number {delta.seq_id} with different delta content"
                        )
                return

        if delta.prev_seq_id is not None:
            if self._last_seq_id is None:
                raise BookGap(
                    f"Sequence gap: delta.prev_seq_id={delta.prev_seq_id!r} "
                    f"but last_seq_id is None (snapshot had no sequence_id)"
                )
            if self._first_delta_after_snapshot:
                is_valid = (
                    delta.seq_id is not None
                    and delta.prev_seq_id < self._last_seq_id <= delta.seq_id
                )
                if not is_valid:
                    raise BookGap(
                        f"Sequence gap on first futures delta after snapshot: "
                        f"expected prev_seq_id={delta.prev_seq_id!r} < "
                        f"last_seq_id={self._last_seq_id!r} <= seq_id={delta.seq_id!r}"
                    )
            else:
                if delta.prev_seq_id != self._last_seq_id:
                    raise BookGap(
                        f"Sequence gap: delta.prev_seq_id={delta.prev_seq_id!r} "
                        f"!= last_seq_id={self._last_seq_id!r}"
                    )
        else:
            if (
                delta.seq_id is not None
                and self._last_seq_id is not None
                and delta.seq_id != self._last_seq_id + 1
            ):
                raise BookGap(
                    f"Spot-shape sequence gap: delta.seq_id={delta.seq_id!r} "
                    f"!= last_seq_id+1={self._last_seq_id + 1!r}"
                )

    @staticmethod
    def _apply_levels(
        levels: list[tuple[float, float]],
        side: dict[float, float],
    ) -> None:
        """Apply a list of (price, size) levels to a book side.

        size == 0.0 → remove the price level.
        size >  0.0 → set/overwrite the absolute size at that price.
        """
        for price, size in levels:
            assert price > 0, f"price must be positive, got {price}"
            assert size >= 0, f"size must be non-negative, got {size}"

            rounded_price = round(price, 8)
            if size == 0.0:
                side.pop(rounded_price, None)
            else:
                side[rounded_price] = size
