"""Gap-detect → backfill bridge (Task 4.3).

Wires OrderBookSync.RESYNC to a REST snapshot fetch, buffers live deltas
during the resync window, and applies them after the snapshot arrives
(dropping any with seq < snapshot.seq_id).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Literal

from stockodile.schema.records import BookDelta, BookSnapshot

log = logging.getLogger(__name__)


class SyncResult(StrEnum):
    DROP = "drop"
    APPLY = "apply"
    RESYNC = "resync"


class OrderBookSync:
    """State machine for synchronising a depth stream with a REST snapshot."""

    def __init__(self, venue: Literal["spot", "futures"]) -> None:
        """Initialise for 'spot' or 'futures' venue."""
        if venue not in ("spot", "futures"):
            raise ValueError(f"venue must be 'spot' or 'futures', got {venue!r}")
        self._venue = venue  # "spot" or "futures"
        self._snapshot_id: int | None = None
        self._prev_u: int | None = None
        self._have_first: bool = False

    def set_snapshot(self, last_update_id: int) -> None:
        """Called once the REST snapshot has been fetched."""
        self._snapshot_id = last_update_id
        self._prev_u = None
        self._have_first = False

    def feed(self, U: int, u: int, pu: int | None) -> SyncResult:
        """Process one depth diff event and return the action to take.

        Parameters
        ----------
        U:  First update id in this event.
        u:  Final update id in this event.
        pu: Previous final update id (futures only; None for spot).
        """
        if self._snapshot_id is None:
            # No snapshot yet — buffer (treat as DROP until snapshot arrives)
            return SyncResult.DROP

        sid = self._snapshot_id

        if not self._have_first:
            if self._venue == "spot":
                # Drop stale events
                if u <= sid:
                    return SyncResult.DROP
                # First valid: U <= sid+1 AND u >= sid+1
                if U <= sid + 1 and u >= sid + 1:
                    self._have_first = True
                    self._prev_u = u
                    return SyncResult.APPLY
                # Otherwise gap before first event -> resync
                return SyncResult.RESYNC
            else:
                # futures
                # Drop stale events: u < lastUpdateId
                if u < sid:
                    return SyncResult.DROP
                # First valid: U <= lastUpdateId AND u >= lastUpdateId
                if U <= sid and u >= sid:
                    self._have_first = True
                    self._prev_u = u
                    return SyncResult.APPLY
                return SyncResult.RESYNC
        else:
            # Subsequent events — check continuity.
            if self._prev_u is None:
                raise RuntimeError(
                    "invariant violated: _prev_u is None with _have_first=True"
                )
            if self._venue == "spot":
                if U == self._prev_u + 1:
                    self._prev_u = u
                    return SyncResult.APPLY
                return SyncResult.RESYNC
            else:
                # futures: pu must equal prev_u
                if pu == self._prev_u:
                    self._prev_u = u
                    return SyncResult.APPLY
                return SyncResult.RESYNC


# Type alias: an async callable that fetches a REST book snapshot for a symbol.
FetchSnapshotFn = Callable[[str], Awaitable[BookSnapshot]]

# Records that the bridge can return / emit.
BookRecord = BookSnapshot | BookDelta


class BookResyncBridge:
    """Stateful bridge between OrderBookSync and REST-snapshot resync logic.

    Responsibilities:
    - Translate ``SyncResult`` (APPLY/DROP/RESYNC) into emit-or-buffer decisions.
    - On RESYNC: enter resyncing mode, buffer subsequent deltas.
    - On ``complete_resync()``: fetch REST snapshot, update the sync state machine,
      apply buffered deltas with ``seq_id >= snapshot.sequence_id``, return
      the ordered list of records to emit.
    - A second RESYNC while already resyncing clears the stale buffer and restarts.

    Thread-safety: not thread-safe; designed for single-coroutine asyncio use.
    """

    def __init__(
        self,
        sync: OrderBookSync,
        fetch_snapshot: FetchSnapshotFn,
        symbol: str,
    ) -> None:
        self._sync = sync
        self._fetch_snapshot = fetch_snapshot
        self._symbol = symbol
        self._resyncing: bool = False
        # Buffer of deltas accumulated during a resync window.
        self._buffer: list[BookDelta] = []

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def is_resyncing(self) -> bool:
        """True while waiting for a REST snapshot to complete a resync."""
        return self._resyncing

    # ------------------------------------------------------------------
    # Public methods — hot path
    # ------------------------------------------------------------------

    def feed_sync_result(
        self,
        result: SyncResult,
        delta: BookDelta,
    ) -> BookDelta | None:
        """Process one sync result from OrderBookSync.feed().

        Parameters
        ----------
        result:
            The ``SyncResult`` returned by ``OrderBookSync.feed()`` for *delta*.
        delta:
            The ``BookDelta`` that was fed into the sync state machine.

        Returns
        -------
        ``delta`` if it should be emitted to the sink immediately, or
        ``None`` if it was dropped or buffered.

        Side-effects:
        - RESYNC: enters resyncing mode; *delta* is buffered (not emitted).
        - DROP / RESYNC while resyncing: delta is buffered; returns ``None``.
        - APPLY while not resyncing: returns *delta* for the caller to emit.
        """
        if result == SyncResult.RESYNC:
            if self._resyncing:
                # Second RESYNC while already resyncing: clear stale buffer,
                # buffer the new RESYNC-triggering delta.
                log.warning(
                    "BookResyncBridge [%s]: second RESYNC while already resyncing; "
                    "clearing stale buffer (had %d deltas).",
                    self._symbol,
                    len(self._buffer),
                )
                self._buffer = []
            else:
                log.warning(
                    "BookResyncBridge [%s]: RESYNC triggered at seq=%s; "
                    "entering resync mode.",
                    self._symbol,
                    delta.seq_id,
                )
                self._resyncing = True
            # Buffer the triggering delta — it may be valid post-resync.
            self._buffer.append(delta)
            return None

        if self._resyncing:
            # While resyncing, buffer everything (even DROPs).  Note: these
            # deltas are emitted as-is after seq-filtering on complete_resync();
            # the sync machine is re-anchored to snap_seq but the kept deltas
            # bypass it.
            self._buffer.append(delta)
            return None

        if result == SyncResult.DROP:
            return None

        # APPLY and not resyncing → emit directly.
        return delta

    def buffer_delta(self, delta: BookDelta) -> None:
        """Manually buffer a delta (e.g. arrived while resync I/O is in flight).

        Callers that manage the resync as a background coroutine can push
        deltas that arrive *after* RESYNC was signalled but *before*
        ``complete_resync()`` has finished.
        """
        self._buffer.append(delta)

    # ------------------------------------------------------------------
    # Resync completion
    # ------------------------------------------------------------------

    async def complete_resync(self) -> list[BookRecord]:
        """Fetch a REST snapshot and apply buffered deltas.

        Must be called when ``is_resyncing`` is True to complete the resync
        cycle.  Fetches the REST snapshot, updates the internal OrderBookSync
        state machine, applies buffered deltas with ``seq_id >=
        snapshot.sequence_id`` (drops stale ones), then clears the buffer and
        exits resyncing mode.

        Returns
        -------
        An ordered list of records to emit: [BookSnapshot] + kept deltas.
        """
        snapshot = await self._fetch_snapshot(self._symbol)
        snap_seq = snapshot.sequence_id  # int | None

        log.info(
            "BookResyncBridge [%s]: REST snapshot fetched, sequence_id=%s; "
            "filtering %d buffered deltas.",
            self._symbol,
            snap_seq,
            len(self._buffer),
        )

        # Update the sync state machine with the new snapshot anchor.
        if snap_seq is not None:
            self._sync.set_snapshot(last_update_id=snap_seq)
        else:
            log.warning(
                "BookResyncBridge [%s]: REST snapshot has sequence_id=None; "
                "sync state machine NOT re-anchored (stale anchor retained). "
                "All buffered deltas will be kept and continuity may be wrong.",
                self._symbol,
            )

        venue = getattr(self._sync, "_venue", "spot")
        kept_deltas: list[BookDelta] = []
        for delta in self._buffer:
            if snap_seq is None or delta.seq_id is None:
                kept_deltas.append(delta)
            elif venue == "futures":
                # Keep when seq_id >= snap_seq (boundary inclusive)
                if delta.seq_id >= snap_seq:
                    kept_deltas.append(delta)
                else:
                    log.debug(
                        "BookResyncBridge [%s]: dropping buffered delta seq=%s (< snapshot %s).",
                        self._symbol,
                        delta.seq_id,
                        snap_seq,
                    )
            else:
                # spot (default): keep when seq_id > snap_seq (boundary exclusive)
                if delta.seq_id > snap_seq:
                    kept_deltas.append(delta)
                else:
                    log.debug(
                        "BookResyncBridge [%s]: dropping buffered delta seq=%s (<= snapshot %s).",
                        self._symbol,
                        delta.seq_id,
                        snap_seq,
                    )

        # Clear state
        self._buffer = []
        self._resyncing = False

        # Return snapshot first, then kept deltas (in buffered order).
        result: list[BookRecord] = [snapshot]
        result.extend(kept_deltas)
        return result


class TradeSeqGap:
    """Detect gaps in a monotonic trade sequence.

    A skip signals missed trades and should trigger a REST backfill for the
    missing range.
    """

    def __init__(self) -> None:
        self._last_seq: int | None = None

    def feed(self, trade_seq: int) -> bool:
        """Process one trade sequence number.

        Parameters
        ----------
        trade_seq:
            The sequence field from the incoming trade message.

        Returns
        -------
        ``True`` if a gap was detected, ``False`` otherwise.
        """
        if self._last_seq is None:
            # First trade: establish baseline, no gap.
            self._last_seq = trade_seq
            return False

        is_gap = trade_seq != self._last_seq + 1
        if is_gap:
            skipped = trade_seq - self._last_seq - 1
            if skipped < 0:
                log.warning(
                    "TradeSeqGap: backward seq — expected seq=%d, got seq=%d "
                    "(reset or reconnect without TradeSeqGap.reset() call?).",
                    self._last_seq + 1,
                    trade_seq,
                )
            else:
                log.warning(
                    "TradeSeqGap: gap detected — expected seq=%d, got seq=%d (skipped %d).",
                    self._last_seq + 1,
                    trade_seq,
                    skipped,
                )
        self._last_seq = trade_seq
        return is_gap

    def reset(self) -> None:
        """Reset the gap detector (e.g. after a reconnect)."""
        self._last_seq = None
