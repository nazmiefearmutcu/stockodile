"""K-way merge replay engine.

Merges N pre-sorted per-(channel, symbol) iterators of Records into a single
globally time-ordered stream using heapq.merge.

Sort key (deterministic tie-break):
    (local_ts, source_ts_or_neg_inf, seq_or_0)

Where:
    - local_ts         — primary ordering key; monotonically increasing capture clock
    - source_ts_or_neg_inf — NULL source_ts is treated as -inf so it sorts
                             BEFORE any present source_ts at the same local_ts
    - seq_or_0         — seq_id / sequence_id (whichever is present)
                         falls back to 0 when absent; breaks remaining ties
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Iterator
from typing import Any

from stockodile.schema.records import (
    BookDelta,
    BookSnapshot,
    Record,
)

# Sentinel for NULL source_ts — must be less than any real ns value.
# Real timestamps start around 1_000_000_000_000_000_000 ns (2001), so -1 is safely less.
_NEG_INF: int = -1


def _sort_key(record: Record) -> tuple[int, int, int, str, str]:
    """Return the (local_ts, source_ts_or_neg_inf, seq_or_0, provider, symbol) tuple for ordering.

    NULL source_ts → -1 (sorts BEFORE any real nanosecond timestamp).
    seq is extracted from whichever field is present on the record type:
        BookDelta   → seq_id
        BookSnapshot → sequence_id
        all others  → 0 (no sequence concept)
    """
    local_ts: int = record.local_ts
    source_ts: int = record.source_ts if record.source_ts is not None else _NEG_INF

    seq: int
    if isinstance(record, BookDelta):
        seq = record.seq_id or 0
    elif isinstance(record, BookSnapshot):
        seq = record.sequence_id or 0
    else:
        seq = 0

    return (local_ts, source_ts, seq, record.provider, record.symbol)


class _Keyed:
    """Wrapper that makes a Record sortable by _sort_key without storing the key twice."""

    __slots__ = ("key", "record")

    def __init__(self, record: Record) -> None:
        self.key: tuple[int, int, int, str, str] = _sort_key(record)
        self.record: Record = record

    def __lt__(self, other: Any) -> bool:
        return bool(self.key < other.key)

    def __le__(self, other: Any) -> bool:
        return bool(self.key <= other.key)

    def __eq__(self, other: Any) -> bool:
        return bool(self.key == other.key)

    def __ge__(self, other: Any) -> bool:
        return bool(self.key >= other.key)

    def __gt__(self, other: Any) -> bool:
        return bool(self.key > other.key)


def replay(streams: Iterable[Iterator[Record]]) -> Iterator[Record]:
    """Merge N pre-sorted Record iterators into a single globally time-ordered stream.

    Args:
        streams: Iterable of iterators, each already sorted by ``local_ts``.
                 An empty iterable (or all-empty streams) produces an empty output.

    Yields:
        Records in non-decreasing ``(local_ts, source_ts_or_neg_inf, seq_or_0)`` order.
    """
    keyed_streams = ((_Keyed(r) for r in s) for s in streams)
    for keyed in heapq.merge(*keyed_streams):
        yield keyed.record
