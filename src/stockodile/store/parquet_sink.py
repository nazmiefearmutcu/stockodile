"""Buffered, hive-partitioned Parquet sink for canonical US equity Records.

Partition layout:
    data/exchange={E}/channel={C}/date=YYYY-MM-DD/bucket={0..127}/part-{uuid}.parquet

Write policy:
  - Buffers rows per channel.
  - Auto-flushes when a channel buffer reaches ``max_buffer_rows`` rows.
  - Time-based flush is triggered on the next ``put`` after ``flush_interval_seconds``
    has elapsed, or explicitly via ``flush()`` / ``close()``.
  - A new ``part-{uuid}.parquet`` file is written on every flush; existing files are
    never appended to.

Compression: ZSTD level 5.
Row group size: 250,000 rows.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import polars as pl

from stockodile.schema.records import Record
from stockodile.sink.base import Sink
from stockodile.store.rows import to_row

# ---------------------------------------------------------------------------
# Polars schema definitions per channel
# ---------------------------------------------------------------------------
# Fields that every record carries.
_COMMON_FIELDS: dict[str, Any] = {
    "provider": pl.Utf8,
    "symbol": pl.Utf8,
    "symbol_raw": pl.Utf8,
    "source_ts": pl.Int64,
    "local_ts": pl.Int64,
    # Partition columns (written as path components, kept in the row for DuckDB
    # hive reads that include them).
    "channel": pl.Utf8,
    "date": pl.Utf8,
    "bucket": pl.Int32,
    "exchange": pl.Utf8,
}

# A named-struct dtype for a single (price, size) level.
_LEVEL_STRUCT = pl.Struct({"price": pl.Float64, "size": pl.Float64})

# Per-channel extra columns.
_CHANNEL_EXTRA: dict[str, dict[str, Any]] = {
    "trade": {
        "id": pl.Utf8,
        "price": pl.Float64,
        "size": pl.Float64,
        "conditions": pl.List(pl.Utf8),
        "tape": pl.Utf8,
        "venue": pl.Utf8,
    },
    "quote": {
        "bid_px": pl.Float64,
        "bid_sz": pl.Float64,
        "ask_px": pl.Float64,
        "ask_sz": pl.Float64,
        "is_nbbo": pl.Boolean,
        "is_consolidated": pl.Boolean,
        "conditions": pl.List(pl.Utf8),
        "tape": pl.Utf8,
    },
    "book_snapshot": {
        "bids": pl.List(_LEVEL_STRUCT),
        "asks": pl.List(_LEVEL_STRUCT),
        "depth": pl.Int64,
        "sequence_id": pl.Int64,
        "is_snapshot": pl.Boolean,
    },
    "book_delta": {
        "bids": pl.List(_LEVEL_STRUCT),
        "asks": pl.List(_LEVEL_STRUCT),
        "seq_id": pl.Int64,
        "prev_seq_id": pl.Int64,
        "is_snapshot": pl.Boolean,
    },
    "bar": {
        "interval": pl.Utf8,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
        "vwap": pl.Float64,
        "trade_count": pl.Int64,
    },
}


def _channel_schema(channel: str) -> dict[str, Any]:
    """Return the full Polars schema for the given channel."""
    extra = _CHANNEL_EXTRA.get(channel, {})
    return {**_COMMON_FIELDS, **extra}


def _coerce_levels(
    rows: list[dict[str, Any]], field: str
) -> None:
    """Convert list-of-tuples book levels to list-of-dicts in-place.

    Polars ``pl.List(pl.Struct(...))`` requires dicts, not tuples.
    """
    for row in rows:
        levels = row.get(field)
        if levels is not None:
            row[field] = [{"price": px, "size": sz} for px, sz in levels]


# ---------------------------------------------------------------------------
# ParquetSink
# ---------------------------------------------------------------------------


class ParquetSink(Sink):
    """Buffered async sink that writes hive-partitioned Parquet files.

    Args:
        data_dir: Root directory for the data lake.
        max_buffer_rows: Flush a channel buffer when it reaches this many rows.
        flush_interval_seconds: Maximum seconds before a time-triggered flush.
            Pass a large number (e.g. 9999) to disable time-based flushing in
            tests.
    """

    def __init__(
        self,
        data_dir: Path | str,
        max_buffer_rows: int = 100_000,
        flush_interval_seconds: float = 5.0,
    ) -> None:
        self._data_dir = Path(data_dir)
        self._max_buffer_rows = max_buffer_rows
        self._flush_interval = flush_interval_seconds
        # channel → list[row dicts]
        self._buffers: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        self._last_flush: float = time.monotonic()

    # ------------------------------------------------------------------
    # Sink interface
    # ------------------------------------------------------------------

    async def put(self, record: Record) -> None:
        """Buffer a record; auto-flush if thresholds are exceeded."""
        row = to_row(record)
        channel: str = row["channel"]
        self._buffers[channel].append(row)

        # Flush on row-count threshold
        if len(self._buffers[channel]) >= self._max_buffer_rows:
            await self._flush_channel(channel)
            self._last_flush = time.monotonic()
            return

        # Flush on time threshold (checked lazily on the next put)
        elapsed = time.monotonic() - self._last_flush
        if elapsed >= self._flush_interval:
            await self.flush()

    async def flush(self) -> None:
        """Flush all buffered channels to Parquet."""
        channels = list(self._buffers.keys())
        for channel in channels:
            if self._buffers[channel]:
                await self._flush_channel(channel)
        self._last_flush = time.monotonic()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _flush_channel(self, channel: str) -> None:
        """Write a channel's buffer to one or more Parquet files and clear it."""
        rows = self._buffers.pop(channel, [])
        if not rows:
            return

        # Group rows by (exchange, date, bucket) — each group → one file
        groups: defaultdict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            key = (row["exchange"], row["date"], row["bucket"])
            groups[key].append(row)

        for (exchange, date, bucket), group_rows in groups.items():
            await asyncio.get_event_loop().run_in_executor(
                None,
                self._write_parquet_sync,
                channel,
                exchange,
                date,
                bucket,
                group_rows,
            )

    def _write_parquet_sync(
        self,
        channel: str,
        exchange: str,
        date: str,
        bucket: int,
        rows: list[dict[str, Any]],
    ) -> None:
        """Synchronous Parquet write (runs in executor to avoid blocking the loop)."""
        # Build output path
        part_dir = (
            self._data_dir
            / f"provider={exchange}"
            / f"channel={channel}"
            / f"date={date}"
            / f"bucket={bucket}"
        )
        part_dir.mkdir(parents=True, exist_ok=True)
        out_path = part_dir / f"part-{uuid.uuid4().hex}.parquet"

        # Coerce book levels (list-of-tuples → list-of-dicts)
        if channel in ("book_snapshot", "book_delta"):
            _coerce_levels(rows, "bids")
            _coerce_levels(rows, "asks")

        # Build DataFrame with explicit schema to ensure type consistency
        schema = _channel_schema(channel)
        # Keep only columns that appear in the schema (unknown extras dropped)
        filtered_rows: list[dict[str, Any]] = [
            {k: row.get(k) for k in schema} for row in rows
        ]
        df = pl.DataFrame(filtered_rows, schema=schema)

        df.write_parquet(
            out_path,
            compression="zstd",
            compression_level=5,
            row_group_size=250_000,
        )
