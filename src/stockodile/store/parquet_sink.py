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
import logging
import os
import re
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import polars as pl

from stockodile.schema.records import Record
from stockodile.sink.base import Sink
from stockodile.store.rows import _date_from_ns, _symbol_bucket, to_row

log = logging.getLogger(__name__)

# Hive partition path segments (provider, channel) must be single path components.
_HIVE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def _is_safe_hive_segment(value: str) -> bool:
    """Return True if ``value`` is a safe single hive path segment.

    Rejects empty values, ``.`` / ``..``, null bytes, and any character outside
    the allowlist ``[A-Za-z0-9._-]`` (which also excludes ``/`` and ``\\``).
    """
    if not value or value in (".", "..") or "\x00" in value:
        return False
    return _HIVE_SEGMENT_RE.fullmatch(value) is not None

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
    "corp_action": {
        "ex_date": pl.Utf8,
        "type": pl.Utf8,
        "value": pl.Float64,
    },
    "fundamental": {
        "taxonomy": pl.Utf8,
        "tag": pl.Utf8,
        "unit": pl.Utf8,
        "val": pl.Float64,
        "end": pl.Utf8,
        "start": pl.Utf8,
        "fy": pl.Int64,
        "fp": pl.Utf8,
        "form": pl.Utf8,
        "filed": pl.Utf8,
        "accn": pl.Utf8,
        "frame": pl.Utf8,
    },
    "filing": {
        "accession_number": pl.Utf8,
        "form": pl.Utf8,
        "filing_date": pl.Utf8,
        "primary_document": pl.Utf8,
        "document_url": pl.Utf8,
        "report_date": pl.Utf8,
        "is_xbrl": pl.Boolean,
    },
    "ohlcv": {
        "interval": pl.Utf8,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
        "vwap": pl.Float64,
        "trade_count": pl.Int64,
    },
    "index_value": {
        "value": pl.Float64,
    },
    "auction": {
        "paired_shares": pl.Float64,
        "imbalance_shares": pl.Float64,
        "imbalance_side": pl.Utf8,
        "reference_price": pl.Float64,
        "indicative_price": pl.Float64,
        "auction_type": pl.Utf8,
    },
    "trading_status": {
        "status": pl.Utf8,
        "reason": pl.Utf8,
        "limit_up_price": pl.Float64,
        "limit_down_price": pl.Float64,
        "indicator": pl.Utf8,
    },
    "instrument": {
        "name": pl.Utf8,
        "cik": pl.Utf8,
        "figi": pl.Utf8,
        "composite_figi": pl.Utf8,
        "share_class_figi": pl.Utf8,
        "cusip": pl.Utf8,
        "exchange_name": pl.Utf8,
        "security_type": pl.Utf8,
        "sic": pl.Utf8,
        "shares_outstanding": pl.Int64,
        "listing_date": pl.Utf8,
        "status": pl.Utf8,
    },
    "insider": {
        "insider_name": pl.Utf8,
        "position": pl.Utf8,
        "transaction_type": pl.Utf8,
        "transaction_date": pl.Utf8,
        "shares": pl.Float64,
        "price": pl.Float64,
        "value": pl.Float64,
        "ownership": pl.Utf8,
    },
    "holding_13f": {
        "manager_name": pl.Utf8,
        "issuer_name": pl.Utf8,
        "cusip": pl.Utf8,
        "value": pl.Float64,
        "shares": pl.Float64,
        "shares_type": pl.Utf8,
        "discretion": pl.Utf8,
        "voting_sole": pl.Float64,
        "voting_shared": pl.Float64,
        "voting_none": pl.Float64,
        "report_date": pl.Utf8,
        "accession_number": pl.Utf8,
    },
    "short_interest": {
        "settlement_date": pl.Utf8,
        "short_interest": pl.Float64,
        "prev_short_interest": pl.Float64,
        "days_to_cover": pl.Float64,
        "change_pct": pl.Float64,
    },
    "short_volume": {
        "date_val": pl.Utf8,
        "short_volume": pl.Float64,
        "short_exempt_volume": pl.Float64,
        "total_volume": pl.Float64,
    },
    "option_quote": {
        "underlying": pl.Utf8,
        "expiry": pl.Utf8,
        "strike": pl.Float64,
        "type": pl.Utf8,
        "bid": pl.Float64,
        "ask": pl.Float64,
        "last": pl.Float64,
        "volume": pl.Float64,
        "open_interest": pl.Float64,
        "implied_volatility": pl.Float64,
        "delta": pl.Float64,
        "gamma": pl.Float64,
        "vega": pl.Float64,
        "theta": pl.Float64,
        "rho": pl.Float64,
    },
    "macro_series": {
        "date_val": pl.Utf8,
        "value": pl.Float64,
        "realtime_start": pl.Utf8,
        "realtime_end": pl.Utf8,
    },
}


def _channel_schema(channel: str) -> dict[str, Any]:
    """Return the full Polars schema for the given channel."""
    extra = _CHANNEL_EXTRA.get(channel, {})
    return {**_COMMON_FIELDS, **extra}


def _coerce_levels(rows: list[dict[str, Any]], field: str) -> None:
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
        # channel → list[Record]
        self._buffers: defaultdict[str, list[Record]] = defaultdict(list)
        self._last_flush: float = time.monotonic()
        self._flush_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Sink interface
    # ------------------------------------------------------------------

    async def put(self, record: Record) -> None:
        """Buffer a record; auto-flush if thresholds are exceeded."""
        channel: str = type(record).__struct_config__.tag  # type: ignore[assignment]
        self._buffers[channel].append(record)

        # Flush on row-count threshold
        if len(self._buffers[channel]) >= self._max_buffer_rows:
            async with self._flush_lock:
                await self._flush_channel(channel)
                self._last_flush = time.monotonic()
            return

        # Flush on time threshold (checked lazily on the next put)
        elapsed = time.monotonic() - self._last_flush
        if elapsed >= self._flush_interval:
            await self.flush()

    async def flush(self) -> None:
        """Flush all buffered channels to Parquet."""
        async with self._flush_lock:
            channels = list(self._buffers.keys())
            for channel in channels:
                if self._buffers[channel]:
                    await self._flush_channel(channel)
            self._last_flush = time.monotonic()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _flush_channel(self, channel: str) -> None:
        """Write a channel's buffer to one or more Parquet files.

        Records are removed from the buffer only after a successful write.
        On write failure, failed groups are re-queued and the error is re-raised.
        """
        records = self._buffers.pop(channel, [])
        if not records:
            return

        if not _is_safe_hive_segment(channel):
            log.error(
                "Rejecting %d record(s): unsafe channel hive segment %r",
                len(records),
                channel,
            )
            return

        # Group records by (provider, date, bucket) — each group → one file.
        # Skip records whose free-form provider is not a safe path segment.
        groups: defaultdict[tuple[str, str, int], list[Record]] = defaultdict(list)
        for record in records:
            provider = record.provider
            if not _is_safe_hive_segment(provider):
                log.warning(
                    "Skipping record: unsafe provider hive segment %r (channel=%s symbol=%s)",
                    provider,
                    channel,
                    getattr(record, "symbol", "?"),
                )
                continue
            key = (provider, _date_from_ns(record.local_ts), _symbol_bucket(record.symbol))
            groups[key].append(record)

        if not groups:
            return

        loop = asyncio.get_running_loop()
        group_items = list(groups.items())
        tasks = [
            loop.run_in_executor(
                None,
                self._write_parquet_sync,
                channel,
                provider,
                date,
                bucket,
                group_records,
            )
            for (provider, date, bucket), group_records in group_items
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed_records: list[Record] = []
        first_error: BaseException | None = None
        for (_, group_records), result in zip(group_items, results, strict=True):
            if isinstance(result, BaseException):
                failed_records.extend(group_records)
                if first_error is None:
                    first_error = result

        if failed_records:
            # Prepend so re-queued rows sit ahead of any concurrent put() arrivals.
            self._buffers[channel][0:0] = failed_records
            assert first_error is not None
            raise first_error

    def _write_parquet_sync(
        self,
        channel: str,
        exchange: str,
        date: str,
        bucket: int,
        records: list[Record],
    ) -> None:
        """Synchronous Parquet write (runs in executor to avoid blocking the loop).

        Writes to ``part-{uuid}.parquet.tmp`` then atomically ``os.replace``s to
        the final ``part-{uuid}.parquet`` path so readers never see partial files.
        """
        if not _is_safe_hive_segment(channel):
            raise ValueError(f"unsafe channel hive segment: {channel!r}")
        if not _is_safe_hive_segment(exchange):
            raise ValueError(f"unsafe provider hive segment: {exchange!r}")
        # date is derived as YYYY-MM-DD; still guard free-form path injection
        if not _is_safe_hive_segment(date):
            raise ValueError(f"unsafe date hive segment: {date!r}")

        rows = [to_row(r) for r in records]

        # Build output path
        part_dir = (
            self._data_dir
            / f"provider={exchange}"
            / f"channel={channel}"
            / f"date={date}"
            / f"bucket={bucket}"
        )
        part_dir.mkdir(parents=True, exist_ok=True)
        part_id = uuid.uuid4().hex
        tmp_path = part_dir / f"part-{part_id}.parquet.tmp"
        out_path = part_dir / f"part-{part_id}.parquet"

        # Coerce book levels (list-of-tuples → list-of-dicts)
        if channel in ("book_snapshot", "book_delta"):
            _coerce_levels(rows, "bids")
            _coerce_levels(rows, "asks")

        # Build DataFrame with explicit schema to ensure type consistency
        schema = _channel_schema(channel)
        # Keep only columns that appear in the schema (unknown extras dropped)
        filtered_rows: list[dict[str, Any]] = [{k: row.get(k) for k in schema} for row in rows]
        df = pl.DataFrame(filtered_rows, schema=schema)

        try:
            df.write_parquet(
                tmp_path,
                compression="zstd",
                compression_level=5,
                row_group_size=250_000,
            )
            os.replace(tmp_path, out_path)
        except Exception:
            # Best-effort cleanup of partial temp file; never leave a half-final.
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass
            raise
