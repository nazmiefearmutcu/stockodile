"""Acceptance tests for ParquetSink in Stockodile."""

from __future__ import annotations

import pathlib
import time

import polars as pl

from stockodile.schema.enums import Tape
from stockodile.schema.records import BookSnapshot, Trade
from stockodile.store.parquet_sink import ParquetSink


def _trade(price: float = 150.0, local_ts: int = 1700000000000000000) -> Trade:
    return Trade(
        provider="alpaca",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=local_ts,
        local_ts=local_ts,
        id=str(price),
        price=price,
        size=10.0,
        conditions=["@"],
        tape=Tape.A,
        venue="NASDAQ",
    )


def _snap(local_ts: int = 1700000000000000000) -> BookSnapshot:
    return BookSnapshot(
        provider="alpaca",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=local_ts,
        local_ts=local_ts,
        bids=[(150.0, 100.0), (149.0, 0.0)],
        asks=[(151.0, 50.0)],
        depth=2,
        sequence_id=42,
        is_snapshot=True,
    )


def _find_parquets(base: pathlib.Path, pattern: str = "*.parquet") -> list[pathlib.Path]:
    """Collect parquet files synchronously."""
    return list(base.rglob(pattern))


async def test_parquet_sink_writes_files_by_channel(tmp_path: pathlib.Path) -> None:
    """3 trades + 1 book_snapshot → files under trade/ and book_snapshot/ dirs."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=10, flush_interval_seconds=9999)

    await sink.put(_trade(150.0))
    await sink.put(_trade(151.0))
    await sink.put(_trade(152.0))
    await sink.put(_snap())

    await sink.flush()

    all_files = _find_parquets(tmp_path)
    trade_files = [p for p in all_files if "channel=trade" in str(p)]
    snap_files = [p for p in all_files if "channel=book_snapshot" in str(p)]

    assert len(trade_files) >= 1, "Expected at least one trade parquet file"
    assert len(snap_files) >= 1, "Expected at least one book_snapshot parquet file"


async def test_parquet_sink_path_structure(tmp_path: pathlib.Path) -> None:
    """Hive path: provider=.../channel=.../date=.../bucket=.../part-*.parquet."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=10, flush_interval_seconds=9999)
    await sink.put(_trade())
    await sink.flush()

    all_parquets = _find_parquets(tmp_path)
    assert all_parquets, "No parquet files written"
    for p in all_parquets:
        parts = p.parts
        # Each path segment should contain hive key=value pairs
        assert any("provider=" in part for part in parts)
        assert any("channel=" in part for part in parts)
        assert any("date=" in part for part in parts)
        assert any("bucket=" in part for part in parts)


async def test_parquet_sink_read_back_rows(tmp_path: pathlib.Path) -> None:
    """Row count + field values survive round-trip."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=10, flush_interval_seconds=9999)
    t1 = _trade(150.0)
    t2 = _trade(151.5)
    await sink.put(t1)
    await sink.put(t2)
    await sink.flush()

    all_files = _find_parquets(tmp_path)
    trade_files = [p for p in all_files if "channel=trade" in str(p)]
    assert trade_files, "No trade parquet files found"
    df = pl.read_parquet(trade_files)
    assert len(df) == 2
    prices = set(df["price"].to_list())
    assert 150.0 in prices
    assert 151.5 in prices
    assert df["tape"][0] == "A"


async def test_parquet_sink_book_removal_level_round_trips(tmp_path: pathlib.Path) -> None:
    """A canonical removal level (px, 0.0) must round-trip through Parquet."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=10, flush_interval_seconds=9999)
    await sink.put(_snap())
    await sink.flush()

    all_files = _find_parquets(tmp_path)
    snap_files = [p for p in all_files if "channel=book_snapshot" in str(p)]
    assert snap_files, "No book_snapshot parquet files found"
    df = pl.read_parquet(snap_files)
    assert len(df) == 1
    # bids col is stored as list[struct{price,size}]
    bids = df["bids"][0]
    price_size_pairs = [(b["price"], b["size"]) for b in bids]
    assert (149.0, 0.0) in price_size_pairs


async def test_parquet_sink_auto_flush_on_row_limit(tmp_path: pathlib.Path) -> None:
    """Buffer auto-flushes when max_buffer_rows is reached."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=3, flush_interval_seconds=9999)

    await sink.put(_trade(150.0))
    await sink.put(_trade(151.0))
    await sink.put(_trade(152.0))

    # Without explicit flush, files should already exist
    trade_files = [p for p in _find_parquets(tmp_path) if "channel=trade" in str(p)]
    assert len(trade_files) >= 1, "Expected auto-flush after max_buffer_rows"


async def test_parquet_sink_never_appends_new_part_files(tmp_path: pathlib.Path) -> None:
    """Two separate flushes produce two distinct part-*.parquet files."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=100, flush_interval_seconds=9999)

    await sink.put(_trade(150.0))
    await sink.flush()
    files_after_first = set(_find_parquets(tmp_path))

    await sink.put(_trade(151.0))
    await sink.flush()
    files_after_second = set(_find_parquets(tmp_path))

    new_files = files_after_second - files_after_first
    assert len(new_files) >= 1, "Second flush should write a new part file, not append"


async def test_parquet_sink_last_flush_updated_after_row_count_flush(
    tmp_path: pathlib.Path,
) -> None:
    """A row-count-triggered flush must update _last_flush."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=3, flush_interval_seconds=9999)

    await sink.put(_trade(150.0))
    await sink.put(_trade(151.0))

    t_after_puts = time.monotonic()

    await sink.put(_trade(152.0))  # row-count flush fires here

    t_upper = time.monotonic()
    assert sink._last_flush >= t_after_puts
    assert sink._last_flush <= t_upper + 0.1
