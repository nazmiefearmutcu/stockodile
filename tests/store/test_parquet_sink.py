"""Acceptance tests for ParquetSink in Stockodile."""

from __future__ import annotations

import os
import pathlib
import time

import polars as pl
import pytest

from stockodile.schema.enums import Tape
from stockodile.schema.records import BookSnapshot, Trade
from stockodile.store.parquet_sink import ParquetSink


def _trade(
    price: float = 150.0,
    local_ts: int = 1700000000000000000,
    *,
    provider: str = "alpaca",
) -> Trade:
    return Trade(
        provider=provider,
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


async def test_flush_failure_requeues_records(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failed writes must re-queue buffer records so data is not discarded."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=100, flush_interval_seconds=9999)
    t1 = _trade(150.0)
    t2 = _trade(151.0)
    await sink.put(t1)
    await sink.put(t2)

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(sink, "_write_parquet_sync", _boom)

    with pytest.raises(OSError, match="disk full"):
        await sink.flush()

    assert len(sink._buffers["trade"]) == 2
    assert sink._buffers["trade"][0] is t1
    assert sink._buffers["trade"][1] is t2
    assert _find_parquets(tmp_path) == []


async def test_flush_grouping_exception_requeues_records(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exception during pre-write grouping must restore popped buffer records."""
    import stockodile.store.parquet_sink as parquet_sink_mod

    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=100, flush_interval_seconds=9999)
    t1 = _trade(150.0)
    t2 = _trade(151.0)
    await sink.put(t1)
    await sink.put(t2)

    def _boom_bucket(_symbol: str) -> int:
        raise RuntimeError("bucket mapping failed")

    monkeypatch.setattr(parquet_sink_mod, "_symbol_bucket", _boom_bucket)

    with pytest.raises(RuntimeError, match="bucket mapping failed"):
        await sink.flush()

    assert len(sink._buffers["trade"]) == 2
    assert sink._buffers["trade"][0] is t1
    assert sink._buffers["trade"][1] is t2
    assert _find_parquets(tmp_path) == []


async def test_evil_provider_does_not_write_outside_data_dir(tmp_path: pathlib.Path) -> None:
    """Path traversal / slash in provider must not escape data_dir."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=100, flush_interval_seconds=9999)
    data_dir = tmp_path.resolve()

    for evil in ("../evil", "foo/bar", r"foo\bar", "a/../../etc", "..", "x\x00y"):
        await sink.put(_trade(provider=evil))

    await sink.flush()

    # Rejected providers must not produce part files
    assert _find_parquets(tmp_path) == []

    # No path under data_dir may resolve outside it
    for p in tmp_path.rglob("*"):
        resolved = p.resolve()
        assert resolved == data_dir or data_dir in resolved.parents

    # Classic escape target must not appear as a sibling of data_dir
    assert not (tmp_path.parent / "evil").exists()


async def test_atomic_write_uses_os_replace(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Part files are written to .tmp then atomically replaced into place."""
    replace_calls: list[tuple[str, str]] = []
    real_replace = os.replace

    def _tracking_replace(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        replace_calls.append((str(src), str(dst)))
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", _tracking_replace)

    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=100, flush_interval_seconds=9999)
    await sink.put(_trade())
    await sink.flush()

    assert replace_calls, "expected os.replace for atomic part publish"
    src, dst = replace_calls[0]
    assert src.endswith(".parquet.tmp")
    assert dst.endswith(".parquet")
    assert not dst.endswith(".tmp")
    assert pathlib.Path(dst).is_file()
    assert not pathlib.Path(src).exists()
    assert _find_parquets(tmp_path)


async def test_atomic_write_no_final_on_mid_failure(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the write fails mid-flight, no final part-*.parquet should appear."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=100, flush_interval_seconds=9999)
    await sink.put(_trade())

    def _failing_write(self: pl.DataFrame, file: object, *args: object, **kwargs: object) -> None:
        path = pathlib.Path(str(file))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"partial-corrupt")
        raise OSError("write interrupted")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", _failing_write)

    with pytest.raises(OSError, match="write interrupted"):
        await sink.flush()

    finals = [p for p in tmp_path.rglob("part-*.parquet") if not str(p).endswith(".tmp")]
    assert finals == [], f"corrupt final part files present: {finals}"
    assert len(sink._buffers["trade"]) == 1
