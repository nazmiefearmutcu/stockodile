import asyncio
from pathlib import Path

from stockodile.schema import DepthProfile as DepthProfileExported
from stockodile.schema.records import DepthProfile, Record
from stockodile.store.catalog import Catalog
from stockodile.store.parquet_sink import ParquetSink
from stockodile.store.rows import from_row, to_row


def test_depthprofile_tag_and_union():
    rec = DepthProfile(
        provider="synth", symbol="synth:AAPL", symbol_raw="AAPL", local_ts=1,
        bids=[(100.0, 5.0)], asks=[(101.0, 4.0)], reference_price=100.5,
        basis="yahoo_1m_vap", is_synthetic=True, depth=2,
    )
    assert type(rec).__struct_config__.tag == "depth"
    assert isinstance(rec, Record)
    assert DepthProfileExported is DepthProfile


def test_depth_row_roundtrip():
    rec = DepthProfile(
        provider="synth", symbol="synth:AAPL", symbol_raw="AAPL",
        local_ts=1_700_000_000_000_000_000,
        bids=[(100.0, 5.0), (99.0, 3.0)], asks=[(101.0, 4.0)], reference_price=100.5,
        basis="yahoo_1m_vap", is_synthetic=True, depth=3,
    )
    row = to_row(rec)
    assert row["channel"] == "depth"
    back = from_row(row)
    assert back == rec


def test_depth_persist_and_query(tmp_path: Path):
    rec = DepthProfile(
        provider="synth", symbol="synth:AAPL", symbol_raw="AAPL",
        local_ts=1_700_000_000_000_000_000,
        bids=[(100.0, 5.0)], asks=[(101.0, 4.0)], reference_price=100.5,
        basis="yahoo_1m_vap", is_synthetic=True, depth=2,
    )

    async def _run() -> None:
        sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=1, flush_interval_seconds=0.1)
        await sink.put(rec)
        await sink.close()

    asyncio.run(_run())
    cat = Catalog(tmp_path)
    df = cat.query("SELECT symbol, basis, is_synthetic, depth FROM depth", readonly=True)
    assert df.height == 1
    assert df["basis"][0] == "yahoo_1m_vap"
    assert bool(df["is_synthetic"][0]) is True
