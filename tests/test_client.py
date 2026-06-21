"""Acceptance tests for StockodileClient."""

from __future__ import annotations

import pathlib

import polars as pl

from stockodile.client.client import StockodileClient
from stockodile.schema.records import BookSnapshot, Trade
from stockodile.store.parquet_sink import ParquetSink

_BASE_TS = 1_700_000_000_000_000_000  # 2023-11-14


def _trade(
    price: float = 1.0,
    local_ts: int = _BASE_TS,
    provider: str = "alpaca",
    symbol: str = "alpaca:AAPL",
) -> Trade:
    return Trade(
        provider=provider,
        symbol=symbol,
        symbol_raw="AAPL",
        source_ts=local_ts,
        local_ts=local_ts,
        id=str(price),
        price=price,
        size=2.0,
    )


def _snap(local_ts: int = _BASE_TS) -> BookSnapshot:
    return BookSnapshot(
        provider="alpaca",
        symbol="alpaca:AAPL",
        symbol_raw="AAPL",
        source_ts=local_ts,
        local_ts=local_ts,
        bids=[(100.0, 5.0)],
        asks=[(101.0, 4.0)],
        depth=1,
        sequence_id=1,
        is_snapshot=True,
    )


async def _write_fixtures(data_dir: pathlib.Path) -> None:
    sink = ParquetSink(data_dir=data_dir, max_buffer_rows=10, flush_interval_seconds=9999)
    await sink.put(_trade(100.0, local_ts=_BASE_TS))
    await sink.put(_trade(200.0, local_ts=_BASE_TS + 1_000_000_000))
    await sink.put(_trade(300.0, local_ts=_BASE_TS + 2_000_000_000))
    await sink.put(_snap(local_ts=_BASE_TS))
    await sink.flush()


async def test_client_query_returns_polars_dataframe(tmp_path: pathlib.Path) -> None:
    """client.query(sql) delegates to Catalog and returns a Polars DataFrame."""
    await _write_fixtures(tmp_path)
    client = StockodileClient(data_dir=tmp_path)
    df = client.query("SELECT count(*) AS n FROM trade")
    assert isinstance(df, pl.DataFrame)
    assert df["n"][0] == 3


async def test_client_scan_single_symbol_returns_rows(tmp_path: pathlib.Path) -> None:
    """client.scan with one symbol returns rows matching catalog.scan output."""
    await _write_fixtures(tmp_path)
    client = StockodileClient(data_dir=tmp_path)
    df = client.scan(
        "trade",
        ["alpaca:AAPL"],
        _BASE_TS,
        _BASE_TS + 3_000_000_000,
    )
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 3
    ts_col = df["local_ts"].to_list()
    assert ts_col == sorted(ts_col), "Not sorted by local_ts"
    assert set(df["price"].to_list()) == {100.0, 200.0, 300.0}


async def test_client_scan_multi_symbol_unions_results(tmp_path: pathlib.Path) -> None:
    """client.scan with multiple symbols concatenates results ordered by local_ts."""
    sink = ParquetSink(data_dir=tmp_path, max_buffer_rows=10, flush_interval_seconds=9999)
    await sink.put(_trade(1.0, local_ts=_BASE_TS, symbol="alpaca:AAPL"))
    await sink.put(
        Trade(
            provider="alpaca",
            symbol="alpaca:MSFT",
            symbol_raw="MSFT",
            source_ts=_BASE_TS + 500_000_000,
            local_ts=_BASE_TS + 500_000_000,
            id="msft1",
            price=2000.0,
            size=1.0,
        )
    )
    await sink.flush()

    client = StockodileClient(data_dir=tmp_path)
    df = client.scan(
        "trade",
        ["alpaca:AAPL", "alpaca:MSFT"],
        _BASE_TS,
        _BASE_TS + 2_000_000_000,
    )
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2
    ts_col = df["local_ts"].to_list()
    assert ts_col == sorted(ts_col), "Multi-symbol results must be sorted by local_ts"


async def test_client_scan_empty_symbols_returns_empty(tmp_path: pathlib.Path) -> None:
    """client.scan with empty symbols list returns an empty DataFrame."""
    await _write_fixtures(tmp_path)
    client = StockodileClient(data_dir=tmp_path)
    df = client.scan("trade", [], _BASE_TS, _BASE_TS + 3_000_000_000)
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


async def test_client_scan_no_matching_rows_returns_empty(tmp_path: pathlib.Path) -> None:
    """client.scan with out-of-range time returns empty DataFrame."""
    await _write_fixtures(tmp_path)
    client = StockodileClient(data_dir=tmp_path)
    df = client.scan(
        "trade",
        ["alpaca:AAPL"],
        _BASE_TS + 1_000_000_000_000,
        _BASE_TS + 2_000_000_000_000,
    )
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


async def test_client_replay(tmp_path: pathlib.Path) -> None:
    """client.replay returns matching records globally sorted by local_ts."""
    await _write_fixtures(tmp_path)
    client = StockodileClient(data_dir=tmp_path)
    records = list(
        client.replay(
            ["trade", "book_snapshot"],
            ["alpaca:AAPL"],
            _BASE_TS,
            _BASE_TS + 3_000_000_000,
        )
    )
    assert len(records) == 4
    # Check that they are sorted by local_ts
    for idx in range(len(records) - 1):
        assert records[idx].local_ts <= records[idx + 1].local_ts


async def test_client_export(tmp_path: pathlib.Path) -> None:
    """client.export exports to multiple formats successfully."""
    await _write_fixtures(tmp_path)
    client = StockodileClient(data_dir=tmp_path)

    csv_dest = tmp_path / "export.csv"
    client.export("trade", ["alpaca:AAPL"], _BASE_TS, _BASE_TS + 3_000_000_000, "csv", csv_dest)
    assert csv_dest.exists()
    assert csv_dest.stat().st_size > 0

    pq_dest = tmp_path / "export.parquet"
    client.export("trade", ["alpaca:AAPL"], _BASE_TS, _BASE_TS + 3_000_000_000, "parquet", pq_dest)
    assert pq_dest.exists()
    assert pq_dest.stat().st_size > 0


async def test_client_resample(tmp_path: pathlib.Path) -> None:
    """client.resample groups trades into OHLCV bars correctly."""
    await _write_fixtures(tmp_path)
    client = StockodileClient(data_dir=tmp_path)
    df = client.resample("alpaca:AAPL", _BASE_TS, _BASE_TS + 3_000_000_000, "1s")
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert "open" in df.columns
    assert "high" in df.columns
