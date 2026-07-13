"""Acceptance tests for DuckDB Catalog in Stockodile."""

from __future__ import annotations

import datetime
import glob as _glob_mod
import pathlib
from typing import Any

import duckdb
import mmh3
import polars as pl

from stockodile.store.catalog import Catalog, _ns_range_to_dates, _ns_to_date

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TS = 1_700_000_000_000_000_000  # 2023-11-14, a known UTC date
_DAY2_TS = 1_700_006_400_000_000_000  # 2023-11-15T00:00:00 UTC exactly


def _symbol_bucket(symbol: str) -> int:
    return mmh3.hash(symbol, signed=False) % 128


def _date_from_ns(local_ts: int) -> str:
    seconds = local_ts // 1_000_000_000
    dt = datetime.datetime.fromtimestamp(seconds, tz=datetime.UTC)
    return dt.strftime("%Y-%m-%d")


def _trade(
    price: float = 1.0,
    local_ts: int = _BASE_TS,
    provider: str = "alpaca",
    symbol: str = "alpaca:AAPL",
) -> dict[str, Any]:
    return {
        "provider": provider,
        "symbol": symbol,
        "symbol_raw": "AAPL",
        "source_ts": local_ts,
        "local_ts": local_ts,
        "id": str(price),
        "price": price,
        "size": 2.0,
        "conditions": None,
        "tape": "A",
        "venue": None,
    }


def _snap(local_ts: int = _BASE_TS) -> dict[str, Any]:
    return {
        "provider": "alpaca",
        "symbol": "alpaca:AAPL",
        "symbol_raw": "AAPL",
        "source_ts": local_ts,
        "local_ts": local_ts,
        "bids": [(100.0, 5.0), (99.0, 0.0)],
        "asks": [(101.0, 4.0)],
        "depth": 2,
        "sequence_id": 42,
        "is_snapshot": True,
    }


def write_parquet_fixture(
    data_dir: pathlib.Path,
    channel: str,
    provider: str,
    symbol: str,
    rows: list[dict[str, Any]],
) -> None:
    for r in rows:
        r["symbol"] = symbol
        r["channel"] = channel
        r["date"] = _date_from_ns(r["local_ts"])
        r["bucket"] = _symbol_bucket(symbol)

    groups: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for r in rows:
        key = (r["date"], r["bucket"])
        groups.setdefault(key, []).append(r)

    for (date, bucket), group_rows in groups.items():
        df = pl.DataFrame(group_rows)
        path = (
            data_dir
            / f"provider={provider}"
            / f"channel={channel}"
            / f"date={date}"
            / f"bucket={bucket}"
        )
        path.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path / "part-0.parquet")


def write_parquet_fixture_nobucket(
    data_dir: pathlib.Path,
    channel: str,
    provider: str,
    symbol: str,
    rows: list[dict[str, Any]],
) -> None:
    for r in rows:
        r["symbol"] = symbol
        r["channel"] = channel
        r["date"] = _date_from_ns(r["local_ts"])

    groups: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        key = r["date"]
        groups.setdefault(key, []).append(r)

    for date, group_rows in groups.items():
        df = pl.DataFrame(group_rows)
        path = data_dir / f"provider={provider}" / f"channel={channel}" / f"date={date}"
        path.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path / "part-0.parquet")


async def _write_fixtures(data_dir: pathlib.Path) -> None:
    """Write 3 trades + 1 book_snapshot fixture."""
    trades = [
        _trade(100.0, local_ts=_BASE_TS),
        _trade(200.0, local_ts=_BASE_TS + 1_000_000_000),
        _trade(300.0, local_ts=_BASE_TS + 2_000_000_000),
    ]
    write_parquet_fixture(data_dir, "trade", "alpaca", "alpaca:AAPL", trades)

    snaps = [_snap(local_ts=_BASE_TS)]
    write_parquet_fixture(data_dir, "book_snapshot", "alpaca", "alpaca:AAPL", snaps)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_catalog_scan_returns_rows_ordered_by_local_ts(
    tmp_path: pathlib.Path,
) -> None:
    """catalog.scan("trade", symbol, start, end) returns rows ordered by local_ts."""
    await _write_fixtures(tmp_path)

    cat = Catalog(tmp_path)
    start = _BASE_TS
    end = _BASE_TS + 3_000_000_000

    df = cat.scan("trade", "alpaca:AAPL", start, end)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 3
    ts_col = df["local_ts"].to_list()
    assert ts_col == sorted(ts_col), f"Not sorted by local_ts: {ts_col}"
    prices = set(df["price"].to_list())
    assert {100.0, 200.0, 300.0} == prices


async def test_catalog_query_count_matches(tmp_path: pathlib.Path) -> None:
    """catalog.query('SELECT count(*) FROM trade') matches the row count from scan."""
    await _write_fixtures(tmp_path)

    cat = Catalog(tmp_path)
    start = _BASE_TS
    end = _BASE_TS + 3_000_000_000

    df_scan = cat.scan("trade", "alpaca:AAPL", start, end)
    df_count = cat.query("SELECT count(*) AS n FROM trade")

    assert isinstance(df_count, pl.DataFrame)
    total_count = df_count["n"][0]
    assert total_count >= len(df_scan)
    assert total_count == 3


async def test_catalog_scan_filters_by_time_range(tmp_path: pathlib.Path) -> None:
    """scan with a narrow time range returns only matching rows."""
    await _write_fixtures(tmp_path)

    cat = Catalog(tmp_path)
    start = _BASE_TS
    end = _BASE_TS + 500_000_000

    df = cat.scan("trade", "alpaca:AAPL", start, end)
    assert len(df) == 1
    assert df["price"][0] == 100.0


async def test_catalog_scan_empty_result_for_out_of_range(tmp_path: pathlib.Path) -> None:
    """scan with a time range that has no matching rows returns an empty DataFrame."""
    await _write_fixtures(tmp_path)

    cat = Catalog(tmp_path)
    start = _BASE_TS + 1_000_000_000_000
    end = _BASE_TS + 2_000_000_000_000

    df = cat.scan("trade", "alpaca:AAPL", start, end)
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


async def test_catalog_query_returns_polars_dataframe(tmp_path: pathlib.Path) -> None:
    """query() returns a Polars DataFrame regardless of SQL shape."""
    await _write_fixtures(tmp_path)

    cat = Catalog(tmp_path)
    df = cat.query("SELECT symbol, price, local_ts FROM trade ORDER BY local_ts")
    assert isinstance(df, pl.DataFrame)
    assert "symbol" in df.columns
    assert "price" in df.columns
    assert len(df) == 3


async def test_catalog_scan_unknown_channel_returns_empty(tmp_path: pathlib.Path) -> None:
    """scan on a channel with no files returns an empty DataFrame, no exception."""
    await _write_fixtures(tmp_path)

    cat = Catalog(tmp_path)
    df = cat.scan("liquidation", "alpaca:AAPL", _BASE_TS, _BASE_TS + 9_999_999_999)
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


async def test_catalog_scan_multiday_partition_pruning(tmp_path: pathlib.Path) -> None:
    """Records on two calendar days: a scan for day N does not touch day N+1 files."""
    trade_day1 = _trade(price=1.0, local_ts=_BASE_TS)
    trade_day2 = _trade(price=2.0, local_ts=_DAY2_TS)
    write_parquet_fixture(tmp_path, "trade", "alpaca", "alpaca:AAPL", [trade_day1, trade_day2])

    cat = Catalog(tmp_path)

    day1_end = _DAY2_TS - 1
    df_day1 = cat.scan("trade", "alpaca:AAPL", _BASE_TS, day1_end)
    assert len(df_day1) == 1
    assert df_day1["price"][0] == 1.0

    df_day2 = cat.scan("trade", "alpaca:AAPL", _DAY2_TS, _DAY2_TS + 1_000_000_000)
    assert len(df_day2) == 1
    assert df_day2["price"][0] == 2.0

    day1_dirs = _glob_mod.glob(str(tmp_path / "provider=*" / "channel=trade" / "date=2023-11-14"))
    day2_dirs = _glob_mod.glob(str(tmp_path / "provider=*" / "channel=trade" / "date=2023-11-15"))
    assert day1_dirs
    assert day2_dirs


def test_ns_to_date_uses_integer_division() -> None:
    """Regression: ns→date must use integer division."""
    assert _ns_to_date(_DAY2_TS - 1) == "2023-11-14"
    assert _ns_to_date(_DAY2_TS) == "2023-11-15"


def test_ns_range_to_dates_does_not_overinclude() -> None:
    """Regression: a range ending one ns before midnight must not pull in the next day."""
    assert _ns_range_to_dates(_BASE_TS, _DAY2_TS - 1) == ["2023-11-14"]
    assert _ns_range_to_dates(_BASE_TS, _DAY2_TS) == ["2023-11-14", "2023-11-15"]


async def test_catalog_single_quote_in_data_dir(tmp_path: pathlib.Path) -> None:
    """Catalog must not crash when data_dir contains a single quote."""
    quoted_dir = tmp_path / "data's_dir"
    quoted_dir.mkdir()

    trades = [_trade()]
    write_parquet_fixture(quoted_dir, "trade", "alpaca", "alpaca:AAPL", trades)

    cat = Catalog(quoted_dir)
    df = cat.scan("trade", "alpaca:AAPL", _BASE_TS, _BASE_TS + 9_999_999_999)
    assert len(df) == 1


def test_catalog_connection_property_returns_duckdb_connection(tmp_path: pathlib.Path) -> None:
    """Catalog.connection must return the DuckDB connection."""
    cat = Catalog(tmp_path)
    conn = cat.connection
    assert isinstance(conn, duckdb.DuckDBPyConnection)


def test_catalog_connection_is_functional(tmp_path: pathlib.Path) -> None:
    """catalog.connection must be able to execute SQL queries."""
    cat = Catalog(tmp_path)
    conn = cat.connection
    result = conn.execute("SELECT 42 AS answer").pl()
    assert result["answer"][0] == 42


def test_catalog_connection_same_object_as_internal(tmp_path: pathlib.Path) -> None:
    """catalog.connection must return the same object as the internal _conn attribute."""
    cat = Catalog(tmp_path)
    assert cat.connection is cat._conn


async def test_catalog_scan_nobucket(tmp_path: pathlib.Path) -> None:
    """Catalog must be able to read and scan non-bucketed layout."""
    trades = [
        _trade(100.0, local_ts=_BASE_TS),
        _trade(200.0, local_ts=_BASE_TS + 1_000_000_000),
    ]
    write_parquet_fixture_nobucket(tmp_path, "fundamental", "sec", "sec:AAPL", trades)

    cat = Catalog(tmp_path)
    df = cat.scan("fundamental", "sec:AAPL", _BASE_TS, _BASE_TS + 3_000_000_000)
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2
    assert df["price"][0] == 100.0
    assert df["price"][1] == 200.0


def test_assert_readonly_sql_rejects_copy() -> None:
    from stockodile.store.catalog import assert_readonly_sql
    import pytest

    with pytest.raises(ValueError, match="disallowed|Only SELECT|Multi-statement|Empty"):
        assert_readonly_sql("COPY (SELECT 1) TO '/tmp/x.csv'")


def test_assert_readonly_sql_allows_select() -> None:
    from stockodile.store.catalog import assert_readonly_sql

    assert_readonly_sql("SELECT 1 AS x")


def test_query_readonly_blocks_mutating_sql(tmp_path: pathlib.Path) -> None:
    import pytest

    cat = Catalog(tmp_path)
    with pytest.raises(ValueError):
        cat.query("DROP TABLE IF EXISTS trade", readonly=True)
    cat.close()
