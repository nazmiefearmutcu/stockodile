"""Tests for the CoverageResolver."""

from __future__ import annotations

import msgspec
import polars as pl
import pytest

from stockodile.coverage import CoverageResolver
from stockodile.schema.enums import OptType
from stockodile.schema.records import Bar, OptionQuote


def test_resolver_records_priority() -> None:
    """Test priority strategy for merging msgspec Records."""
    r_tiingo = Bar(
        provider="tiingo",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=1000,
        interval="1d",
        open=150.0,
        high=155.0,
        low=149.0,
        close=152.0,
        volume=100.0,
    )
    r_yahoo = Bar(
        provider="yahoo",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=1000,
        interval="1d",
        open=151.0,
        high=156.0,
        low=150.0,
        close=153.0,
        volume=200.0,
    )
    r_stooq = Bar(
        provider="stooq",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=1000,
        interval="1d",
        open=149.0,
        high=154.0,
        low=148.0,
        close=151.0,
        volume=300.0,
    )

    # 1. yahoo > tiingo > stooq
    resolver = CoverageResolver(priority_list=["yahoo", "tiingo", "stooq"])
    res = resolver.resolve_records([r_tiingo, r_yahoo, r_stooq], strategy="priority")
    assert len(res) == 1
    rec1 = res[0]
    assert isinstance(rec1, Bar)
    assert rec1.provider == "yahoo"
    assert rec1.open == 151.0

    # 2. tiingo > yahoo > stooq
    resolver = CoverageResolver(priority_list=["tiingo", "yahoo", "stooq"])
    res = resolver.resolve_records([r_tiingo, r_yahoo, r_stooq], strategy="priority")
    assert len(res) == 1
    rec2 = res[0]
    assert isinstance(rec2, Bar)
    assert rec2.provider == "tiingo"
    assert rec2.open == 150.0


def test_resolver_records_fill_nulls() -> None:
    """Test fill_nulls strategy for merging msgspec Records."""
    # yahoo is top priority but has some None fields
    r_yahoo = OptionQuote(
        provider="yahoo",
        symbol="AAPL260619C00150000",
        symbol_raw="AAPL260619C00150000",
        source_ts=None,
        local_ts=1000,
        underlying="AAPL",
        strike=150.0,
        expiry="2026-06-19",
        type=OptType.C,
        bid=10.0,
        ask=None,  # to be filled
        volume=100.0,
        delta=None,  # to be filled
    )
    # tiingo has ask but not delta
    r_tiingo = OptionQuote(
        provider="tiingo",
        symbol="AAPL260619C00150000",
        symbol_raw="AAPL260619C00150000",
        source_ts=None,
        local_ts=1000,
        underlying="AAPL",
        strike=150.0,
        expiry="2026-06-19",
        type=OptType.C,
        bid=10.5,
        ask=11.2,
        volume=200.0,
        delta=None,
    )
    # stooq has delta
    r_stooq = OptionQuote(
        provider="stooq",
        symbol="AAPL260619C00150000",
        symbol_raw="AAPL260619C00150000",
        source_ts=None,
        local_ts=1000,
        underlying="AAPL",
        strike=150.0,
        expiry="2026-06-19",
        type=OptType.C,
        bid=9.8,
        ask=11.0,
        volume=300.0,
        delta=0.65,
    )

    resolver = CoverageResolver(priority_list=["yahoo", "tiingo", "stooq"])
    res = resolver.resolve_records([r_stooq, r_tiingo, r_yahoo], strategy="fill_nulls")
    assert len(res) == 1
    merged = res[0]
    assert isinstance(merged, OptionQuote)
    assert merged.provider == "yahoo"
    assert merged.bid == 10.0  # from yahoo
    assert merged.ask == 11.2  # filled from tiingo
    assert merged.volume == 100.0  # from yahoo
    assert merged.delta == 0.65  # filled from stooq


def test_resolver_df_priority() -> None:
    """Test priority strategy for merging Polars DataFrames."""
    df = pl.DataFrame(
        [
            {"symbol": "AAPL", "local_ts": 1000, "provider": "stooq", "close": 150.0},
            {"symbol": "AAPL", "local_ts": 1000, "provider": "yahoo", "close": 151.0},
            {"symbol": "AAPL", "local_ts": 1000, "provider": "tiingo", "close": 152.0},
            {"symbol": "MSFT", "local_ts": 2000, "provider": "tiingo", "close": 300.0},
            {"symbol": "MSFT", "local_ts": 2000, "provider": "yahoo", "close": 301.0},
        ]
    )

    resolver = CoverageResolver(priority_list=["yahoo", "tiingo", "stooq"])
    res = resolver.resolve_df(df, strategy="priority")
    assert len(res) == 2

    # AAPL
    aapl_row = res.filter(pl.col("symbol") == "AAPL").row(0, named=True)
    assert aapl_row["provider"] == "yahoo"
    assert aapl_row["close"] == 151.0

    # MSFT
    msft_row = res.filter(pl.col("symbol") == "MSFT").row(0, named=True)
    assert msft_row["provider"] == "yahoo"
    assert msft_row["close"] == 301.0


def test_resolver_df_fill_nulls() -> None:
    """Test fill_nulls strategy for merging Polars DataFrames."""
    df = pl.DataFrame(
        [
            {
                "symbol": "AAPL",
                "local_ts": 1000,
                "provider": "yahoo",
                "open": 150.0,
                "high": None,
                "low": 149.0,
                "close": None,
            },
            {
                "symbol": "AAPL",
                "local_ts": 1000,
                "provider": "tiingo",
                "open": 151.0,
                "high": 155.0,
                "low": 148.0,
                "close": 152.0,
            },
        ]
    )

    resolver = CoverageResolver(priority_list=["yahoo", "tiingo"])
    res = resolver.resolve_df(df, strategy="fill_nulls")
    assert len(res) == 1
    row = res.row(0, named=True)
    assert row["provider"] == "yahoo"
    assert row["open"] == 150.0  # from yahoo
    assert row["high"] == 155.0  # from tiingo
    assert row["low"] == 149.0  # from yahoo
    assert row["close"] == 152.0  # from tiingo


def test_empty_inputs() -> None:
    """Test empty records list and empty Polars DataFrames return empty results."""
    resolver = CoverageResolver(priority_list=["yahoo"])
    assert resolver.resolve_records([]) == []

    empty_df = pl.DataFrame(
        schema={"symbol": pl.String, "local_ts": pl.Int64, "provider": pl.String}
    )
    res_df = resolver.resolve_df(empty_df)
    assert res_df.height == 0
    assert list(res_df.columns) == ["symbol", "local_ts", "provider"]


def test_custom_columns() -> None:
    """Test resolver works with custom column names."""
    df = pl.DataFrame(
        [
            {"ticker": "AAPL", "time": "2026-06-21", "source": "stooq", "close": 150.0},
            {"ticker": "AAPL", "time": "2026-06-21", "source": "yahoo", "close": 151.0},
        ]
    )

    resolver = CoverageResolver(
        priority_list=["yahoo", "stooq"],
        symbol_col="ticker",
        timestamp_col="time",
        provider_col="source",
    )

    # DataFrame test
    res_df = resolver.resolve_df(df, strategy="priority")
    assert len(res_df) == 1
    row = res_df.row(0, named=True)
    assert row["ticker"] == "AAPL"
    assert row["time"] == "2026-06-21"
    assert row["source"] == "yahoo"
    assert row["close"] == 151.0

    # Records test using custom field mappings (mocking msgspec records with custom attributes)
    class CustomRecord(msgspec.Struct):
        ticker: str
        time: str
        source: str
        close: float | None

    r_stooq = CustomRecord(ticker="AAPL", time="2026-06-21", source="stooq", close=150.0)
    r_yahoo = CustomRecord(ticker="AAPL", time="2026-06-21", source="yahoo", close=None)

    # Under priority strategy
    res_rec = resolver.resolve_records([r_stooq, r_yahoo], strategy="priority")  # type: ignore
    assert len(res_rec) == 1
    assert res_rec[0].source == "yahoo"  # type: ignore

    # Under fill_nulls strategy
    res_rec_fill = resolver.resolve_records([r_stooq, r_yahoo], strategy="fill_nulls")  # type: ignore
    assert len(res_rec_fill) == 1
    assert res_rec_fill[0].source == "yahoo"  # type: ignore
    assert res_rec_fill[0].close == 150.0  # type: ignore


def test_unregistered_providers() -> None:
    """Test that providers not in priority list are treated as lowest priority."""
    r_unregistered = Bar(
        provider="unknown",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=1000,
        interval="1d",
        open=150.0,
        high=155.0,
        low=149.0,
        close=152.0,
        volume=100.0,
    )
    r_yahoo = Bar(
        provider="yahoo",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=1000,
        interval="1d",
        open=151.0,
        high=156.0,
        low=150.0,
        close=153.0,
        volume=200.0,
    )

    resolver = CoverageResolver(priority_list=["yahoo"])
    # yahoo has priority over unregistered
    res = resolver.resolve_records([r_unregistered, r_yahoo], strategy="priority")
    assert len(res) == 1
    rec = res[0]
    assert isinstance(rec, Bar)
    assert rec.provider == "yahoo"

    # For DataFrame
    df = pl.DataFrame(
        [
            {"symbol": "AAPL", "local_ts": 1000, "provider": "unknown", "close": 150.0},
            {"symbol": "AAPL", "local_ts": 1000, "provider": "yahoo", "close": 151.0},
        ]
    )
    res_df = resolver.resolve_df(df, strategy="priority")
    assert len(res_df) == 1
    assert res_df.row(0, named=True)["provider"] == "yahoo"


def test_unified_resolve_api() -> None:
    """Test the unified resolve method routes correctly to records or df."""
    resolver = CoverageResolver(priority_list=["yahoo"])

    # 1. Test routing list of records
    r = Bar(
        provider="yahoo",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=1000,
        interval="1d",
        open=150.0,
        high=150.0,
        low=150.0,
        close=150.0,
        volume=100.0,
    )
    res_rec = resolver.resolve([r])
    assert isinstance(res_rec, list)
    assert len(res_rec) == 1
    rec = res_rec[0]
    assert isinstance(rec, Bar)
    assert rec.provider == "yahoo"

    # 2. Test routing Polars DataFrame
    df = pl.DataFrame([{"symbol": "AAPL", "local_ts": 1000, "provider": "yahoo", "close": 150.0}])
    res_df = resolver.resolve(df)
    assert isinstance(res_df, pl.DataFrame)
    assert len(res_df) == 1
    assert res_df.row(0, named=True)["provider"] == "yahoo"


def test_invalid_strategies_and_validation() -> None:
    """Test validation and error handling for invalid input configurations."""
    resolver = CoverageResolver(priority_list=["yahoo"])

    # 1. Invalid strategy
    with pytest.raises(ValueError, match="Unknown strategy"):
        resolver.resolve_records([], strategy="invalid_strategy")  # type: ignore

    with pytest.raises(ValueError, match="Unknown strategy"):
        resolver.resolve_df(
            pl.DataFrame([{"symbol": "AAPL", "local_ts": 1000, "provider": "yahoo"}]),
            strategy="invalid_strategy",  # type: ignore
        )

    # 2. Missing columns in DataFrame
    bad_df = pl.DataFrame([{"local_ts": 1000, "provider": "yahoo"}])  # missing symbol
    with pytest.raises(ValueError, match="Required symbol column"):
        resolver.resolve_df(bad_df)
