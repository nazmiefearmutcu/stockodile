"""Tests for Stockodile resampling algorithms."""

from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
import pytest

from stockodile.replay.orderbook import BookGap
from stockodile.resample import (
    parse_interval,
    resample_bars_df,
    resample_bars_to_bars,
    resample_book_snapshots,
    resample_ohlcv,
    resample_quotes_df,
    resample_quotes_to_bars,
    resample_trades_df,
    resample_trades_to_bars,
)
from stockodile.schema.records import Bar, BookDelta, BookSnapshot, Quote, Trade
from stockodile.store.catalog import Catalog


def test_parse_interval() -> None:
    """Test parse_interval translates shorthand correctly."""
    assert parse_interval("1s") == (1_000_000_000, "INTERVAL '1 second'", "1s")
    assert parse_interval("5m") == (300_000_000_000, "INTERVAL '5 minute'", "5m")
    assert parse_interval("1h") == (3_600_000_000_000, "INTERVAL '1 hour'", "1h")
    assert parse_interval("1d") == (86_400_000_000_000, "INTERVAL '1 day'", "1d")

    with pytest.raises(ValueError):
        parse_interval("1x")


def test_resample_trades_to_bars() -> None:
    """Test stream resampling of Trades to Bars."""
    trades = [
        Trade(
            provider="alpaca",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=100_000_000,
            id="1",
            price=150.0,
            size=10.0,
        ),
        Trade(
            provider="alpaca",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=500_000_000,
            id="2",
            price=152.0,
            size=20.0,
        ),
        Trade(
            provider="alpaca",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=1_200_000_000,
            id="3",
            price=151.0,
            size=15.0,
        ),
    ]

    bars = list(resample_trades_to_bars(trades, "1s"))
    assert len(bars) == 2

    # First bar (0s to 1s bucket)
    assert bars[0].local_ts == 0
    assert bars[0].open == 150.0
    assert bars[0].high == 152.0
    assert bars[0].low == 150.0
    assert bars[0].close == 152.0
    assert bars[0].volume == 30.0
    assert bars[0].vwap == pytest.approx((150.0 * 10.0 + 152.0 * 20.0) / 30.0)
    assert bars[0].trade_count == 2

    # Second bar (1s to 2s bucket)
    assert bars[1].local_ts == 1_000_000_000
    assert bars[1].open == 151.0
    assert bars[1].high == 151.0
    assert bars[1].low == 151.0
    assert bars[1].close == 151.0
    assert bars[1].volume == 15.0
    assert bars[1].vwap == 151.0
    assert bars[1].trade_count == 1


def test_resample_quotes_to_bars() -> None:
    """Test stream resampling of Quotes to Bars."""
    quotes = [
        Quote(
            provider="alpaca",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=100_000_000,
            bid_px=149.0,
            bid_sz=100.0,
            ask_px=151.0,
            ask_sz=200.0,
        ),
        Quote(
            provider="alpaca",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=500_000_000,
            bid_px=150.0,
            bid_sz=150.0,
            ask_px=152.0,
            ask_sz=250.0,
        ),
    ]

    # Resample quotes mid-price (mid of Q1: 150.0, mid of Q2: 151.0)
    bars = list(resample_quotes_to_bars(quotes, "1s", price_type="mid"))
    assert len(bars) == 1
    assert bars[0].local_ts == 0
    assert bars[0].open == 150.0
    assert bars[0].close == 151.0
    assert bars[0].high == 151.0
    assert bars[0].low == 150.0
    assert bars[0].volume == 0.0
    assert bars[0].vwap == 150.5
    assert bars[0].trade_count == 2


def test_resample_bars_to_bars() -> None:
    """Test resampling of lower resolution bars to higher resolution bars."""
    bars_1s = [
        Bar(
            provider="alpaca",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=0,
            interval="1s",
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.0,
            volume=1000.0,
            vwap=102.0,
            trade_count=10,
        ),
        Bar(
            provider="alpaca",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=1_000_000_000,
            interval="1s",
            open=102.0,
            high=103.0,
            low=101.0,
            close=102.5,
            volume=2000.0,
            vwap=102.2,
            trade_count=20,
        ),
        Bar(
            provider="alpaca",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=60_000_000_000,
            interval="1s",
            open=103.0,
            high=104.0,
            low=102.0,
            close=103.5,
            volume=1500.0,
            vwap=103.2,
            trade_count=15,
        ),
    ]

    # Resample 1s bars to 1m (60s) bars
    bars_1m = list(resample_bars_to_bars(bars_1s, "1m"))
    assert len(bars_1m) == 2

    # First 1m bar (includes 0s and 1s bars)
    assert bars_1m[0].local_ts == 0
    assert bars_1m[0].open == 100.0
    assert bars_1m[0].high == 105.0
    assert bars_1m[0].low == 99.0
    assert bars_1m[0].close == 102.5
    assert bars_1m[0].volume == 3000.0
    assert bars_1m[0].vwap == pytest.approx((102.0 * 1000.0 + 102.2 * 2000.0) / 3000.0)
    assert bars_1m[0].trade_count == 30

    # Second 1m bar (includes 60s bar)
    assert bars_1m[1].local_ts == 60_000_000_000
    assert bars_1m[1].open == 103.0
    assert bars_1m[1].high == 104.0
    assert bars_1m[1].low == 102.0
    assert bars_1m[1].close == 103.5
    assert bars_1m[1].volume == 1500.0
    assert bars_1m[1].vwap == 103.2
    assert bars_1m[1].trade_count == 15


def test_resample_trades_df() -> None:
    """Test Polars-based trade resampling."""
    df = pl.DataFrame(
        [
            {"local_ts": 100_000_000, "price": 150.0, "size": 10.0, "symbol": "AAPL"},
            {"local_ts": 500_000_000, "price": 152.0, "size": 20.0, "symbol": "AAPL"},
            {"local_ts": 1_200_000_000, "price": 151.0, "size": 15.0, "symbol": "AAPL"},
        ]
    )
    res = resample_trades_df(df, "1s")
    assert len(res) == 2
    assert res.row(0, named=True)["bar"] == 0
    assert res.row(0, named=True)["open"] == 150.0
    assert res.row(0, named=True)["close"] == 152.0
    assert res.row(0, named=True)["volume"] == 30.0
    assert res.row(0, named=True)["vwap"] == pytest.approx(
        (150.0 * 10.0 + 152.0 * 20.0) / 30.0
    )
    assert res.row(0, named=True)["trade_count"] == 2

    assert res.row(1, named=True)["bar"] == 1_000_000_000
    assert res.row(1, named=True)["close"] == 151.0
    assert res.row(1, named=True)["volume"] == 15.0
    assert res.row(1, named=True)["trade_count"] == 1


def test_resample_quotes_df() -> None:
    """Test Polars-based quote resampling."""
    df = pl.DataFrame(
        [
            {"local_ts": 100_000_000, "bid_px": 149.0, "ask_px": 151.0, "symbol": "AAPL"},
            {"local_ts": 500_000_000, "bid_px": 150.0, "ask_px": 152.0, "symbol": "AAPL"},
        ]
    )
    res = resample_quotes_df(df, "1s", price_type="mid")
    assert len(res) == 1
    assert res.row(0, named=True)["bar"] == 0
    assert res.row(0, named=True)["open"] == 150.0
    assert res.row(0, named=True)["close"] == 151.0
    assert res.row(0, named=True)["volume"] == 0.0
    assert res.row(0, named=True)["vwap"] == 150.5
    assert res.row(0, named=True)["trade_count"] == 2


def test_resample_bars_df() -> None:
    """Test Polars-based bar resampling."""
    df = pl.DataFrame(
        [
            {
                "local_ts": 0,
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 102.0,
                "volume": 1000.0,
                "vwap": 102.0,
                "trade_count": 10,
                "symbol": "AAPL",
            },
            {
                "local_ts": 1_000_000_000,
                "open": 102.0,
                "high": 103.0,
                "low": 101.0,
                "close": 102.5,
                "volume": 2000.0,
                "vwap": 102.2,
                "trade_count": 20,
                "symbol": "AAPL",
            },
        ]
    )
    res = resample_bars_df(df, "1m")
    assert len(res) == 1
    assert res.row(0, named=True)["bar"] == 0
    assert res.row(0, named=True)["open"] == 100.0
    assert res.row(0, named=True)["high"] == 105.0
    assert res.row(0, named=True)["low"] == 99.0
    assert res.row(0, named=True)["close"] == 102.5
    assert res.row(0, named=True)["volume"] == 3000.0
    assert res.row(0, named=True)["vwap"] == pytest.approx(
        (102.0 * 1000.0 + 102.2 * 2000.0) / 3000.0
    )
    assert res.row(0, named=True)["trade_count"] == 30


def test_resample_book_snapshots() -> None:
    """Test generating order book snapshots from BookSnapshot and BookDelta stream."""
    records: list[BookSnapshot | BookDelta] = [
        BookSnapshot(
            provider="iex",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=100_000_000,
            bids=[(150.0, 10.0), (149.0, 20.0)],
            asks=[(151.0, 15.0), (152.0, 25.0)],
            depth=4,
        ),
        BookDelta(
            provider="iex",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=500_000_000,
            bids=[(150.0, 12.0)],  # update bid size
            asks=[(151.0, 0.0)],  # remove ask price 151
            seq_id=1,
        ),
        BookDelta(
            provider="iex",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=1_200_000_000,
            bids=[(149.0, 0.0)],  # remove bid price 149
            asks=[(153.0, 30.0)],  # add ask price 153
            seq_id=2,
        ),
        BookDelta(
            provider="iex",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=2_200_000_000,
            bids=[],
            asks=[],
            seq_id=3,
        ),
    ]

    snapshots = list(resample_book_snapshots(records, 1_000_000_000))
    assert len(snapshots) == 2

    # First snapshot boundary at 1_000_000_000 ns
    # Captures state after local_ts=1_200_000_000 delta has been applied
    snap1 = snapshots[0]
    assert snap1.local_ts == 1_000_000_000
    assert snap1.bids == [(150.0, 12.0)]
    assert snap1.asks == [(152.0, 25.0), (153.0, 30.0)]

    # Second snapshot boundary at 2_000_000_000 ns
    # Captures state after local_ts=2_200_000_000 delta has been applied
    snap2 = snapshots[1]
    assert snap2.local_ts == 2_000_000_000
    assert snap2.bids == [(150.0, 12.0)]
    assert snap2.asks == [(152.0, 25.0), (153.0, 30.0)]


def test_resample_book_snapshots_gaps() -> None:
    """Test that BookGap is raised when L2 stream seq_id is discontinuous."""
    records: list[BookSnapshot | BookDelta] = [
        BookSnapshot(
            provider="iex",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=100_000_000,
            bids=[(150.0, 10.0)],
            asks=[(151.0, 15.0)],
            depth=2,
            sequence_id=0,
        ),
        BookDelta(
            provider="iex",
            symbol="AAPL",
            symbol_raw="AAPL",
            source_ts=None,
            local_ts=500_000_000,
            bids=[(150.0, 12.0)],
            asks=[],
            seq_id=5,  # gap! expected 1
        ),
    ]

    with pytest.raises(BookGap):
        list(resample_book_snapshots(records, 1_000_000_000))


def test_resample_ohlcv_catalog() -> None:
    """Test resampling from Catalog / DuckDB."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create mock parquet files mimicking the hive partition layout:
        # provider=alpaca/channel=trade/date=2026-06-21/bucket=42/part-0.parquet
        data_path = Path(tmp_dir) / "provider=alpaca/channel=trade/date=2026-06-21/part-0.parquet"
        data_path.parent.mkdir(parents=True, exist_ok=True)

        # Write trade records to parquet using Polars
        trade_df = pl.DataFrame(
            [
                {
                    "provider": "alpaca",
                    "channel": "trade",
                    "symbol": "AAPL",
                    "symbol_raw": "AAPL",
                    "source_ts": None,
                    "local_ts": 1782060000000000000 + 100_000_000,  # 2026-06-21 + 0.1s
                    "id": "1",
                    "price": 150.0,
                    "size": 10.0,
                },
                {
                    "provider": "alpaca",
                    "channel": "trade",
                    "symbol": "AAPL",
                    "symbol_raw": "AAPL",
                    "source_ts": None,
                    "local_ts": 1782060000000000000 + 500_000_000,  # 2026-06-21 + 0.5s
                    "id": "2",
                    "price": 152.0,
                    "size": 20.0,
                },
            ]
        )
        trade_df.write_parquet(data_path)

        # Load catalog pointing to the temporary directory
        catalog = Catalog(tmp_dir)

        # Resample OHLCV over the catalog
        res = resample_ohlcv(
            catalog,
            "AAPL",
            1782060000000000000,
            1782060000000000000 + 1_000_000_000,
            "1s",
        )
        assert len(res) == 1
        assert res.row(0, named=True)["bar"] == 1782060000000000000
        assert res.row(0, named=True)["open"] == 150.0
        assert res.row(0, named=True)["close"] == 152.0
        assert res.row(0, named=True)["volume"] == 30.0
        assert res.row(0, named=True)["vwap"] == pytest.approx(
            (150.0 * 10.0 + 152.0 * 20.0) / 30.0
        )
        assert res.row(0, named=True)["trade_count"] == 2
