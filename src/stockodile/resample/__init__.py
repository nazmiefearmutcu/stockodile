"""Resampling algorithms for Stockodile.

Supports converting trades, quotes, and lower-resolution bars to higher-resolution
bars (both stream-based and Polars DataFrame-based), as well as generating
order book snapshots from L2 streams.
"""

from stockodile.resample._interval import parse_interval
from stockodile.resample.book import resample_book_snapshots
from stockodile.resample.ohlcv import (
    resample_bars_df,
    resample_bars_to_bars,
    resample_ohlcv,
    resample_quotes_df,
    resample_quotes_to_bars,
    resample_trades_df,
    resample_trades_to_bars,
)

__all__ = [
    "parse_interval",
    "resample_bars_df",
    "resample_bars_to_bars",
    "resample_book_snapshots",
    "resample_ohlcv",
    "resample_quotes_df",
    "resample_quotes_to_bars",
    "resample_trades_df",
    "resample_trades_to_bars",
]
