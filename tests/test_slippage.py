"""Tests for the execution slippage estimator in Stockodile."""

from __future__ import annotations

import pathlib

import pytest

from stockodile.analytics.slippage import estimate_slippage
from stockodile.store.catalog import Catalog
from tests.store.test_catalog import write_parquet_fixture


def test_estimate_slippage_basic(tmp_path: pathlib.Path) -> None:
    # Write a mock book snapshot fixture to the temporary directory
    # Bids: 5 shares at 100.0, 10 shares at 99.0
    # Asks: 4 shares at 101.0, 8 shares at 102.0
    bids = [(100.0, 5.0), (99.0, 10.0)]
    asks = [(101.0, 4.0), (102.0, 8.0)]
    snap = {
        "provider": "alpaca",
        "symbol": "alpaca:AAPL",
        "symbol_raw": "AAPL",
        "source_ts": 1700000000000000000,
        "local_ts": 1700000000000000000,
        "bids": bids,
        "asks": asks,
        "depth": 2,
        "sequence_id": 42,
        "is_snapshot": True,
    }
    write_parquet_fixture(tmp_path, "book_snapshot", "alpaca", "alpaca:AAPL", [snap])

    catalog = Catalog(tmp_path)

    # 1. Test BUY side: requested size <= best ask size
    # size = 2.0, best ask = 101.0 (amount = 4.0)
    # expected price should be exactly 101.0, slippage should be 0.0
    df_buy = estimate_slippage(catalog, "alpaca:AAPL", "buy", 2.0)
    assert len(df_buy) == 1
    assert df_buy["symbol"][0] == "alpaca:AAPL"
    assert df_buy["side"][0] == "buy"
    assert df_buy["size"][0] == 2.0
    assert df_buy["best_price"][0] == 101.0
    assert df_buy["expected_price"][0] == 101.0
    assert df_buy["slippage_usd"][0] == 0.0
    assert df_buy["slippage_pct"][0] == 0.0

    # 2. Test BUY side: requested size > best ask size
    # size = 6.0
    # 4.0 shares filled at 101.0, 2.0 shares filled at 102.0
    # total cost = 4.0 * 101.0 + 2.0 * 102.0 = 404.0 + 204.0 = 608.0
    # expected price = 608.0 / 6.0 = 101.333333
    # slippage USD = 101.333333 - 101.0 = 0.333333
    df_buy_more = estimate_slippage(catalog, "alpaca:AAPL", "buy", 6.0)
    assert pytest.approx(df_buy_more["expected_price"][0]) == 608.0 / 6.0
    assert pytest.approx(df_buy_more["slippage_usd"][0]) == (608.0 / 6.0) - 101.0

    # 3. Test SELL side: requested size <= best bid size
    # size = 2.0, best bid = 100.0 (amount = 5.0)
    df_sell = estimate_slippage(catalog, "alpaca:AAPL", "sell", 2.0)
    assert df_sell["side"][0] == "sell"
    assert df_sell["best_price"][0] == 100.0
    assert df_sell["expected_price"][0] == 100.0
    assert df_sell["slippage_usd"][0] == 0.0

    # 4. Test SELL side: requested size > best bid size
    # size = 10.0
    # 5.0 shares filled at 100.0, 5.0 shares filled at 99.0
    # total cost = 5.0 * 100.0 + 5.0 * 99.0 = 500.0 + 495.0 = 995.0
    # expected price = 995.0 / 10.0 = 99.5
    # slippage USD = 100.0 - 99.5 = 0.5
    df_sell_more = estimate_slippage(catalog, "alpaca:AAPL", "sell", 10.0)
    assert pytest.approx(df_sell_more["expected_price"][0]) == 99.5
    assert pytest.approx(df_sell_more["slippage_usd"][0]) == 0.5

    # 5. Test inputs validation and boundaries
    with pytest.raises(ValueError, match="Size must be greater than zero"):
        estimate_slippage(catalog, "alpaca:AAPL", "buy", 0.0)

    with pytest.raises(ValueError, match="Size must be greater than zero"):
        estimate_slippage(catalog, "alpaca:AAPL", "buy", -5.0)

    with pytest.raises(ValueError, match="Invalid side"):
        estimate_slippage(catalog, "alpaca:AAPL", "invalid_side", 2.0)

    with pytest.raises(ValueError, match="exceeds total order book depth"):
        estimate_slippage(catalog, "alpaca:AAPL", "buy", 15.0)

    with pytest.raises(ValueError, match="No book snapshots found"):
        estimate_slippage(catalog, "alpaca:MSFT", "buy", 2.0)
