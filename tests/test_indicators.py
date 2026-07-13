"""Tests for stockodile technical analysis indicators."""

import numpy as np
import polars as pl
import pytest

from stockodile.analytics.indicators import (
    calculate_bollinger_bands,
    calculate_ema,
    calculate_macd,
    calculate_rsi,
    calculate_sma,
)


def test_sma() -> None:
    prices = [10.0, 20.0, 30.0, 40.0, 50.0]

    # 1. Test List Input
    res_list = calculate_sma(prices, period=3)
    assert len(res_list) == 5
    assert res_list[0] is None
    assert res_list[1] is None
    assert pytest.approx(res_list[2]) == 20.0  # (10+20+30)/3
    assert pytest.approx(res_list[3]) == 30.0  # (20+30+40)/3
    assert pytest.approx(res_list[4]) == 40.0  # (30+40+50)/3

    # 2. Test NumPy Input
    res_np = calculate_sma(np.array(prices), period=3)
    assert isinstance(res_np, np.ndarray)
    assert np.isnan(res_np[0])
    assert np.isnan(res_np[1])
    assert pytest.approx(res_np[2]) == 20.0

    # 3. Test Polars Input
    res_pl = calculate_sma(pl.Series(prices), period=3)
    assert isinstance(res_pl, pl.Series)
    assert res_pl[0] is None
    assert res_pl[1] is None
    assert pytest.approx(res_pl[2]) == 20.0

    # 4. Error case
    with pytest.raises(ValueError):
        calculate_sma(prices, period=0)


def test_ema() -> None:
    prices = [1.0, 2.0, 3.0]

    # EWM mean with span=2, adjust=False:
    # alpha = 2 / (2 + 1) = 2/3
    # y0 = 1.0
    # y1 = (1 - 2/3)*1.0 + (2/3)*2.0 = 1/3 + 4/3 = 1.666667
    # y2 = (1 - 2/3)*1.666667 + (2/3)*3.0 = 5/9 + 2 = 2.555556

    # 1. Test List Input
    res_list = calculate_ema(prices, period=2)
    assert len(res_list) == 3
    assert pytest.approx(res_list[0]) == 1.0
    assert pytest.approx(res_list[1]) == 1.666667
    assert pytest.approx(res_list[2]) == 2.555556

    # 2. Test NumPy Input
    res_np = calculate_ema(np.array(prices), period=2)
    assert isinstance(res_np, np.ndarray)
    assert pytest.approx(res_np[1]) == 1.666667

    # 3. Test Polars Input
    res_pl = calculate_ema(pl.Series(prices), period=2)
    assert isinstance(res_pl, pl.Series)
    assert pytest.approx(res_pl[2]) == 2.555556

    # 4. Error case
    with pytest.raises(ValueError):
        calculate_ema(prices, period=-1)


def test_rsi() -> None:
    # Setup price movement: prices increasing steadily
    prices = [10.0, 11.0, 12.0, 13.0, 14.0]

    # Wilder warm-up: first valid RSI at index == period (needs period+1 prices)
    res_list = calculate_rsi(prices, period=3)
    assert len(res_list) == 5
    assert res_list[0] is None
    assert res_list[1] is None
    assert res_list[2] is None
    # All gains, zero losses → RSI 100 after seed
    assert pytest.approx(res_list[3]) == 100.0
    assert pytest.approx(res_list[4]) == 100.0

    # Steadily decreasing prices. RSI should tend to 0.
    dec_prices = [50.0, 40.0, 30.0, 20.0, 10.0]
    res_dec = calculate_rsi(dec_prices, period=3)
    assert len(res_dec) == 5
    assert res_dec[0] is None
    assert res_dec[1] is None
    assert res_dec[2] is None
    assert pytest.approx(res_dec[3]) == 0.0
    assert pytest.approx(res_dec[4]) == 0.0

    # No price movement. RSI should be 50.0 since both average gain and loss are 0.0
    flat_prices = [10.0, 10.0, 10.0, 10.0, 10.0]
    res_flat = calculate_rsi(flat_prices, period=3)
    assert len(res_flat) == 5
    assert res_flat[0] is None
    assert res_flat[1] is None
    assert res_flat[2] is None
    assert pytest.approx(res_flat[3]) == 50.0

    # Error case
    with pytest.raises(ValueError):
        calculate_rsi(prices, period=0)


def test_macd() -> None:
    prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]

    macd_line, signal_line, hist = calculate_macd(
        prices, fast_period=3, slow_period=6, signal_period=3
    )

    assert len(macd_line) == 10
    assert len(signal_line) == 10
    assert len(hist) == 10

    # Assert type preservation
    assert isinstance(macd_line, list)

    macd_np, _, _ = calculate_macd(np.array(prices), fast_period=3, slow_period=6, signal_period=3)
    assert isinstance(macd_np, np.ndarray)

    # Error case
    with pytest.raises(ValueError):
        calculate_macd(prices, fast_period=0)


def test_bollinger_bands() -> None:
    prices = [10.0, 12.0, 11.0, 13.0, 12.0, 14.0]

    upper, mid, lower = calculate_bollinger_bands(prices, period=3, k=2.0)

    assert len(mid) == 6
    assert len(upper) == 6
    assert len(lower) == 6

    # Assert types
    assert isinstance(mid, list)

    # Polars Series validation
    upper_pl, mid_pl, lower_pl = calculate_bollinger_bands(pl.Series(prices), period=3, k=2.0)
    assert isinstance(mid_pl, pl.Series)

    # Check that upper > mid > lower
    for i in range(2, 6):
        assert upper_pl[i] > mid_pl[i]
        assert mid_pl[i] > lower_pl[i]

    # Error case
    with pytest.raises(ValueError):
        calculate_bollinger_bands(prices, period=-5)
