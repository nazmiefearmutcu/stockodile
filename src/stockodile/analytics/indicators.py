"""Technical Analysis Indicators Engine using Polars."""

from collections.abc import Sequence
from typing import Any, overload

import numpy as np
import polars as pl


def _to_series(prices: pl.Series | np.ndarray | Sequence[float | None]) -> pl.Series:
    """Helper to convert input type to a Polars Float64 Series."""
    if isinstance(prices, pl.Series):
        return prices.cast(pl.Float64)
    elif isinstance(prices, np.ndarray):
        return pl.Series(values=prices, dtype=pl.Float64)
    else:
        return pl.Series(values=list(prices), dtype=pl.Float64)


def _from_series(
    result: pl.Series,
    original: pl.Series | np.ndarray | Sequence[float | None],
) -> pl.Series | np.ndarray | list[float | None]:
    """Helper to convert a Polars Series back to the original input type."""
    if isinstance(original, pl.Series):
        return result
    elif isinstance(original, np.ndarray):
        return result.to_numpy()
    else:
        return result.to_list()


@overload
def calculate_sma(prices: pl.Series, period: int) -> pl.Series: ...


@overload
def calculate_sma(prices: np.ndarray, period: int) -> np.ndarray: ...


@overload
def calculate_sma(prices: Sequence[float | None], period: int) -> list[float | None]: ...


def calculate_sma(
    prices: pl.Series | np.ndarray | Sequence[float | None],
    period: int,
) -> pl.Series | np.ndarray | list[float | None]:
    """Calculate Simple Moving Average (SMA) over a given period."""
    if period <= 0:
        raise ValueError("Period must be a positive integer.")
    series = _to_series(prices)
    if len(series) == 0:
        return _from_series(series, prices)

    res = series.rolling_mean(window_size=period)
    return _from_series(res, prices)


@overload
def calculate_ema(prices: pl.Series, period: int) -> pl.Series: ...


@overload
def calculate_ema(prices: np.ndarray, period: int) -> np.ndarray: ...


@overload
def calculate_ema(prices: Sequence[float | None], period: int) -> list[float | None]: ...


def calculate_ema(
    prices: pl.Series | np.ndarray | Sequence[float | None],
    period: int,
) -> pl.Series | np.ndarray | list[float | None]:
    """Calculate Exponential Moving Average (EMA) over a given period."""
    if period <= 0:
        raise ValueError("Period must be a positive integer.")
    series = _to_series(prices)
    if len(series) == 0:
        return _from_series(series, prices)

    res = series.ewm_mean(span=period, adjust=False)
    return _from_series(res, prices)


@overload
def calculate_rsi(prices: pl.Series, period: int) -> pl.Series: ...


@overload
def calculate_rsi(prices: np.ndarray, period: int) -> np.ndarray: ...


@overload
def calculate_rsi(prices: Sequence[float | None], period: int) -> list[float | None]: ...


def calculate_rsi(
    prices: pl.Series | np.ndarray | Sequence[float | None],
    period: int,
) -> pl.Series | np.ndarray | list[float | None]:
    """Calculate Relative Strength Index (RSI) using Wilder's smoothing.

    Warm-up: first *period* changes seed SMA averages; first valid RSI is at
    index ``period`` (needs ``period + 1`` prices). Matches classic Wilder/TA-Lib.
    """
    if period <= 0:
        raise ValueError("Period must be a positive integer.")
    series = _to_series(prices)
    n = len(series)
    if n == 0:
        return _from_series(series, prices)

    change = series.diff()
    gain = change.clip(lower_bound=0.0)
    loss = (-change).clip(lower_bound=0.0)

    # Seed with SMA of first `period` gains/losses (indices 1..period), then Wilder
    avg_gain_vals: list[float | None] = [None] * n
    avg_loss_vals: list[float | None] = [None] * n
    gain_list = gain.to_list()
    loss_list = loss.to_list()

    if n > period:
        seed_gains = [float(gain_list[i] or 0.0) for i in range(1, period + 1)]
        seed_losses = [float(loss_list[i] or 0.0) for i in range(1, period + 1)]
        avg_g = sum(seed_gains) / period
        avg_l = sum(seed_losses) / period
        avg_gain_vals[period] = avg_g
        avg_loss_vals[period] = avg_l
        for i in range(period + 1, n):
            g = float(gain_list[i] or 0.0)
            l = float(loss_list[i] or 0.0)
            avg_g = (avg_g * (period - 1) + g) / period
            avg_l = (avg_l * (period - 1) + l) / period
            avg_gain_vals[i] = avg_g
            avg_loss_vals[i] = avg_l

    rsi_vals: list[float | None] = []
    for i in range(n):
        ag = avg_gain_vals[i]
        al = avg_loss_vals[i]
        if ag is None or al is None:
            rsi_vals.append(None)
        elif al == 0.0 and ag == 0.0:
            rsi_vals.append(50.0)
        elif al == 0.0:
            rsi_vals.append(100.0)
        else:
            rs = ag / al
            rsi_vals.append(100.0 - (100.0 / (1.0 + rs)))

    rsi = pl.Series(rsi_vals, dtype=pl.Float64)
    return _from_series(rsi, prices)


@overload
def calculate_macd(
    prices: pl.Series,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[pl.Series, pl.Series, pl.Series]: ...


@overload
def calculate_macd(
    prices: np.ndarray,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]: ...


@overload
def calculate_macd(
    prices: Sequence[float | None],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[list[float | None], list[float | None], list[float | None]]: ...


def calculate_macd(
    prices: pl.Series | np.ndarray | Sequence[float | None],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[Any, Any, Any]:
    """Calculate Moving Average Convergence Divergence (MACD)."""
    if fast_period <= 0 or slow_period <= 0 or signal_period <= 0:
        raise ValueError("Periods must be positive integers.")
    series = _to_series(prices)
    if len(series) == 0:
        empty = _from_series(series, prices)
        return empty, empty, empty

    fast_ema = series.ewm_mean(span=fast_period, adjust=False)
    slow_ema = series.ewm_mean(span=slow_period, adjust=False)
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm_mean(span=signal_period, adjust=False)
    macd_hist = macd_line - signal_line

    return (
        _from_series(macd_line, prices),
        _from_series(signal_line, prices),
        _from_series(macd_hist, prices),
    )


@overload
def calculate_bollinger_bands(
    prices: pl.Series,
    period: int = 20,
    k: float = 2.0,
) -> tuple[pl.Series, pl.Series, pl.Series]: ...


@overload
def calculate_bollinger_bands(
    prices: np.ndarray,
    period: int = 20,
    k: float = 2.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]: ...


@overload
def calculate_bollinger_bands(
    prices: Sequence[float | None],
    period: int = 20,
    k: float = 2.0,
) -> tuple[list[float | None], list[float | None], list[float | None]]: ...


def calculate_bollinger_bands(
    prices: pl.Series | np.ndarray | Sequence[float | None],
    period: int = 20,
    k: float = 2.0,
) -> tuple[Any, Any, Any]:
    """Calculate Bollinger Bands (Upper, Middle, Lower)."""
    if period <= 0:
        raise ValueError("Period must be a positive integer.")
    series = _to_series(prices)
    if len(series) == 0:
        empty = _from_series(series, prices)
        return empty, empty, empty

    middle = series.rolling_mean(window_size=period)
    # Canonical Bollinger Bands use population std (ddof=0), not sample (ddof=1)
    std = series.rolling_std(window_size=period, ddof=0)
    upper = middle + k * std
    lower = middle - k * std

    return (
        _from_series(upper, prices),
        _from_series(middle, prices),
        _from_series(lower, prices),
    )
