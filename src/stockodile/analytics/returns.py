"""Returns analytics module."""

import math
from collections.abc import Sequence
from typing import overload

import numpy as np
import polars as pl


@overload
def calculate_simple_returns(prices: pl.Series) -> pl.Series: ...


@overload
def calculate_simple_returns(prices: np.ndarray) -> np.ndarray: ...


@overload
def calculate_simple_returns(prices: Sequence[float | None]) -> list[float]: ...


def calculate_simple_returns(
    prices: pl.Series | np.ndarray | Sequence[float | None],
) -> pl.Series | np.ndarray | list[float]:
    """Calculate simple returns from a sequence of prices.

    R_t = (P_t - P_{t-1}) / P_{t-1}

    The first element of the returned sequence will be NaN.
    """
    if isinstance(prices, pl.Series):
        shifted = prices.shift(1)
        return (prices - shifted) / shifted
    elif isinstance(prices, np.ndarray):
        with np.errstate(divide="ignore", invalid="ignore"):
            returns = np.empty_like(prices, dtype=float)
            returns[0] = np.nan
            if len(prices) > 1:
                returns[1:] = (prices[1:] - prices[:-1]) / prices[:-1]
            return returns
    else:
        n = len(prices)
        if n == 0:
            return []
        if n == 1:
            return [float("nan")]
        res = [float("nan")]
        for i in range(1, n):
            prev = prices[i - 1]
            curr = prices[i]
            if prev is None or curr is None or prev == 0.0 or math.isnan(prev) or math.isnan(curr):
                res.append(float("nan"))
            else:
                res.append((curr - prev) / prev)
        return res


@overload
def calculate_log_returns(prices: pl.Series) -> pl.Series: ...


@overload
def calculate_log_returns(prices: np.ndarray) -> np.ndarray: ...


@overload
def calculate_log_returns(prices: Sequence[float | None]) -> list[float]: ...


def calculate_log_returns(
    prices: pl.Series | np.ndarray | Sequence[float | None],
) -> pl.Series | np.ndarray | list[float]:
    """Calculate log returns from a sequence of prices.

    R_t = ln(P_t / P_{t-1})

    The first element of the returned sequence will be NaN.
    """
    if isinstance(prices, pl.Series):
        return (prices / prices.shift(1)).log()
    elif isinstance(prices, np.ndarray):
        with np.errstate(divide="ignore", invalid="ignore"):
            returns = np.empty_like(prices, dtype=float)
            returns[0] = np.nan
            if len(prices) > 1:
                returns[1:] = np.log(prices[1:] / prices[:-1])
            return returns
    else:
        n = len(prices)
        if n == 0:
            return []
        if n == 1:
            return [float("nan")]
        res = [float("nan")]
        for i in range(1, n):
            prev = prices[i - 1]
            curr = prices[i]
            if (
                prev is None
                or curr is None
                or prev <= 0.0
                or curr <= 0.0
                or math.isnan(prev)
                or math.isnan(curr)
            ):
                res.append(float("nan"))
            else:
                res.append(math.log(curr / prev))
        return res
