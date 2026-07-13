"""Metrics analytics module (Beta and Realized Volatility)."""

import math
from collections.abc import Sequence

import numpy as np
import polars as pl


def calculate_beta(
    asset_returns: Sequence[float] | np.ndarray | pl.Series,
    market_returns: Sequence[float] | np.ndarray | pl.Series,
) -> float:
    """Calculate the beta of an asset relative to a market index.

    Beta = Cov(R_asset, R_market) / Var(R_market)

    Pairs containing NaN or infinite values in either series are excluded.
    Returns NaN if the variance of market returns is zero or if there are fewer
    than 2 valid observations.
    """
    if isinstance(asset_returns, pl.Series):
        a_arr = asset_returns.to_numpy()
    else:
        a_arr = np.asarray(asset_returns, dtype=float)

    if isinstance(market_returns, pl.Series):
        m_arr = market_returns.to_numpy()
    else:
        m_arr = np.asarray(market_returns, dtype=float)

    if len(a_arr) != len(m_arr):
        raise ValueError("Asset returns and market returns must have the same length.")

    # Mask for finite values in both
    mask = np.isfinite(a_arr) & np.isfinite(m_arr)
    a_valid = a_arr[mask]
    m_valid = m_arr[mask]

    if len(a_valid) < 2:
        return float("nan")

    m_var = float(np.var(m_valid, ddof=1))
    # Near-zero market variance → undefined / unstable beta
    if m_var <= max(1e-18, float(np.finfo(float).eps) * 10.0):
        return float("nan")

    cov_matrix = np.cov(a_valid, m_valid, ddof=1)
    cov = cov_matrix[0, 1]
    return float(cov / m_var)


def calculate_realized_volatility(
    returns: Sequence[float] | np.ndarray | pl.Series,
    annualization_factor: float = 252.0,
    method: str = "standard",
) -> float:
    """Calculate realized volatility of returns.

    If method is "standard":
        Realized Volatility = StdDev(returns) * sqrt(annualization_factor)
    If method is "sum_of_squares":
        Realized Volatility = sqrt(sum(R_t^2) * (annualization_factor / n))

    NaN and infinite values are excluded.
    Returns NaN if there are insufficient valid observations.
    """
    if isinstance(returns, pl.Series):
        arr = returns.to_numpy()
    else:
        arr = np.asarray(returns, dtype=float)

    if not math.isfinite(annualization_factor) or annualization_factor < 0:
        raise ValueError("annualization_factor must be finite and >= 0")

    valid_arr = arr[np.isfinite(arr)]

    if len(valid_arr) == 0:
        return float("nan")

    method_norm = method.lower()
    if method_norm == "standard":
        if len(valid_arr) < 2:
            return float("nan")
        std = np.std(valid_arr, ddof=1)
        return float(std * math.sqrt(annualization_factor))
    elif method_norm == "sum_of_squares":
        sum_sq = np.sum(valid_arr**2)
        n = len(valid_arr)
        return float(math.sqrt(sum_sq * (annualization_factor / n)))
    else:
        raise ValueError(f"Unknown method: {method}. Must be 'standard' or 'sum_of_squares'.")
