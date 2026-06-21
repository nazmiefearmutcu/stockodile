"""Tests for stockodile analytics modules."""

import math

import numpy as np
import polars as pl
import pytest

from stockodile.analytics import (
    bsm_delta,
    bsm_gamma,
    bsm_greeks,
    bsm_implied_volatility,
    bsm_price,
    bsm_rho,
    bsm_theta,
    bsm_vega,
    calculate_beta,
    calculate_gross_margin,
    calculate_log_returns,
    calculate_net_margin,
    calculate_operating_margin,
    calculate_pb_ratio,
    calculate_pe_ratio,
    calculate_realized_volatility,
    calculate_roe,
    calculate_simple_returns,
)
from stockodile.schema.enums import OptType


def test_returns_simple() -> None:
    prices = [10.0, 11.0, 9.9, 10.89]

    # List input
    res_list = calculate_simple_returns(prices)
    assert len(res_list) == 4
    assert math.isnan(res_list[0])
    assert pytest.approx(res_list[1]) == 0.1
    assert pytest.approx(res_list[2]) == -0.1
    assert pytest.approx(res_list[3]) == 0.1

    # NumPy input
    res_np = calculate_simple_returns(np.array(prices))
    assert isinstance(res_np, np.ndarray)
    assert np.isnan(res_np[0])
    assert pytest.approx(res_np[1]) == 0.1
    assert pytest.approx(res_np[2]) == -0.1
    assert pytest.approx(res_np[3]) == 0.1

    # Polars input
    res_pl = calculate_simple_returns(pl.Series(prices))
    assert isinstance(res_pl, pl.Series)
    assert res_pl[0] is None
    assert pytest.approx(res_pl[1]) == 0.1
    assert pytest.approx(res_pl[2]) == -0.1
    assert pytest.approx(res_pl[3]) == 0.1


def test_returns_log() -> None:
    prices = [10.0, 11.0, 9.9, 10.89]

    # List input
    res_list = calculate_log_returns(prices)
    assert len(res_list) == 4
    assert math.isnan(res_list[0])
    assert pytest.approx(res_list[1]) == math.log(1.1)
    assert pytest.approx(res_list[2]) == math.log(0.9)
    assert pytest.approx(res_list[3]) == math.log(1.1)

    # NumPy input
    res_np = calculate_log_returns(np.array(prices))
    assert isinstance(res_np, np.ndarray)
    assert np.isnan(res_np[0])
    assert pytest.approx(res_np[1]) == np.log(1.1)

    # Polars input
    res_pl = calculate_log_returns(pl.Series(prices))
    assert isinstance(res_pl, pl.Series)
    assert res_pl[0] is None
    assert pytest.approx(res_pl[1]) == math.log(1.1)


def test_beta() -> None:
    asset = [0.01, 0.02, -0.01, 0.03, 0.0]
    market = [0.005, 0.01, -0.005, 0.015, 0.0]
    # Asset returns are exactly twice the market returns
    beta = calculate_beta(asset, market)
    assert pytest.approx(beta) == 2.0

    # With NaNs
    asset_nan = [0.01, 0.02, float("nan"), 0.03, 0.0]
    beta_nan = calculate_beta(asset_nan, market)
    assert pytest.approx(beta_nan) == 2.0


def test_realized_volatility() -> None:
    returns = [0.01, -0.015, 0.02, -0.005, 0.01]
    vol_std = calculate_realized_volatility(returns, annualization_factor=252.0, method="standard")
    expected_std = np.std(returns, ddof=1) * math.sqrt(252.0)
    assert pytest.approx(vol_std) == expected_std

    vol_sq = calculate_realized_volatility(
        returns, annualization_factor=252.0, method="sum_of_squares"
    )
    expected_sq = math.sqrt(sum(x**2 for x in returns) * (252.0 / 5))
    assert pytest.approx(vol_sq) == expected_sq


def test_bsm_pricing_and_greeks() -> None:
    # Test values from standard textbook examples
    s, k, t, r, sigma, q = 100.0, 100.0, 1.0, 0.05, 0.2, 0.02

    # Call price and Greeks
    c_price = bsm_price(s, k, t, r, sigma, q, "call")
    assert c_price > 0.0

    c_greeks = bsm_greeks(s, k, t, r, sigma, q, OptType.C)
    assert pytest.approx(c_greeks["delta"]) == bsm_delta(s, k, t, r, sigma, q, "call")
    assert pytest.approx(c_greeks["gamma"]) == bsm_gamma(s, k, t, r, sigma, q, "call")
    assert pytest.approx(c_greeks["theta"]) == bsm_theta(s, k, t, r, sigma, q, "call")
    assert pytest.approx(c_greeks["vega"]) == bsm_vega(s, k, t, r, sigma, q, "call")
    assert pytest.approx(c_greeks["rho"]) == bsm_rho(s, k, t, r, sigma, q, "call")

    # Put price and Greeks
    p_price = bsm_price(s, k, t, r, sigma, q, "put")
    p_greeks = bsm_greeks(s, k, t, r, sigma, q, OptType.P)
    assert p_price > 0.0
    assert p_greeks["delta"] < 0.0

    # Put-Call Parity: C - P = S * e^(-q T) - K * e^(-r T)
    parity_lhs = c_price - p_price
    parity_rhs = s * math.exp(-q * t) - k * math.exp(-r * t)
    assert pytest.approx(parity_lhs) == parity_rhs


def test_implied_volatility() -> None:
    s, k, t, r, q = 100.0, 100.0, 1.0, 0.05, 0.02
    target_vol = 0.25

    # Compute a price first
    c_price = bsm_price(s, k, t, r, target_vol, q, "call")

    # Back out IV
    iv = bsm_implied_volatility(c_price, s, k, t, r, q, "call")
    assert pytest.approx(iv, abs=1e-5) == target_vol

    # Out of bounds should return NaN
    invalid_iv = bsm_implied_volatility(1000.0, s, k, t, r, q, "call")
    assert math.isnan(invalid_iv)


def test_ratios() -> None:
    assert pytest.approx(calculate_pe_ratio(100.0, 5.0)) == 20.0
    assert math.isnan(calculate_pe_ratio(100.0, 0.0))

    assert pytest.approx(calculate_pb_ratio(100.0, 10.0)) == 10.0
    assert math.isnan(calculate_pb_ratio(100.0, 0.0))

    assert pytest.approx(calculate_roe(10.0, 50.0)) == 0.2
    assert math.isnan(calculate_roe(10.0, 0.0))

    assert pytest.approx(calculate_gross_margin(40.0, 100.0)) == 0.4
    assert math.isnan(calculate_gross_margin(40.0, 0.0))

    assert pytest.approx(calculate_operating_margin(20.0, 100.0)) == 0.2
    assert math.isnan(calculate_operating_margin(20.0, 0.0))

    assert pytest.approx(calculate_net_margin(10.0, 100.0)) == 0.1
    assert math.isnan(calculate_net_margin(10.0, 0.0))
