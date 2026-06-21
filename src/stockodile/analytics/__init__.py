"""Analytics module for Stockodile.

Includes functions for returns, risk metrics, option pricing, and fundamental ratios.
"""

from stockodile.analytics.metrics import (
    calculate_beta,
    calculate_realized_volatility,
)
from stockodile.analytics.options import (
    bsm_delta,
    bsm_gamma,
    bsm_greeks,
    bsm_implied_volatility,
    bsm_price,
    bsm_rho,
    bsm_theta,
    bsm_vega,
)
from stockodile.analytics.ratios import (
    calculate_gross_margin,
    calculate_net_margin,
    calculate_operating_margin,
    calculate_pb_ratio,
    calculate_pe_ratio,
    calculate_roe,
)
from stockodile.analytics.returns import (
    calculate_log_returns,
    calculate_simple_returns,
)

__all__ = [
    "bsm_delta",
    "bsm_gamma",
    "bsm_greeks",
    "bsm_implied_volatility",
    "bsm_price",
    "bsm_rho",
    "bsm_theta",
    "bsm_vega",
    "calculate_beta",
    "calculate_gross_margin",
    "calculate_log_returns",
    "calculate_net_margin",
    "calculate_operating_margin",
    "calculate_pb_ratio",
    "calculate_pe_ratio",
    "calculate_realized_volatility",
    "calculate_roe",
    "calculate_simple_returns",
]
