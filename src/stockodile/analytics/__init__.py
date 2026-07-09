"""Analytics module for Stockodile.

Includes functions for returns, risk metrics, option pricing, and fundamental ratios.
"""

from stockodile.analytics.indicators import (
    calculate_bollinger_bands,
    calculate_ema,
    calculate_macd,
    calculate_rsi,
    calculate_sma,
)
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
from stockodile.analytics.slippage import estimate_slippage

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
    "calculate_bollinger_bands",
    "calculate_ema",
    "calculate_gross_margin",
    "calculate_log_returns",
    "calculate_macd",
    "calculate_net_margin",
    "calculate_operating_margin",
    "calculate_pb_ratio",
    "calculate_pe_ratio",
    "calculate_realized_volatility",
    "calculate_roe",
    "calculate_rsi",
    "calculate_simple_returns",
    "calculate_sma",
    "estimate_slippage",
]
