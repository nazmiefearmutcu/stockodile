"""Options analytics module (Black-Scholes-Merton pricing, Greeks, and Implied Volatility)."""

import math

from stockodile.schema.enums import OptType


def _n_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _n_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bsm_price(
    s: float,
    k: float,
    t: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptType = "call",
) -> float:
    """Calculate the Black-Scholes-Merton price for a European option with continuous
    dividend yield.

    s: Stock price
    k: Strike price
    t: Time to maturity (in years)
    r: Risk-free interest rate (annualized, decimal)
    sigma: Volatility (annualized, decimal)
    q: Continuous dividend yield (annualized, decimal)
    option_type: "call"/"c" or "put"/"p" (case-insensitive, or OptType enum)
    """
    opt_type_str = option_type.value if isinstance(option_type, OptType) else str(option_type)
    opt_type_lower = opt_type_str.lower()

    if opt_type_lower not in ("call", "c", "put", "p"):
        raise ValueError("option_type must be 'call'/'c' or 'put'/'p'")

    if s < 0.0:
        raise ValueError("Stock price (s) must be non-negative")
    if k < 0.0:
        raise ValueError("Strike price (k) must be non-negative")
    if t < 0.0:
        raise ValueError("Time to maturity (t) must be non-negative")

    if t == 0.0:
        if opt_type_lower in ("call", "c"):
            return max(0.0, s - k)
        else:
            return max(0.0, k - s)

    if s == 0.0:
        return 0.0

    if k == 0.0:
        if opt_type_lower in ("call", "c"):
            return s * math.exp(-q * t)
        else:
            return 0.0

    if sigma <= 0.0:
        call_val = s * math.exp(-q * t) - k * math.exp(-r * t)
        if opt_type_lower in ("call", "c"):
            return max(0.0, call_val)
        else:
            return max(0.0, -call_val)

    sqrt_t = math.sqrt(t)
    d1 = (math.log(s / k) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t

    if opt_type_lower in ("call", "c"):
        return s * math.exp(-q * t) * _n_cdf(d1) - k * math.exp(-r * t) * _n_cdf(d2)
    else:
        return k * math.exp(-r * t) * _n_cdf(-d2) - s * math.exp(-q * t) * _n_cdf(-d1)


def bsm_greeks(
    s: float,
    k: float,
    t: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptType = "call",
) -> dict[str, float]:
    """Calculate the Black-Scholes-Merton Greeks for a European option.

    s: Stock price
    k: Strike price
    t: Time to maturity (in years)
    r: Risk-free interest rate (annualized, decimal)
    sigma: Volatility (annualized, decimal)
    q: Continuous dividend yield (annualized, decimal)
    option_type: "call"/"c" or "put"/"p" (case-insensitive, or OptType enum)

    Returns a dictionary with keys: 'delta', 'gamma', 'theta', 'vega', 'rho'.
    Note: theta is annualized (rate of change per year).
    """
    opt_type_str = option_type.value if isinstance(option_type, OptType) else str(option_type)
    opt_type_lower = opt_type_str.lower()

    if opt_type_lower not in ("call", "c", "put", "p"):
        raise ValueError("option_type must be 'call'/'c' or 'put'/'p'")

    if s < 0.0:
        raise ValueError("Stock price (s) must be non-negative")
    if k < 0.0:
        raise ValueError("Strike price (k) must be non-negative")
    if t < 0.0:
        raise ValueError("Time to maturity (t) must be non-negative")

    if t == 0.0 or s == 0.0 or k == 0.0 or sigma <= 0.0:
        is_call = opt_type_lower in ("call", "c")
        if k == 0.0 and s > 0.0 and t > 0.0:
            return {
                "delta": math.exp(-q * t) if is_call else 0.0,
                "gamma": 0.0,
                "theta": -q * s * math.exp(-q * t) if is_call else 0.0,
                "vega": 0.0,
                "rho": 0.0,
            }
        return {
            "delta": 1.0 if (is_call and s > k) else (-1.0 if (not is_call and s < k) else 0.0),
            "gamma": 0.0,
            "theta": 0.0,
            "vega": 0.0,
            "rho": 0.0,
        }

    sqrt_t = math.sqrt(t)
    d1 = (math.log(s / k) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t

    n_d1 = _n_pdf(d1)
    n_cdf_d1 = _n_cdf(d1)
    n_cdf_d2 = _n_cdf(d2)
    n_cdf_minus_d1 = _n_cdf(-d1)
    n_cdf_minus_d2 = _n_cdf(-d2)

    exp_qt = math.exp(-q * t)
    exp_rt = math.exp(-r * t)

    if opt_type_lower in ("call", "c"):
        delta = exp_qt * n_cdf_d1
    else:
        delta = -exp_qt * n_cdf_minus_d1

    gamma = (exp_qt * n_d1) / (s * sigma * sqrt_t)
    vega = s * exp_qt * sqrt_t * n_d1

    term1 = -(s * exp_qt * n_d1 * sigma) / (2.0 * sqrt_t)
    if opt_type_lower in ("call", "c"):
        theta = term1 + q * s * exp_qt * n_cdf_d1 - r * k * exp_rt * n_cdf_d2
    else:
        theta = term1 - q * s * exp_qt * n_cdf_minus_d1 + r * k * exp_rt * n_cdf_minus_d2

    if opt_type_lower in ("call", "c"):
        rho = k * t * exp_rt * n_cdf_d2
    else:
        rho = -k * t * exp_rt * n_cdf_minus_d2

    return {
        "delta": float(delta),
        "gamma": float(gamma),
        "theta": float(theta),
        "vega": float(vega),
        "rho": float(rho),
    }


def bsm_delta(
    s: float,
    k: float,
    t: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptType = "call",
) -> float:
    """Calculate the Black-Scholes-Merton Delta."""
    return bsm_greeks(s, k, t, r, sigma, q, option_type)["delta"]


def bsm_gamma(
    s: float,
    k: float,
    t: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptType = "call",
) -> float:
    """Calculate the Black-Scholes-Merton Gamma."""
    return bsm_greeks(s, k, t, r, sigma, q, option_type)["gamma"]


def bsm_theta(
    s: float,
    k: float,
    t: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptType = "call",
) -> float:
    """Calculate the Black-Scholes-Merton Theta (annualized)."""
    return bsm_greeks(s, k, t, r, sigma, q, option_type)["theta"]


def bsm_vega(
    s: float,
    k: float,
    t: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptType = "call",
) -> float:
    """Calculate the Black-Scholes-Merton Vega."""
    return bsm_greeks(s, k, t, r, sigma, q, option_type)["vega"]


def bsm_rho(
    s: float,
    k: float,
    t: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptType = "call",
) -> float:
    """Calculate the Black-Scholes-Merton Rho."""
    return bsm_greeks(s, k, t, r, sigma, q, option_type)["rho"]


def bsm_implied_volatility(
    price: float,
    s: float,
    k: float,
    t: float,
    r: float,
    q: float = 0.0,
    option_type: str | OptType = "call",
    max_iterations: int = 100,
    tolerance: float = 1e-6,
) -> float:
    """Calculate implied volatility for a European option using BSM.

    Uses Newton-Raphson with bisection fallback for extreme robustness.
    Returns NaN if IV cannot be found or if price is outside arbitrage bounds.
    """
    opt_type_str = option_type.value if isinstance(option_type, OptType) else str(option_type)
    opt_type_lower = opt_type_str.lower()

    if opt_type_lower not in ("call", "c", "put", "p"):
        raise ValueError("option_type must be 'call'/'c' or 'put'/'p'")

    if t <= 0.0 or s <= 0.0 or k <= 0.0 or price <= 0.0:
        return float("nan")

    exp_qt = math.exp(-q * t)
    exp_rt = math.exp(-r * t)

    if opt_type_lower in ("call", "c"):
        min_price = max(0.0, s * exp_qt - k * exp_rt)
        max_price = s * exp_qt
    else:
        min_price = max(0.0, k * exp_rt - s * exp_qt)
        max_price = k * exp_rt

    if price < min_price - 1e-9 or price > max_price + 1e-9:
        return float("nan")

    price = max(min_price, min(price, max_price))

    low_sigma = 1e-6
    high_sigma = 10.0

    p_low = bsm_price(s, k, t, r, low_sigma, q, opt_type_lower)
    p_high = bsm_price(s, k, t, r, high_sigma, q, opt_type_lower)

    if price <= p_low:
        return low_sigma
    if price >= p_high:
        high_sigma = 50.0
        p_high = bsm_price(s, k, t, r, high_sigma, q, opt_type_lower)
        if price >= p_high:
            return float("nan")

    sigma = 0.4

    for _ in range(max_iterations):
        current_price = bsm_price(s, k, t, r, sigma, q, opt_type_lower)
        diff = current_price - price

        if abs(diff) < tolerance:
            return sigma

        if diff > 0:
            high_sigma = sigma
        else:
            low_sigma = sigma

        greeks = bsm_greeks(s, k, t, r, sigma, q, opt_type_lower)
        vega = greeks["vega"]

        if vega > 1e-4:
            new_sigma = sigma - diff / vega
            if low_sigma < new_sigma < high_sigma:
                sigma = new_sigma
                continue

        sigma = 0.5 * (low_sigma + high_sigma)

    final_price = bsm_price(s, k, t, r, sigma, q, opt_type_lower)
    if abs(final_price - price) < tolerance:
        return sigma
    return float("nan")
