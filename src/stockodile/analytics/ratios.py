"""Financial ratios module (P/E, P/B, ROE, and Margins)."""

import math


def calculate_pe_ratio(price_per_share: float | None, eps: float | None) -> float:
    """Calculate the Price-to-Earnings (P/E) ratio.

    PE = Price / EPS
    Returns NaN if EPS is zero or NaN, or if price is NaN.
    """
    if (
        price_per_share is None
        or eps is None
        or eps == 0.0
        or math.isnan(eps)
        or math.isnan(price_per_share)
    ):
        return float("nan")
    return price_per_share / eps


def calculate_pb_ratio(price_per_share: float | None, book_value_per_share: float | None) -> float:
    """Calculate the Price-to-Book (P/B) ratio.

    PB = Price / Book Value Per Share
    Returns NaN if Book Value per share is zero or NaN, or if price is NaN.
    """
    if (
        price_per_share is None
        or book_value_per_share is None
        or book_value_per_share == 0.0
        or math.isnan(book_value_per_share)
        or math.isnan(price_per_share)
    ):
        return float("nan")
    return price_per_share / book_value_per_share


def calculate_roe(net_income: float | None, book_value: float | None) -> float:
    """Calculate Return on Equity (ROE).

    ROE = Net Income / Book Value
    Returns NaN if Book Value is zero or NaN, or if net income is NaN.
    """
    if (
        net_income is None
        or book_value is None
        or book_value == 0.0
        or math.isnan(book_value)
        or math.isnan(net_income)
    ):
        return float("nan")
    return net_income / book_value


def calculate_gross_margin(gross_profit: float | None, revenue: float | None) -> float:
    """Calculate Gross Profit Margin.

    Gross Margin = Gross Profit / Revenue
    Returns NaN if Revenue is zero or NaN, or if gross profit is NaN.
    """
    if (
        gross_profit is None
        or revenue is None
        or revenue == 0.0
        or math.isnan(revenue)
        or math.isnan(gross_profit)
    ):
        return float("nan")
    return gross_profit / revenue


def calculate_operating_margin(operating_income: float | None, revenue: float | None) -> float:
    """Calculate Operating Profit Margin.

    Operating Margin = Operating Income / Revenue
    Returns NaN if Revenue is zero or NaN, or if operating income is NaN.
    """
    if (
        operating_income is None
        or revenue is None
        or revenue == 0.0
        or math.isnan(revenue)
        or math.isnan(operating_income)
    ):
        return float("nan")
    return operating_income / revenue


def calculate_net_margin(net_income: float | None, revenue: float | None) -> float:
    """Calculate Net Profit Margin.

    Net Margin = Net Income / Revenue
    Returns NaN if Revenue is zero or NaN, or if net income is NaN.
    """
    if (
        net_income is None
        or revenue is None
        or revenue == 0.0
        or math.isnan(revenue)
        or math.isnan(net_income)
    ):
        return float("nan")
    return net_income / revenue
