"""Execution Slippage Estimator (Task R1).

Queries the latest book_snapshot for a symbol and calculates the expected 
execution price (VWAP) and slippage for a given base asset size.
"""

from __future__ import annotations

import polars as pl

from stockodile.store.catalog import Catalog
from stockodile.store.rows import _coerce_levels_from_row

__all__ = ["estimate_slippage"]


def estimate_slippage(
    catalog: Catalog,
    symbol: str,
    side: str,
    size: float,
) -> pl.DataFrame:
    """Calculate the expected execution price and slippage for a given size.

    Args:
        catalog: A :class:`~stockodile.store.catalog.Catalog` instance.
        symbol: Canonical symbol string.
        side: "buy" or "sell".
        size: The base asset size to execute.

    Returns:
        A Polars DataFrame containing:
        - symbol: Canonical symbol
        - side: "buy" or "sell"
        - size: Requested size
        - best_price: Best bid/ask price
        - expected_price: VWAP price
        - slippage_usd: Absolute slippage in USD
        - slippage_pct: Percentage slippage (%)
    """
    if size <= 0:
        raise ValueError("Size must be greater than zero.")

    side_lower = side.lower()
    if side_lower not in ("buy", "sell"):
        raise ValueError(f"Invalid side '{side}'. Must be 'buy' or 'sell'.")

    # Query the latest book snapshot for the symbol
    try:
        catalog.refresh_views()
        df = catalog.connection.execute(
            "SELECT bids, asks FROM book_snapshot WHERE symbol = ? ORDER BY local_ts DESC LIMIT 1",
            [symbol]
        ).pl()
    except ValueError:
        raise
    except Exception as e:
        raise RuntimeError(
            f"Failed to load book snapshots for symbol '{symbol}': {e}"
        ) from e

    if df.is_empty():
        raise ValueError(f"No book snapshots found for symbol '{symbol}'.")

    row = df.to_dicts()[0]
    bids = _coerce_levels_from_row(row.get("bids"))
    asks = _coerce_levels_from_row(row.get("asks"))

    raw_levels = bids if side_lower == "sell" else asks
    # Drop removed/empty levels; enforce price-time priority walk order
    levels = [(p, a) for p, a in raw_levels if a is not None and a > 0 and p is not None and p > 0]
    if not levels:
        raise ValueError(f"Order book for symbol '{symbol}' has no levels on the {side} side.")

    if side_lower == "buy":
        levels = sorted(levels, key=lambda x: x[0])  # asks: best (low) first
    else:
        levels = sorted(levels, key=lambda x: x[0], reverse=True)  # bids: best (high) first

    best_price = levels[0][0]

    filled = 0.0
    total_cost = 0.0
    for price, amount in levels:
        if filled >= size:
            break
        to_fill = min(amount, size - filled)
        total_cost += to_fill * price
        filled += to_fill

    if filled < size:
        raise ValueError(
            f"Requested size {size} exceeds total order book depth ({filled:.6f}) "
            f"for symbol '{symbol}' on the {side} side."
        )

    expected_price = total_cost / size
    if side_lower == "buy":
        slippage_usd = expected_price - best_price
    else:
        slippage_usd = best_price - expected_price

    slippage_pct = (slippage_usd / best_price) * 100.0 if best_price > 0 else 0.0

    return pl.DataFrame(
        {
            "symbol": [symbol],
            "side": [side_lower],
            "size": [size],
            "best_price": [best_price],
            "expected_price": [expected_price],
            "slippage_usd": [slippage_usd],
            "slippage_pct": [slippage_pct],
        }
    )
