"""Pure volume-at-price synthesis. No I/O — fully unit-testable.

Turns free 1-minute OHLCV bars into a *relative* volume-at-price histogram
(where volume historically concentrated). This is NOT real resting liquidity.
"""

from __future__ import annotations

from collections.abc import Sequence

from stockodile.schema.records import Bar, Level

Method = str  # "uniform" | "typical" | "close"


def reference_price(bars: Sequence[Bar]) -> float:
    """Last bar close — the price the ladder is centered on."""
    if not bars:
        raise ValueError("no bars to derive reference price")
    return float(bars[-1].close)


def volume_at_price(
    bars: Sequence[Bar], *, bins: int = 40, method: Method = "uniform"
) -> list[Level]:
    """Accumulate bar volume into ``bins`` price buckets across the session range.

    - ``uniform``: spread each bar's volume evenly across [low, high].
    - ``typical``: dump each bar's volume at its typical price (H+L+C)/3.
    - ``close``:   dump each bar's volume at its close.
    Returns ``[(price, size), ...]`` sorted ascending by price; empty if no volume.
    """
    if bins < 1:
        raise ValueError("bins must be >= 1")
    usable = [b for b in bars if b.volume > 0 and b.high >= b.low]
    if not usable:
        return []
    lo = min(b.low for b in usable)
    hi = max(b.high for b in usable)
    if hi <= lo:
        # all trading at one price — single bucket
        total = sum(b.volume for b in usable)
        return [(round(lo, 6), float(total))]
    width = (hi - lo) / bins
    buckets = [0.0] * bins

    def _idx(price: float) -> int:
        i = int((price - lo) / width)
        return min(max(i, 0), bins - 1)

    for b in usable:
        if method == "typical":
            buckets[_idx((b.high + b.low + b.close) / 3.0)] += b.volume
        elif method == "close":
            buckets[_idx(b.close)] += b.volume
        else:  # uniform across the bar's own [low, high]
            b_lo, b_hi = _idx(b.low), _idx(b.high)
            span = b_hi - b_lo + 1
            share = b.volume / span
            for i in range(b_lo, b_hi + 1):
                buckets[i] += share
    out: list[Level] = []
    for i, vol in enumerate(buckets):
        if vol > 0:
            center = lo + (i + 0.5) * width
            out.append((round(center, 6), round(vol, 6)))
    return out


def split_ladder(
    profile: Sequence[Level], reference_price: float, *, top_n: int = 10
) -> tuple[list[Level], list[Level]]:
    """Split a price/volume profile into (bids, asks) around ``reference_price``.

    bids = levels below ref, price descending; asks = levels above ref, price ascending.
    Each truncated to ``top_n``.
    """
    bids = sorted((lv for lv in profile if lv[0] < reference_price), key=lambda lv: -lv[0])
    asks = sorted((lv for lv in profile if lv[0] > reference_price), key=lambda lv: lv[0])
    return bids[:top_n], asks[:top_n]
