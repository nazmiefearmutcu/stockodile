from __future__ import annotations

import os

from stockodile.depth.base import DepthSource


def select_depth_source(*, bins: int = 40, top_n: int = 10, method: str = "uniform") -> DepthSource:
    """Return Alpaca L1 iff both Alpaca env keys are set, else the keyless synthetic source.

    This is the 'upgrade without code change' switch: the user sets env vars only.
    """
    if os.environ.get("ALPACA_API_KEY") and os.environ.get("ALPACA_API_SECRET"):
        from stockodile.depth.alpaca_l1 import AlpacaL1DepthSource
        return AlpacaL1DepthSource()
    from stockodile.depth.synthetic import SyntheticYahooDepthSource
    return SyntheticYahooDepthSource(bins=bins, top_n=top_n, method=method)
