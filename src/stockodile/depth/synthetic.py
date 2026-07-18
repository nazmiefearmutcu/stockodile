from __future__ import annotations

import time
from typing import Any

from stockodile.depth.vap import reference_price, split_ladder, volume_at_price
from stockodile.providers.yahoo import YahooClient
from stockodile.schema.records import DepthProfile


class SyntheticYahooDepthSource:
    """Keyless synthetic depth: Yahoo 1m bars -> volume-at-price ladder.

    RELATIVE liquidity (where volume concentrated), NOT real resting orders.
    """

    def __init__(
        self,
        client: Any = None,
        *,
        bins: int = 40,
        top_n: int = 10,
        method: str = "uniform",
    ) -> None:
        self._client = client or YahooClient()
        self._bins = bins
        self._top_n = top_n
        self._method = method

    async def snapshot(self, symbol: str) -> DepthProfile:
        bars = await self._client.fetch_intraday_bars(symbol, "1m")
        if not bars:
            raise ValueError(f"no Yahoo 1m bars for {symbol!r}")
        ref = reference_price(bars)
        profile = volume_at_price(bars, bins=self._bins, method=self._method)
        bids, asks = split_ladder(profile, ref, top_n=self._top_n)
        return DepthProfile(
            provider="synth", symbol=f"synth:{symbol.upper()}", symbol_raw=symbol.upper(),
            local_ts=time.time_ns(), bids=bids, asks=asks, reference_price=ref,
            basis="yahoo_1m_vap", is_synthetic=True, depth=len(bids) + len(asks),
            source_ts=bars[-1].local_ts,
        )
