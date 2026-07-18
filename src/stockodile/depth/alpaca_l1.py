from __future__ import annotations

import os
import time
from typing import Any

import aiohttp

from stockodile.schema.records import DepthProfile, Level

_LATEST_QUOTE_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/quotes/latest"


class AlpacaL1DepthSource:
    """Real L1 top-of-book via Alpaca's official REST latest-quote endpoint."""

    def __init__(
        self, *, key: str | None = None, secret: str | None = None, feed: str | None = None
    ) -> None:
        resolved_key = key or os.environ.get("ALPACA_API_KEY")
        resolved_secret = secret or os.environ.get("ALPACA_API_SECRET")
        if not resolved_key or not resolved_secret:
            raise ValueError("Alpaca credentials missing (ALPACA_API_KEY / ALPACA_API_SECRET).")
        self._key: str = resolved_key
        self._secret: str = resolved_secret
        self._feed: str = feed or os.environ.get("ALPACA_FEED") or "iex"

    async def snapshot(self, symbol: str) -> DepthProfile:
        headers = {"APCA-API-KEY-ID": self._key, "APCA-API-SECRET-KEY": self._secret}
        url = _LATEST_QUOTE_URL.format(symbol=symbol.upper())
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, params={"feed": self._feed}) as resp:
                if resp.status in (401, 403):
                    raise ValueError(
                        "Alpaca auth failed — check ALPACA_API_KEY/ALPACA_API_SECRET."
                    )
                if resp.status >= 400:
                    raise ValueError(f"Alpaca request failed: HTTP {resp.status}")
                data: dict[str, Any] = await resp.json()
        q = data["quote"]
        bid_px, bid_sz = float(q["bp"]), float(q["bs"])
        ask_px, ask_sz = float(q["ap"]), float(q["as"])
        ref = (bid_px + ask_px) / 2.0 if bid_px and ask_px else (bid_px or ask_px)
        bids: list[Level] = [(bid_px, bid_sz)] if bid_px else []
        asks: list[Level] = [(ask_px, ask_sz)] if ask_px else []
        return DepthProfile(
            provider="alpaca", symbol=f"alpaca:{symbol.upper()}", symbol_raw=symbol.upper(),
            local_ts=time.time_ns(), bids=bids, asks=asks, reference_price=ref,
            basis="alpaca_l1", is_synthetic=False, depth=len(bids) + len(asks),
        )
