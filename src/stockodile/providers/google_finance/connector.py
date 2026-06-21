from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Iterable
from datetime import datetime
from typing import Any

import aiohttp
from bs4 import BeautifulSoup

from stockodile.providers.base import Provider
from stockodile.reference.registry import Instrument, InstrumentRegistry
from stockodile.schema.enums import SecurityType
from stockodile.schema.records import Fundamental, IndexValue, Quote, Record, Trade
from stockodile.sink.base import Sink

log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",  # noqa: E501
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",  # noqa: E501
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",  # noqa: E501
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/125.0.0.0 Safari/537.36",  # noqa: E501
]


def get_spoofed_headers() -> dict[str, str]:
    muid = "".join(random.choices("0123456789ABCDEF", k=32))
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Cookie": f"MUID={muid};",
    }


def get_possible_google_symbols(symbol: str) -> list[str]:
    symbol_upper = symbol.upper()
    if ":" in symbol_upper:
        return [symbol_upper]
    if symbol_upper in ("^SPX", ".INX", "SPX"):
        return [".INX:INDEXSP"]
    if symbol_upper in ("^IXIC", "COMP"):
        return [".IXIC:INDEXNASDAQ"]
    if symbol_upper in ("^DJI", "DJI"):
        return [".DJI:INDEXDJX"]
    return [
        f"{symbol_upper}:NASDAQ",
        f"{symbol_upper}:NYSE",
        f"{symbol_upper}:INDEXSP",
        symbol_upper,
    ]


def parse_val_and_unit(val_str: str, key: str) -> tuple[float, str]:
    val_str = val_str.strip()
    currency = "USD"
    if val_str.startswith("$"):
        currency = "USD"
        val_str = val_str[1:]
    elif val_str.startswith("€"):
        currency = "EUR"
        val_str = val_str[1:]
    elif val_str.startswith("£"):
        currency = "GBP"
        val_str = val_str[1:]

    if val_str.endswith("%"):
        val_str = val_str[:-1]
        try:
            return float(val_str.replace(",", "")), "percent"
        except ValueError:
            return 0.0, "percent"

    multiplier = 1.0
    if val_str.endswith("T"):
        multiplier = 1e12
        val_str = val_str[:-1]
    elif val_str.endswith("B"):
        multiplier = 1e9
        val_str = val_str[:-1]
    elif val_str.endswith("M"):
        multiplier = 1e6
        val_str = val_str[:-1]
    elif val_str.endswith("K"):
        multiplier = 1e3
        val_str = val_str[:-1]

    val_str = val_str.replace(",", "")
    try:
        val = float(val_str) * multiplier
    except ValueError:
        val = 0.0

    if key in (
        "Open",
        "High",
        "Low",
        "Mkt. cap",
        "Quarterly dividend",
        "52-wk high",
        "52-wk low",
        "EPS",
    ):
        unit = currency
    elif key in ("Volume", "Avg. vol.", "Shares outstanding"):
        unit = "shares"
    elif key in ("Dividend",):
        unit = "percent"
    elif key in ("No. of employees",):
        unit = "count"
    elif key in ("P/E ratio", "Beta"):
        unit = "ratio"
    else:
        unit = "unknown"

    return val, unit


def parse_date(date_str: str) -> str:
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


TAG_MAP = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Mkt. cap": "market_cap",
    "Avg. vol.": "avg_volume",
    "Volume": "volume",
    "Dividend": "dividend_yield",
    "Quarterly dividend": "quarterly_dividend",
    "Ex-dividend date": "ex_dividend_date",
    "P/E ratio": "pe_ratio",
    "52-wk high": "52_week_high",
    "52-wk low": "52_week_low",
    "EPS": "eps",
    "Beta": "beta",
    "Shares outstanding": "shares_outstanding",
    "No. of employees": "employees",
}


class GoogleFinanceProvider(Provider):
    name = "google_finance"
    ws_url = ""
    rest_url = "https://www.google.com/finance"

    def __init__(
        self,
        symbols: list[str],
        channels: list[str],
        out: Sink,
        registry: InstrumentRegistry,
    ) -> None:
        super().__init__(symbols, channels, out, registry)
        self.session: aiohttp.ClientSession | None = None
        self._running = False

    async def list_instruments(self) -> list[Instrument]:
        insts = []
        for sym in self.symbols:
            sec_type = (
                SecurityType.UNKNOWN
                if (sym.startswith("^") or sym.startswith("."))
                else SecurityType.CS
            )
            insts.append(
                Instrument(
                    symbol=sym,
                    provider=self.name,
                    symbol_raw=sym,
                    security_type=sec_type,
                )
            )
        return insts

    async def _subscribe(self, transport: Any) -> None:
        pass

    def normalize(self, msg: object, local_ts: int) -> Iterable[Record]:
        return ()

    async def run(self, max_reconnects: int = -1) -> None:
        self._running = True
        try:
            async with aiohttp.ClientSession() as session:
                self.session = session
                while self._running:
                    for symbol in self.symbols:
                        try:
                            records = await self._scrape_symbol(symbol)
                            for rec in records:
                                await self.out.put(rec)
                        except Exception as e:
                            log.error("Google Finance scraper error for %s: %s", symbol, e)
                    # Poll interval
                    for _ in range(10):
                        if not self._running:
                            break
                        await asyncio.sleep(1.0)
        finally:
            self._running = False
            self.session = None

    async def _scrape_symbol(self, symbol: str) -> list[Record]:
        if not self.session:
            return []

        possibilities = get_possible_google_symbols(symbol)
        local_ts = time.time_ns()

        for g_sym in possibilities:
            url = f"{self.rest_url}/quote/{g_sym}"
            headers = get_spoofed_headers()
            try:
                async with self.session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10.0),
                ) as resp:
                    if resp.status != 200:
                        continue
                    html = await resp.text()

                    soup = BeautifulSoup(html, "html.parser")

                    # Parse price
                    price_el = soup.find(class_="N6SYTe")
                    if not price_el:
                        continue
                    price_str = price_el.get_text(strip=True)
                    price = float(price_str.replace(",", "").replace("$", ""))

                    records: list[Record] = []

                    # Real-time update
                    is_index = symbol.startswith("^") or symbol.startswith(".") or "INDEX" in g_sym
                    if is_index:
                        if "index_value" in self.channels:
                            records.append(
                                IndexValue(
                                    provider=self.name,
                                    symbol=symbol.upper(),
                                    symbol_raw=symbol,
                                    source_ts=None,
                                    local_ts=local_ts,
                                    value=price,
                                )
                            )
                    else:
                        if "trade" in self.channels:
                            records.append(
                                Trade(
                                    provider=self.name,
                                    symbol=symbol.upper(),
                                    symbol_raw=symbol,
                                    source_ts=None,
                                    local_ts=local_ts,
                                    id="",
                                    price=price,
                                    size=1.0,
                                )
                            )
                        if "quote" in self.channels:
                            records.append(
                                Quote(
                                    provider=self.name,
                                    symbol=symbol.upper(),
                                    symbol_raw=symbol,
                                    source_ts=None,
                                    local_ts=local_ts,
                                    bid_px=price,
                                    bid_sz=1.0,
                                    ask_px=price,
                                    ask_sz=1.0,
                                )
                            )

                    # Parse fundamentals
                    if "fundamental" in self.channels:
                        for key_el in soup.find_all(class_="SwQK7"):
                            val_el = key_el.find_next(class_="dO6ijd")
                            if val_el:
                                key_text = key_el.get_text(strip=True)
                                val_text = val_el.get_text(strip=True)
                                if key_text in TAG_MAP:
                                    tag = TAG_MAP[key_text]
                                    end_str = ""
                                    if key_text == "Ex-dividend date":
                                        end_str = parse_date(val_text)
                                        val = 0.0
                                        unit = "date"
                                    else:
                                        val, unit = parse_val_and_unit(val_text, key_text)
                                    records.append(
                                        Fundamental(
                                            provider=self.name,
                                            symbol=symbol.upper(),
                                            symbol_raw=symbol,
                                            source_ts=None,
                                            local_ts=local_ts,
                                            taxonomy=self.name,
                                            tag=tag,
                                            unit=unit,
                                            val=val,
                                            end=end_str,
                                        )
                                    )
                    return records
            except Exception as e:
                log.debug("Failed checking symbol possibility %s: %s", g_sym, e)
                continue
        return []
