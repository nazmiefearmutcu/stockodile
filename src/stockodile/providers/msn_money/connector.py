from __future__ import annotations

import json
import logging
import random
import time
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime
from typing import Any

import aiohttp

from stockodile.providers.base import Provider
from stockodile.reference.registry import Instrument, InstrumentRegistry
from stockodile.schema.enums import CorpActionType, SecurityType
from stockodile.schema.records import Bar, CorporateAction, Record
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
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.msn.com/",
        "Cookie": f"MUID={muid};",
    }


def safe_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        if isinstance(v, str):
            v = v.strip().replace(",", "")
            if v in ("", "N/A", "null", "None"):
                return default
        return float(v)
    except (ValueError, TypeError):
        return default


class MsnMoneyProvider(Provider):
    name = "msn_money"
    ws_url = ""
    rest_url = "https://assets.msn.com"

    def __init__(
        self,
        symbols: list[str],
        channels: list[str],
        out: Sink,
        registry: InstrumentRegistry,
        apikey: str = "0QfOX3Vn51YCzitbLaRkTTBadtWpgTN8NZLW0C1SEM",
        ocid: str = "finance-utils-peregrine",
    ) -> None:
        super().__init__(symbols, channels, out, registry)
        self.apikey = apikey
        self.ocid = ocid
        self.session: aiohttp.ClientSession | None = None

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
        raise NotImplementedError(
            "MSN Money provider is strictly a batch/backfill provider and "
            "does not support streaming run loop."
        )

    async def backfill(
        self,
        channel: str,
        symbol: str,
        start_ns: int,
        end_ns: int,
    ) -> AsyncIterator[Record]:
        if self.session is None:
            self.session = aiohttp.ClientSession()

        try:
            sec_id = await self._resolve_sec_id(symbol)
            local_ts = time.time_ns()

            if channel in ("bar", "ohlcv"):
                duration_ns = end_ns - start_ns
                one_day_ns = 24 * 60 * 60 * 1_000_000_000
                five_days_ns = 5 * one_day_ns
                one_month_ns = 31 * one_day_ns
                three_months_ns = 92 * one_day_ns
                six_months_ns = 184 * one_day_ns
                one_year_ns = 365 * one_day_ns
                five_years_ns = 5 * one_year_ns

                if duration_ns <= five_days_ns:
                    chart_type = "5D"
                elif duration_ns <= one_month_ns:
                    chart_type = "1M"
                elif duration_ns <= three_months_ns:
                    chart_type = "3M"
                elif duration_ns <= six_months_ns:
                    chart_type = "6M"
                elif duration_ns <= one_year_ns:
                    chart_type = "1Y"
                elif duration_ns <= five_years_ns:
                    chart_type = "5Y"
                else:
                    chart_type = "All"

                url = f"{self.rest_url}/service/Finance/Charts"
                params = {
                    "apikey": self.apikey,
                    "ocid": self.ocid,
                    "ids": sec_id,
                    "type": chart_type,
                    "wrapodata": "false",
                    "cm": "en-us",
                }
                headers = get_spoofed_headers()

                async with self.session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        is_valid = (
                            data
                            and isinstance(data, list)
                            and isinstance(data[0], dict)
                            and "series" in data[0]
                        )
                        if is_valid:
                            series = data[0]["series"]
                            open_p = series.get("openPrices", [])
                            close_p = series.get("prices", [])
                            high_p = series.get("pricesHigh", [])
                            low_p = series.get("pricesLow", [])
                            volumes = series.get("volumes", [])
                            timestamps = series.get("timeStamps", [])

                            # Compute interval dynamically from first two timestamps
                            computed_interval = None
                            if len(timestamps) >= 2:
                                try:
                                    ts0 = timestamps[0].replace("Z", "+00:00")
                                    ts1 = timestamps[1].replace("Z", "+00:00")
                                    dt0 = datetime.fromisoformat(ts0)
                                    dt1 = datetime.fromisoformat(ts1)
                                    delta_sec = abs((dt1 - dt0).total_seconds())

                                    if delta_sec <= 90:
                                        computed_interval = "1m"
                                    elif delta_sec <= 350:
                                        computed_interval = "5m"
                                    elif delta_sec <= 1000:
                                        computed_interval = "15m"
                                    elif delta_sec <= 2000:
                                        computed_interval = "30m"
                                    elif delta_sec <= 4000:
                                        computed_interval = "1h"
                                    elif delta_sec <= 100000:
                                        computed_interval = "1d"
                                    elif delta_sec <= 7 * 86400 + 10000:
                                        computed_interval = "1w"
                                    else:
                                        computed_interval = "1mo"
                                except Exception:
                                    pass

                            if computed_interval is None:
                                if "Y" in chart_type or chart_type == "All":
                                    computed_interval = "1d"
                                else:
                                    computed_interval = "15m"

                            n = len(timestamps)
                            for idx in range(n):
                                ts_str = timestamps[idx]
                                try:
                                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                                    source_ts = int(dt.timestamp() * 1e9)
                                except Exception:
                                    source_ts = None

                                if source_ts is not None and not (start_ns <= source_ts <= end_ns):
                                    continue

                                bar = Bar(
                                    provider=self.name,
                                    symbol=symbol.upper(),
                                    symbol_raw=symbol,
                                    source_ts=source_ts,
                                    local_ts=local_ts,
                                    interval=computed_interval,
                                    open=safe_float(open_p[idx]) if idx < len(open_p) else 0.0,
                                    high=safe_float(high_p[idx]) if idx < len(high_p) else 0.0,
                                    low=safe_float(low_p[idx]) if idx < len(low_p) else 0.0,
                                    close=safe_float(close_p[idx]) if idx < len(close_p) else 0.0,
                                    volume=safe_float(volumes[idx]) if idx < len(volumes) else 0.0,
                                )
                                yield bar

            elif channel in ("corp_action", "corp_actions"):
                url = f"{self.rest_url}/service/Finance/QuoteSummary"
                params = {
                    "apikey": self.apikey,
                    "ocid": self.ocid,
                    "cm": "en-us",
                    "it": "web",
                    "ids": sec_id,
                    "intents": "Quotes,Exchanges,QuoteDetails",
                }
                headers = get_spoofed_headers()

                async with self.session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data and isinstance(data, list) and isinstance(data[0], dict):
                            equity = data[0].get("equity", {})
                            share_stats = equity.get("shareStatistics", {})

                            # Parse Dividend
                            ex_div_amt = share_stats.get("exDividendAmount")
                            ex_div_date = share_stats.get("exDividendDate")
                            if ex_div_amt and ex_div_date:
                                ex_date = ex_div_date.split("T")[0]
                                try:
                                    dt = datetime.strptime(ex_date, "%Y-%m-%d").replace(tzinfo=UTC)
                                    ts = int(dt.timestamp() * 1e9)
                                    if start_ns <= ts <= end_ns:
                                        yield CorporateAction(
                                            provider=self.name,
                                            symbol=symbol.upper(),
                                            symbol_raw=symbol,
                                            source_ts=ts,
                                            local_ts=local_ts,
                                            ex_date=ex_date,
                                            type=CorpActionType.DIVIDEND_CASH,
                                            value=safe_float(ex_div_amt),
                                        )
                                except Exception as e:
                                    log.debug(
                                        "Error parsing dividend ex_date %s: %s",
                                        ex_div_date,
                                        e,
                                    )

                            # Parse Split
                            last_split_factor = share_stats.get("lastSplitFactor")
                            last_split_date = share_stats.get("lastSplitDate")
                            if last_split_factor and last_split_date:
                                ex_date = last_split_date.split("T")[0]
                                try:
                                    dt = datetime.strptime(ex_date, "%Y-%m-%d").replace(tzinfo=UTC)
                                    ts = int(dt.timestamp() * 1e9)
                                    if start_ns <= ts <= end_ns:
                                        split_str = str(last_split_factor)
                                        if ":" in split_str:
                                            parts = split_str.split(":")
                                            try:
                                                val = float(parts[0]) / float(parts[1])
                                            except (ValueError, ZeroDivisionError):
                                                val = float(parts[0])
                                        else:
                                            val = float(split_str)
                                        yield CorporateAction(
                                            provider=self.name,
                                            symbol=symbol.upper(),
                                            symbol_raw=symbol,
                                            source_ts=ts,
                                            local_ts=local_ts,
                                            ex_date=ex_date,
                                            type=CorpActionType.SPLIT,
                                            value=val,
                                        )
                                except Exception as e:
                                    log.debug(
                                        "Error parsing split factor/date %s/%s: %s",
                                        last_split_factor,
                                        last_split_date,
                                        e,
                                    )

        except Exception as e:
            log.error("MSN Money backfill error for %s: %s", symbol, e)

    async def _resolve_sec_id(self, symbol: str) -> str:
        if not self.session:
            self.session = aiohttp.ClientSession()

        query_sym = symbol.split(".")[0].split(":")[0]
        url = "https://services.bingapis.com/contentservices-finance.csautosuggest/api/v1/Query"
        params = {"query": query_sym, "market": "en-us", "count": "3"}
        headers = get_spoofed_headers()

        def normalize_ticker(t: str) -> str:
            return t.upper().replace(".", "").replace("-", "").replace("/", "").strip()

        norm_orig = normalize_ticker(symbol)
        norm_query = normalize_ticker(query_sym)

        try:
            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    stocks = data.get("data", {}).get("stocks", [])
                    for stock_str in stocks:
                        try:
                            stock_data = json.loads(stock_str)
                            rt00s = stock_data.get("RT00S", "")
                            is_match = (
                                normalize_ticker(rt00s) == norm_orig
                                or normalize_ticker(rt00s) == norm_query
                            )
                            if is_match:
                                return str(stock_data.get("SecId"))
                        except Exception:
                            continue
                    if stocks:
                        try:
                            return str(json.loads(stocks[0]).get("SecId"))
                        except Exception:
                            pass
        except Exception as e:
            log.debug("Error resolving ticker symbol suggestions: %s", e)

        return symbol.lower()

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None
