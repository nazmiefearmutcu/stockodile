"""Tiingo provider client implementation."""

import asyncio
import csv
import io
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
import msgspec

from stockodile.ratelimit.token_bucket import TokenBucketLimiter
from stockodile.schema.enums import CorpActionType
from stockodile.schema.records import Bar, CorporateAction, Record


class TiingoError(Exception):
    """Base exception for the Tiingo provider."""
    pass


class TiingoQuotaError(TiingoError):
    """Exception raised when the unique symbols monthly quota is exceeded."""
    pass


class TiingoRateLimitError(TiingoError):
    """Exception raised when API returns 429 Too Many Requests."""
    pass


class TiingoTicker(msgspec.Struct, frozen=True):
    """Represents a ticker from the supported tickers file."""

    ticker: str
    exchange: str
    asset_type: str
    price_currency: str
    start_date: str
    end_date: str


class TiingoEodPrice(msgspec.Struct):
    """Raw EOD price record from Tiingo."""

    date: str
    close: float
    high: float
    low: float
    open: float
    volume: float
    adjClose: float
    adjHigh: float
    adjLow: float
    adjOpen: float
    adjVolume: float
    divCash: float
    splitFactor: float


class TiingoIexPrice(msgspec.Struct):
    """Raw IEX intraday price record from Tiingo."""

    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


def parse_tiingo_date(date_str: str) -> int:
    """Parse ISO8601 date string to epoch nanoseconds."""
    clean_str = date_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean_str)
    return int(dt.timestamp() * 1_000_000_000)


class SymbolCapTracker:
    """Tracks unique symbols requested within the current calendar month."""

    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath
        self.lock = asyncio.Lock()

    def _get_current_month(self) -> str:
        return datetime.now(UTC).strftime("%Y-%m")

    async def register_symbol(self, symbol: str) -> None:
        """Register a symbol, raising TiingoQuotaError if quota is exceeded."""
        symbol = symbol.upper()
        current_month = self._get_current_month()

        async with self.lock:
            state: dict[str, Any] = {"month": current_month, "symbols": []}
            if self.filepath.exists():
                try:
                    data = self.filepath.read_bytes()
                    if data:
                        state = msgspec.json.decode(data)
                except Exception:
                    pass

            if state.get("month") != current_month:
                state = {"month": current_month, "symbols": []}

            symbols = list(state.get("symbols", []))
            if symbol in symbols:
                return

            if len(symbols) >= 500:
                raise TiingoQuotaError(
                    f"Tiingo monthly unique symbols quota (500) exceeded for {current_month}."
                )

            symbols.append(symbol)
            state["symbols"] = symbols

            self.filepath.parent.mkdir(parents=True, exist_ok=True)
            self.filepath.write_bytes(msgspec.json.encode(state))


class TiingoClient:
    """Client for querying the Tiingo API."""

    def __init__(
        self,
        api_key: str,
        tracker_path: str | Path | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        """Initialize the client.

        Args:
            api_key: Tiingo API token.
            tracker_path: Path to the JSON file tracking unique symbols.
            session: Optional pre-existing aiohttp client session.
        """
        self.api_key = api_key
        self.session = session
        self._own_session = False

        if tracker_path is None:
            tracker_path = Path.home() / ".stockodile_tiingo_quota.json"
        self.tracker = SymbolCapTracker(Path(tracker_path))

        # Enforce free tier rate limit: 50 requests/hour, capacity 50.
        self.limiter = TokenBucketLimiter(rate=50.0 / 3600.0, capacity=50.0)

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or initialize the HTTP client session."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
            self._own_session = True
        return self.session

    async def close(self) -> None:
        """Close the HTTP client session if owned by this client."""
        if self._own_session and self.session and not self.session.closed:
            await self.session.close()

    async def __aenter__(self) -> "TiingoClient":
        await self.get_session()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def download_supported_tickers(self) -> list[TiingoTicker]:
        """Fetch the full symbol universe from Supported Tickers ZIP.

        This endpoint is public and does not require an API token or count towards quota.
        """
        session = await self.get_session()
        url = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"

        async with session.get(url) as resp:
            if resp.status != 200:
                raise TiingoError(f"Failed to fetch supported tickers: HTTP {resp.status}")
            data = await resp.read()

        tickers = []
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for filename in zf.namelist():
                if filename.endswith(".csv"):
                    with zf.open(filename) as f:
                        text_file = io.TextIOWrapper(f, encoding="utf-8")
                        reader = csv.DictReader(text_file)
                        for row in reader:
                            tickers.append(
                                TiingoTicker(
                                    ticker=row["ticker"],
                                    exchange=row["exchange"],
                                    asset_type=row["assetType"],
                                    price_currency=row["priceCurrency"],
                                    start_date=row["startDate"],
                                    end_date=row["endDate"],
                                )
                            )
        return tickers

    async def get_eod_prices(
        self,
        ticker: str,
        start_date: str | None = None,
        end_date: str | None = None,
        resample_freq: str = "daily",
    ) -> list[Record]:
        """Fetch EOD historical prices with inline corporate actions.

        Respects the 500 unique symbols/month limit and enforces rate limiting.
        """
        ticker_upper = ticker.upper()
        await self.tracker.register_symbol(ticker_upper)
        await self.limiter.acquire()

        session = await self.get_session()
        url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"

        params: dict[str, str] = {}
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        if resample_freq:
            params["resampleFreq"] = resample_freq

        headers = {"Authorization": f"Token {self.api_key}"}

        async with session.get(url, params=params, headers=headers) as resp:
            if resp.status == 429:
                raise TiingoRateLimitError("Tiingo API rate limit exceeded (429).")
            if resp.status != 200:
                body = await resp.text()
                raise TiingoError(
                    f"Failed to fetch EOD prices for {ticker}: HTTP {resp.status} - {body}"
                )
            data = await resp.read()

        raw_prices = msgspec.json.decode(data, type=list[TiingoEodPrice])
        records: list[Record] = []

        for item in raw_prices:
            source_ts = parse_tiingo_date(item.date)
            local_ts = time.time_ns()

            bar = Bar(
                provider="tiingo",
                symbol=ticker_upper,
                symbol_raw=ticker,
                source_ts=source_ts,
                local_ts=local_ts,
                interval="1d",
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
                vwap=None,
                trade_count=None,
            )
            records.append(bar)

            if item.divCash > 0.0:
                ex_date = item.date.split("T")[0]
                div = CorporateAction(
                    provider="tiingo",
                    symbol=ticker_upper,
                    symbol_raw=ticker,
                    source_ts=source_ts,
                    local_ts=local_ts,
                    ex_date=ex_date,
                    type=CorpActionType.DIVIDEND_CASH,
                    value=item.divCash,
                )
                records.append(div)

            if abs(item.splitFactor - 1.0) > 1e-9:
                ex_date = item.date.split("T")[0]
                split = CorporateAction(
                    provider="tiingo",
                    symbol=ticker_upper,
                    symbol_raw=ticker,
                    source_ts=source_ts,
                    local_ts=local_ts,
                    ex_date=ex_date,
                    type=CorpActionType.SPLIT,
                    value=item.splitFactor,
                )
                records.append(split)

        return records

    async def get_intraday_bars(
        self,
        ticker: str,
        start_date: str | None = None,
        end_date: str | None = None,
        resample_freq: str = "1min",
    ) -> list[Bar]:
        """Fetch intraday bars from the Tiingo IEX endpoint.

        Respects the 500 unique symbols/month limit and enforces rate limiting.
        """
        ticker_upper = ticker.upper()
        await self.tracker.register_symbol(ticker_upper)
        await self.limiter.acquire()

        session = await self.get_session()
        url = f"https://api.tiingo.com/iex/{ticker}/prices"

        params: dict[str, str] = {}
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        if resample_freq:
            params["resampleFreq"] = resample_freq

        headers = {"Authorization": f"Token {self.api_key}"}

        async with session.get(url, params=params, headers=headers) as resp:
            if resp.status == 429:
                raise TiingoRateLimitError("Tiingo API rate limit exceeded (429).")
            if resp.status != 200:
                body = await resp.text()
                raise TiingoError(
                    f"Failed to fetch intraday bars for {ticker}: HTTP {resp.status} - {body}"
                )
            data = await resp.read()

        raw_prices = msgspec.json.decode(data, type=list[TiingoIexPrice])
        bars: list[Bar] = []

        for item in raw_prices:
            source_ts = parse_tiingo_date(item.date)
            local_ts = time.time_ns()

            bar = Bar(
                provider="tiingo",
                symbol=ticker_upper,
                symbol_raw=ticker,
                source_ts=source_ts,
                local_ts=local_ts,
                interval=resample_freq,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
                vwap=None,
                trade_count=None,
            )
            bars.append(bar)

        return bars
