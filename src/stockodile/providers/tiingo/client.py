"""Tiingo provider client implementation."""

import asyncio
import csv
import io
import os
import tempfile
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
import msgspec

from stockodile.ratelimit.api_key import ApiKeyPool
from stockodile.ratelimit.token_bucket import TokenBucketLimiter
from stockodile.schema.enums import CorpActionType
from stockodile.schema.records import Bar, CorporateAction, OptionQuote, Record


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

        def _sync_register() -> None:
            import fcntl

            lock_filepath = self.filepath.with_suffix(".lock")
            # Create parent directory for lock file if not exists
            lock_filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(lock_filepath, "a+") as lock_file:
                fcntl.flock(lock_file, fcntl.LOCK_EX)
                try:
                    state: dict[str, Any] = {"month": current_month, "symbols": []}
                    if self.filepath.exists():
                        try:
                            data = self.filepath.read_bytes()
                            if data:
                                state = msgspec.json.decode(data)
                        except Exception as e:
                            # If file is not empty but fails to parse, it is corrupted.
                            if self.filepath.stat().st_size > 0:
                                raise TiingoError(
                                    "Tiingo quota tracking file at "
                                    f"{self.filepath} is corrupted: {e}"
                                ) from e

                    if state.get("month") != current_month:
                        state = {"month": current_month, "symbols": []}

                    symbols = list(state.get("symbols", []))
                    if symbol in symbols:
                        return

                    if len(symbols) >= 500:
                        raise TiingoQuotaError(
                            "Tiingo monthly unique symbols quota (500) "
                            f"exceeded for {current_month}."
                        )

                    symbols.append(symbol)
                    state["symbols"] = symbols

                    self.filepath.parent.mkdir(parents=True, exist_ok=True)
                    with tempfile.NamedTemporaryFile(
                        "wb", dir=self.filepath.parent, delete=False
                    ) as tf:
                        tf.write(msgspec.json.encode(state))
                        temp_name = tf.name
                    try:
                        os.replace(temp_name, self.filepath)
                    except Exception:
                        if os.path.exists(temp_name):
                            os.remove(temp_name)
                        raise
                finally:
                    fcntl.flock(lock_file, fcntl.LOCK_UN)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _sync_register)


class TiingoClient:
    """Client for querying the Tiingo API."""

    def __init__(
        self,
        api_key: str | None = None,
        api_key_pool: ApiKeyPool | None = None,
        tracker_path: str | Path | None = None,
        session: aiohttp.ClientSession | None = None,
        rate_limiter: TokenBucketLimiter | None = None,
    ) -> None:
        """Initialize the client.

        Args:
            api_key: Tiingo API token.
            api_key_pool: Optional API key pool.
            tracker_path: Path to the JSON file tracking unique symbols.
            session: Optional pre-existing aiohttp client session.
            rate_limiter: Optional TokenBucketLimiter instance.
        """
        self.api_key = api_key
        self.api_key_pool = api_key_pool
        self.session = session
        self._own_session = False

        if tracker_path is None:
            tracker_path = Path.home() / ".stockodile_tiingo_quota.json"
        self.tracker = SymbolCapTracker(Path(tracker_path))

        # Enforce free tier rate limit: 50 requests/hour, capacity 50.
        self.limiter = rate_limiter or TokenBucketLimiter(
            rate=50.0 / 3600.0,
            capacity=50.0,
            api_key_pool=api_key_pool,
            provider="tiingo",
        )

    def _get_api_key(self) -> str:
        """Retrieve the API key from pool or direct parameter."""
        if self.api_key_pool:
            key = self.api_key_pool.get_key("tiingo")
            if key:
                return key
        if self.api_key:
            return self.api_key
        raise TiingoError("No Tiingo API key or API key pool available.")

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or initialize the HTTP client session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=60.0, connect=10.0)
            self.session = aiohttp.ClientSession(timeout=timeout)
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
        # Register only after successful response (quota is scarce: 500 symbols/month)
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

        interval_map = {
            "daily": "1d",
            "weekly": "1w",
            "monthly": "1mo",
            "annually": "1y",
        }
        bar_interval = interval_map.get((resample_freq or "daily").lower(), "1d")

        key = self._get_api_key()
        headers = {"Authorization": f"Token {key}"}

        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 429:
                    if self.api_key_pool:
                        self.api_key_pool.report_throttled("tiingo", key, 60.0)
                    raise TiingoRateLimitError("Tiingo API rate limit exceeded (429).")
                if resp.status in (401, 403):
                    if self.api_key_pool:
                        self.api_key_pool.report_exhausted("tiingo", key, 86400.0 * 365)
                    body = await resp.text()
                    raise TiingoError(f"Tiingo authentication failure: HTTP {resp.status} - {body}")
                if resp.status != 200:
                    if self.api_key_pool:
                        self.api_key_pool.report_failure("tiingo", key)
                    body = await resp.text()
                    raise TiingoError(
                        f"Failed to fetch EOD prices for {ticker}: HTTP {resp.status} - {body}"
                    )
                data = await resp.read()
        except aiohttp.ClientError as e:
            if self.api_key_pool:
                self.api_key_pool.report_failure("tiingo", key)
            raise TiingoError(f"Network error while calling Tiingo: {e}") from e

        try:
            raw_prices = msgspec.json.decode(data, type=list[TiingoEodPrice])
        except Exception as e:
            if self.api_key_pool:
                self.api_key_pool.report_failure("tiingo", key)
            raise TiingoError(f"Failed to decode Tiingo EOD response: {e}") from e

        if self.api_key_pool:
            self.api_key_pool.report_success("tiingo", key)
        await self.tracker.register_symbol(ticker_upper)

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
                interval=bar_interval,
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

        key = self._get_api_key()
        headers = {"Authorization": f"Token {key}"}

        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 429:
                    if self.api_key_pool:
                        self.api_key_pool.report_throttled("tiingo", key, 60.0)
                    raise TiingoRateLimitError("Tiingo API rate limit exceeded (429).")
                if resp.status in (401, 403):
                    if self.api_key_pool:
                        self.api_key_pool.report_exhausted("tiingo", key, 86400.0 * 365)
                    body = await resp.text()
                    raise TiingoError(f"Tiingo authentication failure: HTTP {resp.status} - {body}")
                if resp.status != 200:
                    if self.api_key_pool:
                        self.api_key_pool.report_failure("tiingo", key)
                    body = await resp.text()
                    raise TiingoError(
                        f"Failed to fetch intraday bars for {ticker}: HTTP {resp.status} - {body}"
                    )
                data = await resp.read()
        except aiohttp.ClientError as e:
            if self.api_key_pool:
                self.api_key_pool.report_failure("tiingo", key)
            raise TiingoError(f"Network error while calling Tiingo: {e}") from e

        try:
            raw_prices = msgspec.json.decode(data, type=list[TiingoIexPrice])
        except Exception as e:
            if self.api_key_pool:
                self.api_key_pool.report_failure("tiingo", key)
            raise TiingoError(f"Failed to decode Tiingo IEX response: {e}") from e

        if self.api_key_pool:
            self.api_key_pool.report_success("tiingo", key)
        await self.tracker.register_symbol(ticker_upper)

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

    async def fetch_option_chain(
        self,
        symbol: str,
        expiry: str | None = None,
    ) -> list[OptionQuote]:
        """Fetch option chain quotes.

        Not supported by TiingoClient.
        """
        raise NotImplementedError("TiingoClient does not support option chain fetching.")
