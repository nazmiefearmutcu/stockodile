"""Yahoo Finance provider client implementation."""

import asyncio
import logging
import random
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any, Self

import pandas as pd
import requests
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from yfinance.exceptions import YFRateLimitError

from stockodile.ratelimit.token_bucket import TokenBucket
from stockodile.schema.enums import CorpActionType, FundPeriod, OptType
from stockodile.schema.records import (
    Bar,
    CorporateAction,
    Fundamental,
    InsiderTransaction,
    OptionQuote,
    Record,
)

logger = logging.getLogger(__name__)

USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15"
    ),
]


def _clean_float(val: Any) -> float | None:
    """Helper to convert float values to None if NaN."""
    if pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _clean_str(val: Any) -> str | None:
    """Helper to convert string values to None if NaN or None."""
    if pd.isna(val) or val is None:
        return None
    return str(val)


class YahooClient:
    """Client for Yahoo Finance using the unofficial yfinance library.

    Designed with rate limiting, retries, and crumb/cookie session resets on 429 errors.
    """

    def __init__(
        self,
        rate_limiter: TokenBucket | None = None,
        backoff_delay: float = 5.0,
    ) -> None:
        """Initialize YahooClient.

        Args:
            rate_limiter: Token bucket rate limiter. Defaults to ~1 req/sec.
            backoff_delay: Initial retry/backoff delay on rate limits.
        """
        # Default rate limit of ~1 req per 1.5 seconds, burst capacity 5
        self.rate_limiter = rate_limiter or TokenBucket(capacity=5.0, refill_rate=1.0 / 1.5)
        self.backoff_delay = backoff_delay
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Construct a requests session with randomized User-Agent and retry logic."""
        session = requests.Session()
        ua = random.choice(USER_AGENTS)
        session.headers.update(
            {
                "User-Agent": ua,
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        retries = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def reset_session(self) -> None:
        """Reset the internal requests session to force a fresh cookie and crumb handshake."""
        try:
            self.session.close()
        except Exception as e:
            logger.debug("Error closing old requests session: %s", e)
        self.session = self._create_session()
        logger.info("Yahoo Finance requests session reset with fresh User-Agent and clean cookies.")

    async def _execute_yf_call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run a yfinance blocking call in an executor, handling 429s and crumb reset logic."""
        max_retries = 5
        delay = self.backoff_delay

        for attempt in range(max_retries):
            await self.rate_limiter.acquire(1.0)

            # Bind the current requests session
            kwargs["session"] = self.session

            try:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, lambda: func(*args, **kwargs))
                return result
            except YFRateLimitError:
                logger.warning(
                    "Yahoo Finance rate limit (HTTP 429) hit. "
                    "Attempt %d/%d. Backing off for %.2f seconds.",
                    attempt + 1,
                    max_retries,
                    delay,
                )
                self.rate_limiter.update_backoff(delay)
                self.reset_session()
                await asyncio.sleep(delay)
                delay *= 2.0
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 429:
                    logger.warning(
                        "Yahoo Finance HTTP 429 error. "
                        "Attempt %d/%d. Backing off for %.2f seconds.",
                        attempt + 1,
                        max_retries,
                        delay,
                    )
                    self.rate_limiter.update_backoff(delay)
                    self.reset_session()
                    await asyncio.sleep(delay)
                    delay *= 2.0
                else:
                    raise
            except Exception as exc:
                exc_str = str(exc).lower()
                if "crumb" in exc_str or "rate limit" in exc_str or "429" in exc_str:
                    logger.warning(
                        "Yahoo Finance potential rate limit or crumb issue: %s. "
                        "Attempt %d/%d. Resetting session and backing off for %.2f seconds.",
                        exc,
                        attempt + 1,
                        max_retries,
                        delay,
                    )
                    self.rate_limiter.update_backoff(delay)
                    self.reset_session()
                    await asyncio.sleep(delay)
                    delay *= 2.0
                else:
                    logger.error("Unexpected Yahoo Finance client error: %s", exc)
                    raise

        raise RuntimeError(f"Yahoo Finance provider request failed after {max_retries} attempts.")

    async def close(self) -> None:
        """Close the requests session."""
        self.session.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def fetch_eod_history(
        self,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
    ) -> list[Record]:
        """Fetch daily historical EOD bars and embedded splits/dividends.

        Args:
            symbol: Ticker symbol (e.g. AAPL)
            start: Start date string (YYYY-MM-DD)
            end: End date string (YYYY-MM-DD)

        Returns:
            A list of Bar and CorporateAction records.
        """

        def _call(session: requests.Session) -> pd.DataFrame:
            ticker = yf.Ticker(symbol, session=session)
            if start:
                return ticker.history(
                    start=start,
                    end=end,
                    interval="1d",
                    auto_adjust=False,
                    actions=True,
                )
            else:
                return ticker.history(
                    period="max",
                    interval="1d",
                    auto_adjust=False,
                    actions=True,
                )

        df = await self._execute_yf_call(_call)
        if df is None or df.empty:
            return []

        records: list[Record] = []
        local_ts = time.time_ns()

        for idx, row in df.iterrows():
            # Index is typically a DatetimeIndex
            if not isinstance(idx, datetime):
                # Fallback conversion
                ts_val = pd.to_datetime(idx)
            else:
                ts_val = idx

            # Convert to UTC epoch nanoseconds
            source_ts = int(ts_val.timestamp() * 1e9)
            date_str = ts_val.strftime("%Y-%m-%d")

            # Map the Bar record
            bar = Bar(
                provider="yahoo",
                symbol=symbol,
                symbol_raw=symbol,
                source_ts=source_ts,
                local_ts=local_ts,
                interval="1d",
                open=float(row["Open"]),
                high=float(row["High"]),
                low=float(row["Low"]),
                close=float(row["Close"]),
                volume=float(row["Volume"]),
                vwap=None,
                trade_count=None,
            )
            records.append(bar)

            # Map dividends
            if "Dividends" in row:
                div_val = float(row["Dividends"])
                if div_val > 0.0:
                    div = CorporateAction(
                        provider="yahoo",
                        symbol=symbol,
                        symbol_raw=symbol,
                        source_ts=source_ts,
                        local_ts=local_ts,
                        ex_date=date_str,
                        type=CorpActionType.DIVIDEND_CASH,
                        value=div_val,
                    )
                    records.append(div)

            # Map stock splits
            if "Stock Splits" in row:
                split_val = float(row["Stock Splits"])
                if split_val > 0.0:
                    split = CorporateAction(
                        provider="yahoo",
                        symbol=symbol,
                        symbol_raw=symbol,
                        source_ts=source_ts,
                        local_ts=local_ts,
                        ex_date=date_str,
                        type=CorpActionType.SPLIT,
                        value=split_val,
                    )
                    records.append(split)

        return records

    async def fetch_intraday_bars(
        self,
        symbol: str,
        interval: str,
        start: str | None = None,
        end: str | None = None,
    ) -> list[Bar]:
        """Fetch intraday historical bars.

        Args:
            symbol: Ticker symbol (e.g. AAPL)
            interval: Bar resolution (e.g. 1m, 5m, 1h)
            start: Start date string (YYYY-MM-DD)
            end: End date string (YYYY-MM-DD)

        Returns:
            A list of Bar records.
        """

        def _call(session: requests.Session) -> pd.DataFrame:
            ticker = yf.Ticker(symbol, session=session)
            if start:
                return ticker.history(
                    start=start,
                    end=end,
                    interval=interval,
                    auto_adjust=False,
                    actions=False,
                )
            else:
                # If no start date, fetch maximum window allowed by Yahoo for that interval
                # e.g., 1m has 7 days limit, 5m has 60 days.
                period = "7d" if interval == "1m" else "60d"
                return ticker.history(
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    actions=False,
                )

        df = await self._execute_yf_call(_call)
        if df is None or df.empty:
            return []

        records: list[Bar] = []
        local_ts = time.time_ns()

        for idx, row in df.iterrows():
            if not isinstance(idx, datetime):
                ts_val = pd.to_datetime(idx)
            else:
                ts_val = idx

            source_ts = int(ts_val.timestamp() * 1e9)

            bar = Bar(
                provider="yahoo",
                symbol=symbol,
                symbol_raw=symbol,
                source_ts=source_ts,
                local_ts=local_ts,
                interval=interval,
                open=float(row["Open"]),
                high=float(row["High"]),
                low=float(row["Low"]),
                close=float(row["Close"]),
                volume=float(row["Volume"]),
                vwap=None,
                trade_count=None,
            )
            records.append(bar)

        return records

    async def fetch_option_chain(
        self,
        symbol: str,
        expiry: str | None = None,
    ) -> list[OptionQuote]:
        """Fetch option chain quotes.

        Args:
            symbol: Underlying symbol (e.g. AAPL)
            expiry: Optional specific expiration date string (YYYY-MM-DD).
                    If None, fetches options for all available expirations.

        Returns:
            A list of OptionQuote records.
        """
        if expiry:
            expirations = (expiry,)
        else:

            def _get_exp(session: requests.Session) -> tuple[str, ...]:
                opts = yf.Ticker(symbol, session=session).options
                if not isinstance(opts, (list, tuple)):
                    return ()
                return tuple(str(x) for x in opts)

            try:
                expirations = await self._execute_yf_call(_get_exp)
            except Exception as e:
                logger.error("Failed to fetch option expirations list for %s: %s", symbol, e)
                return []

        if not expirations:
            return []

        records: list[OptionQuote] = []
        local_ts = time.time_ns()

        for exp in expirations:

            def _get_chain(session: requests.Session, current_exp: str = exp) -> Any:
                return yf.Ticker(symbol, session=session).option_chain(current_exp)

            try:
                chain = await self._execute_yf_call(_get_chain)
            except Exception as e:
                logger.error("Failed to fetch option chain for %s expiry %s: %s", symbol, exp, e)
                continue

            for opt_type, df in (("calls", chain.calls), ("puts", chain.puts)):
                opt_enum = OptType.C if opt_type == "calls" else OptType.P
                for _, row in df.iterrows():
                    contract_symbol = str(row["contractSymbol"])

                    # Parse lastTradeDate if present
                    source_ts = None
                    if "lastTradeDate" in row and not pd.isna(row["lastTradeDate"]):
                        ts_val = pd.to_datetime(row["lastTradeDate"])
                        source_ts = int(ts_val.timestamp() * 1e9)

                    quote = OptionQuote(
                        provider="yahoo",
                        symbol=contract_symbol,
                        symbol_raw=contract_symbol,
                        source_ts=source_ts,
                        local_ts=local_ts,
                        underlying=symbol,
                        expiry=exp,
                        strike=float(row["strike"]),
                        type=opt_enum,
                        bid=_clean_float(row.get("bid")),
                        ask=_clean_float(row.get("ask")),
                        last=_clean_float(row.get("lastPrice")),
                        volume=_clean_float(row.get("volume")),
                        open_interest=_clean_float(row.get("openInterest")),
                        implied_volatility=_clean_float(row.get("impliedVolatility")),
                        delta=None,
                        gamma=None,
                        vega=None,
                        theta=None,
                        rho=None,
                    )
                    records.append(quote)

        return records

    async def fetch_financial_statements(self, symbol: str) -> list[Fundamental]:
        """Fetch annual and quarterly income statement, balance sheet, and cash flow.

        Maps financial statements to Fundamental records.

        Args:
            symbol: Ticker symbol (e.g. AAPL)

        Returns:
            A list of Fundamental records.
        """

        def _call_financials(session: requests.Session) -> dict[str, pd.DataFrame]:
            ticker = yf.Ticker(symbol, session=session)
            return {
                "annual_financials": ticker.financials,
                "quarterly_financials": ticker.quarterly_financials,
                "annual_balance_sheet": ticker.balance_sheet,
                "quarterly_balance_sheet": ticker.quarterly_balance_sheet,
                "annual_cashflow": ticker.cashflow,
                "quarterly_cashflow": ticker.quarterly_cashflow,
            }

        try:
            statements = await self._execute_yf_call(_call_financials)
        except Exception as e:
            logger.error("Failed to fetch financials for %s: %s", symbol, e)
            return []

        records: list[Fundamental] = []
        local_ts = time.time_ns()

        # Config mapping for parsing statements
        config = [
            ("annual_financials", "FY", "10-K", True),
            ("quarterly_financials", None, "10-Q", True),
            ("annual_balance_sheet", "FY", "10-K", False),
            ("quarterly_balance_sheet", None, "10-Q", False),
            ("annual_cashflow", "FY", "10-K", True),
            ("quarterly_cashflow", None, "10-Q", True),
        ]

        for name, fp_val, form, is_duration in config:
            df = statements.get(name)
            if df is None or df.empty:
                continue

            for tag, row in df.iterrows():
                tag_str = str(tag)
                for col_date, val in row.items():
                    val_float = _clean_float(val)
                    if val_float is None:
                        continue

                    # Parse date column
                    ts = pd.to_datetime(col_date)
                    end_str = ts.strftime("%Y-%m-%d")

                    # Period end date timestamp as source_ts
                    source_ts = int(ts.timestamp() * 1e9)

                    # Determine fiscal period for quarter if None
                    fp = fp_val
                    if fp is None:
                        # Calendar approximation for Q1-Q4 based on month
                        m = ts.month
                        if m in (1, 2, 3):
                            fp = "Q1"
                        elif m in (4, 5, 6):
                            fp = "Q2"
                        elif m in (7, 8, 9):
                            fp = "Q3"
                        else:
                            fp = "Q4"

                    # Calculate period start for duration statements
                    start_str = None
                    if is_duration:
                        if fp == "FY":
                            # Approx 1 year prior
                            start_str = (ts - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
                        else:
                            # Approx 3 months prior
                            start_str = (ts - pd.DateOffset(months=3)).strftime("%Y-%m-%d")

                    fundamental = Fundamental(
                        provider="yahoo",
                        symbol=symbol,
                        symbol_raw=symbol,
                        source_ts=source_ts,
                        local_ts=local_ts,
                        taxonomy="yahoo",
                        tag=tag_str,
                        unit="USD",
                        val=val_float,
                        end=end_str,
                        start=start_str,
                        fy=int(ts.year),
                        fp=FundPeriod(fp) if fp else None,
                        form=form,
                        filed=None,
                        accn=None,
                        frame=None,
                    )
                    records.append(fundamental)

        return records

    async def fetch_insider_transactions(self, symbol: str) -> list[InsiderTransaction]:
        """Fetch insider transactions.

        Args:
            symbol: Ticker symbol (e.g. AAPL)

        Returns:
            A list of InsiderTransaction records.
        """

        def _call(session: requests.Session) -> pd.DataFrame | None:
            ticker = yf.Ticker(symbol, session=session)
            return ticker.insider_transactions

        df = await self._execute_yf_call(_call)
        if df is None or df.empty:
            return []

        records: list[InsiderTransaction] = []
        local_ts = time.time_ns()

        for _, row in df.iterrows():
            insider_name = _clean_str(row.get("Insider"))
            transaction_type = _clean_str(row.get("Transaction"))
            position = _clean_str(row.get("Position"))
            date_val = row.get("Start Date")

            if not insider_name or not transaction_type or pd.isna(date_val):
                continue

            ts = pd.to_datetime(date_val)
            date_str = ts.strftime("%Y-%m-%d")

            shares = _clean_float(row.get("Shares"))
            value = _clean_float(row.get("Value"))
            ownership = _clean_str(row.get("Ownership"))

            # Calculate price if possible
            price = None
            if value is not None and shares is not None and shares > 0.0:
                price = value / shares

            txn = InsiderTransaction(
                provider="yahoo",
                symbol=symbol,
                symbol_raw=symbol,
                source_ts=None,
                local_ts=local_ts,
                insider_name=insider_name,
                position=position or "Unknown",
                transaction_type=transaction_type,
                transaction_date=date_str,
                shares=shares,
                price=price,
                value=value,
                ownership=ownership,
            )
            records.append(txn)

        return records
