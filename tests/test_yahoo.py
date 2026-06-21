"""Unit tests for the Yahoo Finance provider client."""

from datetime import UTC
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from yfinance.exceptions import YFRateLimitError

from stockodile.providers.yahoo.client import YahooClient
from stockodile.schema.enums import CorpActionType, OptType
from stockodile.schema.records import (
    Bar,
    CorporateAction,
    Fundamental,
    InsiderTransaction,
    OptionQuote,
)


@pytest.fixture
def mock_history_df() -> pd.DataFrame:
    """Fixture for historical bars data."""
    dates = pd.to_datetime(["2026-06-15", "2026-06-16"])
    data = {
        "Open": [100.0, 102.0],
        "High": [105.0, 103.0],
        "Low": [99.0, 101.0],
        "Close": [102.0, 102.5],
        "Volume": [1000.0, 1500.0],
        "Dividends": [0.5, 0.0],
        "Stock Splits": [0.0, 2.0],
    }
    return pd.DataFrame(data, index=dates)


@pytest.fixture
def mock_option_chain() -> MagicMock:
    """Fixture for option chain object."""
    chain = MagicMock()

    calls_data = {
        "contractSymbol": ["AAPL260626C00150000"],
        "lastTradeDate": [pd.Timestamp("2026-06-19 19:00:00", tz=UTC)],
        "strike": [150.0],
        "lastPrice": [5.0],
        "bid": [4.8],
        "ask": [5.2],
        "volume": [100.0],
        "openInterest": [500.0],
        "impliedVolatility": [0.25],
    }
    chain.calls = pd.DataFrame(calls_data)

    puts_data = {
        "contractSymbol": ["AAPL260626P00150000"],
        "lastTradeDate": [pd.Timestamp("2026-06-19 19:05:00", tz=UTC)],
        "strike": [150.0],
        "lastPrice": [3.0],
        "bid": [2.9],
        "ask": [3.1],
        "volume": [80.0],
        "openInterest": [400.0],
        "impliedVolatility": [0.22],
    }
    chain.puts = pd.DataFrame(puts_data)

    return chain


@pytest.fixture
def mock_financials() -> pd.DataFrame:
    """Fixture for financials statements."""
    dates = pd.to_datetime(["2025-12-31"])
    data = {
        dates[0]: [1000000.0],
    }
    return pd.DataFrame(data, index=["Total Revenue"])


@pytest.fixture
def mock_insider_df() -> pd.DataFrame:
    """Fixture for insider transactions DataFrame."""
    data = {
        "Insider": ["John Doe"],
        "Position": ["CEO"],
        "Transaction": ["Buy"],
        "Start Date": ["2026-06-16"],
        "Shares": [100.0],
        "Value": [10000.0],
        "Ownership": ["D"],
    }
    return pd.DataFrame(data)


@pytest.mark.asyncio
async def test_fetch_eod_history(mock_history_df: pd.DataFrame) -> None:
    """Test fetching EOD history produces correct Bar and CorporateAction records."""
    client = YahooClient()

    with patch("yfinance.Ticker") as mock_ticker_cls:
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = mock_history_df
        mock_ticker_cls.return_value = mock_ticker

        records = await client.fetch_eod_history("AAPL")

        # Two days of history -> 2 Bars, 1 Dividend, 1 Split = 4 records
        assert len(records) == 4

        bars = [r for r in records if isinstance(r, Bar)]
        corp_actions = [r for r in records if isinstance(r, CorporateAction)]

        assert len(bars) == 2
        assert len(corp_actions) == 2

        assert bars[0].symbol == "AAPL"
        assert bars[0].open == 100.0
        assert bars[0].close == 102.0
        assert bars[0].volume == 1000.0

        div = next(c for c in corp_actions if c.type == CorpActionType.DIVIDEND_CASH)
        assert div.ex_date == "2026-06-15"
        assert div.value == 0.5

        split = next(c for c in corp_actions if c.type == CorpActionType.SPLIT)
        assert split.ex_date == "2026-06-16"
        assert split.value == 2.0

    await client.close()


@pytest.mark.asyncio
async def test_fetch_intraday_bars(mock_history_df: pd.DataFrame) -> None:
    """Test fetching intraday bars produces correct Bar records."""
    client = YahooClient()

    with patch("yfinance.Ticker") as mock_ticker_cls:
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = mock_history_df
        mock_ticker_cls.return_value = mock_ticker

        records = await client.fetch_intraday_bars("AAPL", interval="1m")

        assert len(records) == 2
        assert all(isinstance(r, Bar) for r in records)
        assert records[0].open == 100.0
        assert records[1].close == 102.5

    await client.close()


@pytest.mark.asyncio
async def test_fetch_option_chain(mock_option_chain: MagicMock) -> None:
    """Test fetching option chains produces correct OptionQuote records."""
    client = YahooClient()

    with patch("yfinance.Ticker") as mock_ticker_cls:
        mock_ticker = MagicMock()
        mock_ticker.options = ("2026-06-26",)
        mock_ticker.option_chain.return_value = mock_option_chain
        mock_ticker_cls.return_value = mock_ticker

        records = await client.fetch_option_chain("AAPL")

        assert len(records) == 2
        assert all(isinstance(r, OptionQuote) for r in records)

        call = next(r for r in records if r.type == OptType.C)
        assert call.symbol == "AAPL260626C00150000"
        assert call.strike == 150.0
        assert call.last == 5.0
        assert call.bid == 4.8
        assert call.ask == 5.2
        assert call.implied_volatility == 0.25

        put = next(r for r in records if r.type == OptType.P)
        assert put.symbol == "AAPL260626P00150000"
        assert put.last == 3.0
        assert put.implied_volatility == 0.22

    await client.close()


@pytest.mark.asyncio
async def test_fetch_financial_statements(mock_financials: pd.DataFrame) -> None:
    """Test fetching financials produces Fundamental records."""
    client = YahooClient()

    with patch("yfinance.Ticker") as mock_ticker_cls:
        mock_ticker = MagicMock()
        mock_ticker.financials = mock_financials
        mock_ticker.quarterly_financials = pd.DataFrame()
        mock_ticker.balance_sheet = pd.DataFrame()
        mock_ticker.quarterly_balance_sheet = pd.DataFrame()
        mock_ticker.cashflow = pd.DataFrame()
        mock_ticker.quarterly_cashflow = pd.DataFrame()
        mock_ticker_cls.return_value = mock_ticker

        records = await client.fetch_financial_statements("AAPL")

        assert len(records) == 1
        fund = records[0]
        assert isinstance(fund, Fundamental)
        assert fund.symbol == "AAPL"
        assert fund.tag == "Total Revenue"
        assert fund.val == 1000000.0
        assert fund.end == "2025-12-31"
        assert fund.start == "2024-12-31"  # Derived 1 year back for annual duration
        assert fund.fp == "FY"
        assert fund.form == "10-K"

    await client.close()


@pytest.mark.asyncio
async def test_fetch_insider_transactions(mock_insider_df: pd.DataFrame) -> None:
    """Test fetching insider transactions produces correct InsiderTransaction records."""
    client = YahooClient()

    with patch("yfinance.Ticker") as mock_ticker_cls:
        mock_ticker = MagicMock()
        mock_ticker.insider_transactions = mock_insider_df
        mock_ticker_cls.return_value = mock_ticker

        records = await client.fetch_insider_transactions("AAPL")

        assert len(records) == 1
        txn = records[0]
        assert isinstance(txn, InsiderTransaction)
        assert txn.insider_name == "John Doe"
        assert txn.position == "CEO"
        assert txn.transaction_type == "Buy"
        assert txn.transaction_date == "2026-06-16"
        assert txn.shares == 100.0
        assert txn.value == 10000.0
        assert txn.price == 100.0  # Computed value / shares
        assert txn.ownership == "D"

    await client.close()


@pytest.mark.asyncio
async def test_rate_limiting_and_session_reset() -> None:
    """Test that RateLimitError triggers backoff and resets requests session."""
    client = YahooClient(backoff_delay=0.01)

    call_count = 0
    original_session = client.session

    def flaky_call(*args: Any, **kwargs: Any) -> pd.DataFrame:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise YFRateLimitError()
        return pd.DataFrame()

    with patch("yfinance.Ticker") as mock_ticker_cls:
        mock_ticker = MagicMock()
        mock_ticker.history = flaky_call
        mock_ticker_cls.return_value = mock_ticker

        # This should hit rate limit on first call, trigger reset, then succeed on second call
        records = await client.fetch_intraday_bars("AAPL", "1d")

        assert call_count == 2
        assert len(records) == 0
        assert client.session is not original_session

    await client.close()
