"""Tests for the Tiingo provider."""

import io
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import msgspec
import pytest

from stockodile.providers.tiingo.client import (
    TiingoClient,
    TiingoError,
    TiingoQuotaError,
    TiingoRateLimitError,
)
from stockodile.schema.enums import CorpActionType
from stockodile.schema.records import Bar, CorporateAction


@pytest.fixture
def temp_tracker_path(tmp_path: Path) -> Path:
    """Fixture providing a temporary path for the quota tracker."""
    return tmp_path / "tiingo_tracker.json"


@pytest.fixture
def mock_session() -> MagicMock:
    """Fixture providing a mocked aiohttp ClientSession."""
    session = MagicMock(spec=aiohttp.ClientSession)
    session.closed = False
    return session


def create_mock_response(status: int, data: bytes) -> AsyncMock:
    """Helper to create a mocked aiohttp client response."""
    response = AsyncMock()
    response.status = status
    response.read = AsyncMock(return_value=data)
    response.text = AsyncMock(return_value=data.decode("utf-8", errors="replace"))
    return response


@pytest.mark.asyncio
async def test_download_supported_tickers(mock_session: MagicMock, temp_tracker_path: Path) -> None:
    # 1. Create a dummy zip containing a csv
    csv_content = (
        "ticker,exchange,assetType,priceCurrency,startDate,endDate\n"
        "AAPL,NASDAQ,CS,USD,1980-12-12,2026-06-18\n"
        "MSFT,NASDAQ,CS,USD,1986-03-13,2026-06-18\n"
    )
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("supported_tickers.csv", csv_content)
    zip_bytes = zip_buffer.getvalue()

    # 2. Mock session.get
    mock_resp = create_mock_response(200, zip_bytes)
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    async with TiingoClient(
        "test_api_key", tracker_path=temp_tracker_path, session=mock_session
    ) as client:
        tickers = await client.download_supported_tickers()

    assert len(tickers) == 2
    assert tickers[0].ticker == "AAPL"
    assert tickers[0].exchange == "NASDAQ"
    assert tickers[0].asset_type == "CS"
    assert tickers[0].price_currency == "USD"
    assert tickers[0].start_date == "1980-12-12"
    assert tickers[0].end_date == "2026-06-18"

    assert tickers[1].ticker == "MSFT"


@pytest.mark.asyncio
async def test_get_eod_prices(mock_session: MagicMock, temp_tracker_path: Path) -> None:
    # Mock data with split (splitFactor != 1.0) and dividend (divCash > 0)
    eod_json = [
        {
            "date": "2026-06-19T00:00:00.000Z",
            "close": 180.2,
            "high": 182.0,
            "low": 179.5,
            "open": 180.0,
            "volume": 50000000.0,
            "adjClose": 180.2,
            "adjHigh": 182.0,
            "adjLow": 179.5,
            "adjOpen": 180.0,
            "adjVolume": 50000000.0,
            "divCash": 0.25,
            "splitFactor": 1.0,
        },
        {
            "date": "2026-06-20T00:00:00.000Z",
            "close": 185.0,
            "high": 186.0,
            "low": 184.0,
            "open": 184.5,
            "volume": 60000000.0,
            "adjClose": 185.0,
            "adjHigh": 186.0,
            "adjLow": 184.0,
            "adjOpen": 184.5,
            "adjVolume": 60000000.0,
            "divCash": 0.0,
            "splitFactor": 2.0,
        },
    ]
    raw_bytes = msgspec.json.encode(eod_json)
    mock_resp = create_mock_response(200, raw_bytes)
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    async with TiingoClient(
        "test_api_key", tracker_path=temp_tracker_path, session=mock_session
    ) as client:
        records = await client.get_eod_prices(
            "AAPL", start_date="2026-06-19", end_date="2026-06-20"
        )

    # Expecting: 2 Bar records, 1 Dividend action (date 19), 1 Split action (date 20)
    # Total = 4 records
    assert len(records) == 4

    bars = [r for r in records if isinstance(r, Bar)]
    corp_actions = [r for r in records if isinstance(r, CorporateAction)]

    assert len(bars) == 2
    assert len(corp_actions) == 2

    # Verify Bar
    assert bars[0].symbol == "AAPL"
    assert bars[0].provider == "tiingo"
    assert bars[0].interval == "1d"
    assert bars[0].open == 180.0
    assert bars[0].close == 180.2
    assert bars[0].source_ts == 1781827200000000000  # 2026-06-19T00:00:00Z in ns

    # Verify CorporateActions
    div_action = next(c for c in corp_actions if c.type == CorpActionType.DIVIDEND_CASH)
    assert div_action.value == 0.25
    assert div_action.ex_date == "2026-06-19"

    split_action = next(c for c in corp_actions if c.type == CorpActionType.SPLIT)
    assert split_action.value == 2.0
    assert split_action.ex_date == "2026-06-20"


@pytest.mark.asyncio
async def test_get_intraday_bars(mock_session: MagicMock, temp_tracker_path: Path) -> None:
    iex_json = [
        {
            "date": "2026-06-19T13:30:00.000Z",
            "open": 180.0,
            "high": 180.5,
            "low": 179.8,
            "close": 180.2,
            "volume": 12000.0,
        }
    ]
    raw_bytes = msgspec.json.encode(iex_json)
    mock_resp = create_mock_response(200, raw_bytes)
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    async with TiingoClient(
        "test_api_key", tracker_path=temp_tracker_path, session=mock_session
    ) as client:
        bars = await client.get_intraday_bars(
            "AAPL", start_date="2026-06-19", resample_freq="15min"
        )

    assert len(bars) == 1
    bar = bars[0]
    assert bar.symbol == "AAPL"
    assert bar.interval == "15min"
    assert bar.open == 180.0
    assert bar.close == 180.2
    assert bar.volume == 12000.0
    assert bar.source_ts == 1781875800000000000  # 2026-06-19T13:30:00Z in ns


@pytest.mark.asyncio
async def test_error_handling(mock_session: MagicMock, temp_tracker_path: Path) -> None:
    # Test 429 Rate Limit
    mock_resp = create_mock_response(429, b"Rate limit exceeded")
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    async with TiingoClient(
        "test_api_key", tracker_path=temp_tracker_path, session=mock_session
    ) as client:
        with pytest.raises(TiingoRateLimitError):
            await client.get_eod_prices("AAPL")

    # Test other HTTP error
    mock_resp = create_mock_response(500, b"Internal server error")
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    async with TiingoClient(
        "test_api_key", tracker_path=temp_tracker_path, session=mock_session
    ) as client:
        with pytest.raises(TiingoError):
            await client.get_eod_prices("AAPL")


@pytest.mark.asyncio
async def test_quota_tracker_limits(mock_session: MagicMock, temp_tracker_path: Path) -> None:
    mock_resp = create_mock_response(200, b"[]")
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    async with TiingoClient(
        "test_api_key", tracker_path=temp_tracker_path, session=mock_session
    ) as client:
        # Pre-populate quota to 499 symbols
        state = {
            "month": client.tracker._get_current_month(),
            "symbols": [f"SYM{i}" for i in range(499)],
        }
        temp_tracker_path.write_bytes(msgspec.json.encode(state))  # noqa: ASYNC240

        # Apple should succeed (adds 500th symbol)
        await client.get_eod_prices("AAPL")

        # Microsoft should fail (exceeds 500 unique symbols)
        with pytest.raises(TiingoQuotaError):
            await client.get_eod_prices("MSFT")

        # Apple should still succeed because it is already in the monthly list
        await client.get_eod_prices("AAPL")


@pytest.mark.asyncio
async def test_quota_tracker_reset_on_new_month(
    mock_session: MagicMock, temp_tracker_path: Path
) -> None:
    mock_resp = create_mock_response(200, b"[]")
    mock_session.get.return_value.__aenter__.return_value = mock_resp

    async with TiingoClient(
        "test_api_key", tracker_path=temp_tracker_path, session=mock_session
    ) as client:
        # Pre-populate quota to 500 symbols for a past month
        state = {
            "month": "2020-01",
            "symbols": [f"SYM{i}" for i in range(500)],
        }
        temp_tracker_path.write_bytes(msgspec.json.encode(state))  # noqa: ASYNC240

        # AAPL query should reset tracker to current month and succeed
        await client.get_eod_prices("AAPL")

        current_state = msgspec.json.decode(
            temp_tracker_path.read_bytes()  # noqa: ASYNC240
        )
        assert current_state["month"] == client.tracker._get_current_month()
        assert current_state["symbols"] == ["AAPL"]
