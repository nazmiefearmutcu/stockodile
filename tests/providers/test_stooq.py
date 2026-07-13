from __future__ import annotations

import os
import tempfile
import zipfile
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from stockodile.providers.stooq.connector import StooqProvider
from stockodile.reference.registry import InstrumentRegistry
from stockodile.schema.enums import SecurityType
from stockodile.schema.records import OHLCV, IndexValue
from stockodile.sink.base import MemorySink


def test_stooq_provider_init() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()

    provider = StooqProvider(
        symbols=["AAPL.US", "^SPX"],
        channels=["ohlcv", "index_value"],
        out=sink,
        registry=registry,
        zip_path="/mock/path/stooq.zip",
        captcha_api_key="mock_captcha_key",
        captcha_service="2captcha",
        domain="stooq.com",
    )

    assert provider.zip_path == "/mock/path/stooq.zip"
    assert provider.captcha_api_key == "mock_captcha_key"
    assert provider.captcha_service == "2captcha"
    assert provider.domain == "stooq.com"
    assert provider.symbols == ["AAPL.US", "^SPX"]


@pytest.mark.asyncio
async def test_stooq_list_instruments() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()

    provider = StooqProvider(
        symbols=["AAPL.US", "^SPX"],
        channels=["ohlcv", "index_value"],
        out=sink,
        registry=registry,
    )

    insts = await provider.list_instruments()
    assert len(insts) == 2
    assert insts[0].symbol == "AAPL.US"
    assert insts[0].security_type == SecurityType.CS
    assert insts[1].symbol == "^SPX"
    assert insts[1].security_type == SecurityType.UNKNOWN


@pytest.mark.asyncio
async def test_stooq_no_ops() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()

    provider = StooqProvider(
        symbols=["AAPL.US"],
        channels=["ohlcv"],
        out=sink,
        registry=registry,
    )

    await provider._subscribe(None)
    assert list(provider.normalize({}, 0)) == []


@pytest.mark.asyncio
async def test_stooq_backfill_from_zip() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()

    # Create a temporary dummy ZIP file
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "d_world_txt.zip")
        with zipfile.ZipFile(zip_path, "w") as z:
            # Write a dummy index CSV file inside zip
            index_content = (
                "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>\n"
                "^SPX,D,20260620,000000,5000.0,5050.0,4990.0,5020.0,1000000,0\n"
                "^SPX,D,20260621,000000,5025.0,5060.0,5010.0,5040.0,1100000,0\n"
            )
            z.writestr("data/daily/world/indices/^spx.txt", index_content)

            # Write a dummy stock CSV file inside zip
            stock_content = (
                "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>\n"
                "AAPL.US,D,20260620,000000,180.0,182.0,179.0,181.0,5000000,0\n"
                "AAPL.US,D,20260621,000000,181.5,183.0,180.5,182.5,5200000,0\n"
            )
            z.writestr("data/daily/us/nasdaq stocks/aapl.us.txt", stock_content)

        provider = StooqProvider(
            symbols=["AAPL.US", "^SPX"],
            channels=["ohlcv", "index_value"],
            out=sink,
            registry=registry,
            zip_path=zip_path,
        )

        # Backfill index symbol
        start_dt = datetime(2026, 6, 20, tzinfo=UTC)
        end_dt = datetime(2026, 6, 21, tzinfo=UTC)
        start_ns = int(start_dt.timestamp()) * 1_000_000_000
        end_ns = int(end_dt.timestamp()) * 1_000_000_000

        index_records = []
        async for rec in provider.backfill("index_value", "^SPX", start_ns, end_ns):
            index_records.append(rec)

        assert len(index_records) == 2
        rec1 = index_records[0]
        assert isinstance(rec1, IndexValue)
        assert rec1.symbol == "^SPX"
        assert rec1.value == 5020.0
        assert rec1.source_ts == start_ns

        # Backfill stock symbol
        stock_records = []
        async for rec in provider.backfill("ohlcv", "AAPL.US", start_ns, end_ns):
            stock_records.append(rec)

        assert len(stock_records) == 2
        srec1 = stock_records[0]
        assert isinstance(srec1, OHLCV)
        assert srec1.symbol == "AAPL.US"
        assert srec1.open == 180.0
        assert srec1.high == 182.0
        assert srec1.low == 179.0
        assert srec1.close == 181.0
        assert srec1.volume == 5000000.0
        assert srec1.source_ts == start_ns


@pytest.mark.asyncio
async def test_stooq_captcha_api_solvers() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()

    provider = StooqProvider(
        symbols=["AAPL.US"],
        channels=["ohlcv"],
        out=sink,
        registry=registry,
        captcha_api_key="test_key",
    )

    # Mock responses for 2captcha
    mock_post_resp = MagicMock()
    mock_post_resp.status = 200
    mock_post_resp.json = AsyncMock(return_value={"status": 1, "request": "mock_task_id"})

    mock_get_resp = MagicMock()
    mock_get_resp.status = 200
    mock_get_resp.json = AsyncMock(return_value={"status": 1, "request": "solved_code_123"})

    class MockClientSession2Captcha:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.closed = False

        async def __aenter__(self) -> MockClientSession2Captcha:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        def post(self, *args: Any, **kwargs: Any) -> Any:
            return mock_post_resp

        def get(self, *args: Any, **kwargs: Any) -> Any:
            return mock_get_resp

    mock_post_resp.__aenter__ = AsyncMock(return_value=mock_post_resp)
    mock_post_resp.__aexit__ = AsyncMock(return_value=None)
    mock_get_resp.__aenter__ = AsyncMock(return_value=mock_get_resp)
    mock_get_resp.__aexit__ = AsyncMock(return_value=None)

    with patch("aiohttp.ClientSession", MockClientSession2Captcha):
        code = await provider.solve_captcha_2captcha("test_key", b"dummy_img")
        assert code == "solved_code_123"

    # Reset cached session so the new patch works
    provider.session = None

    # Mock responses for anticaptcha
    mock_anti_post_resp1 = MagicMock()
    mock_anti_post_resp1.status = 200
    mock_anti_post_resp1.json = AsyncMock(return_value={"errorId": 0, "taskId": 999})
    mock_anti_post_resp2 = MagicMock()
    mock_anti_post_resp2.status = 200
    mock_anti_post_resp2.json = AsyncMock(
        return_value={
            "errorId": 0,
            "status": "ready",
            "solution": {"text": "solved_anti_456"},
        }
    )

    class MockClientSessionAntiCaptcha:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.closed = False
            self._call_count = 0

        async def __aenter__(self) -> MockClientSessionAntiCaptcha:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        def post(self, *args: Any, **kwargs: Any) -> Any:
            if self._call_count == 0:
                self._call_count += 1
                return mock_anti_post_resp1
            return mock_anti_post_resp2

    mock_anti_post_resp1.__aenter__ = AsyncMock(return_value=mock_anti_post_resp1)
    mock_anti_post_resp1.__aexit__ = AsyncMock(return_value=None)
    mock_anti_post_resp2.__aenter__ = AsyncMock(return_value=mock_anti_post_resp2)
    mock_anti_post_resp2.__aexit__ = AsyncMock(return_value=None)

    with patch("aiohttp.ClientSession", MockClientSessionAntiCaptcha):
        code = await provider.solve_captcha_anticaptcha("test_key", b"dummy_img")
        assert code == "solved_anti_456"


def test_parse_csv_rest_format_yyyy_mm_dd() -> None:
    """REST downloads use Date,Open,... with yyyy-MM-dd (not bulk ZIP layout)."""
    from stockodile.providers.stooq.connector import StooqProvider
    from stockodile.schema.records import OHLCV

    p = object.__new__(StooqProvider)
    p.name = "stooq"
    csv = b"Date,Open,High,Low,Close,Volume\n2026-06-20,180.0,182.0,179.0,181.0,5000000\n"
    recs = StooqProvider._parse_csv(p, csv, "AAPL.US", 0, 2 * 10**18)
    assert len(recs) == 1
    assert isinstance(recs[0], OHLCV)
    assert recs[0].close == 181.0
    assert recs[0].open == 180.0


def test_parse_csv_bulk_zip_angle_headers() -> None:
    from stockodile.providers.stooq.connector import StooqProvider
    from stockodile.schema.records import OHLCV

    p = object.__new__(StooqProvider)
    p.name = "stooq"
    csv = b"<DATE>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>\n20260620,180.0,182.0,179.0,181.0,5000000\n"
    recs = StooqProvider._parse_csv(p, csv, "AAPL.US", 0, 2 * 10**18)
    assert len(recs) == 1
    assert isinstance(recs[0], OHLCV)
