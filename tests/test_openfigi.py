"""Tests for OpenFIGI provider implementation."""

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import aiohttp
import msgspec
import pytest

from stockodile.providers.openfigi.cache import InMemoryCache, SQLiteCache
from stockodile.providers.openfigi.client import OpenFigiClient
from stockodile.providers.openfigi.models import (
    FigiRecord,
    OpenFigiJob,
    OpenFigiResponseItem,
    OpenFigiResult,
)


class MockResponse:
    """Mock aiohttp client response context manager."""

    def __init__(
        self, status: int, body: bytes, headers: dict[str, str] | None = None
    ) -> None:
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def read(self) -> bytes:
        return self._body

    async def __aenter__(self) -> "MockResponse":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


@pytest.mark.asyncio
async def test_in_memory_cache() -> None:
    """Test in-memory cache functionality."""
    cache = InMemoryCache()
    job = OpenFigiJob(id_type="TICKER", id_value="AAPL", exch_code="US")

    assert await cache.get(job) is None

    record = FigiRecord(figi="BBG000B9XVV8", ticker="AAPL", exch_code="US")
    await cache.set(job, [record])

    cached = await cache.get(job)
    assert cached == [record]


@pytest.mark.asyncio
async def test_sqlite_cache() -> None:
    """Test persistent SQLite-based cache functionality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_cache.db"
        cache = SQLiteCache(db_path)
        job = OpenFigiJob(id_type="TICKER", id_value="MSFT", exch_code="US")

        assert await cache.get(job) is None

        record = FigiRecord(figi="BBG000BPH4D1", ticker="MSFT", exch_code="US")
        await cache.set(job, [record])

        # Test retrieval from same cache instance
        cached = await cache.get(job)
        assert cached == [record]

        # Test retrieval from a new cache instance pointing to same file
        cache2 = SQLiteCache(db_path)
        cached2 = await cache2.get(job)
        assert cached2 == [record]


@pytest.mark.asyncio
async def test_client_mapping_success() -> None:
    """Test successful OpenFIGI mapping call."""
    raw_result = OpenFigiResult(
        figi="BBG000BLNNH6",
        securityType="Common Stock",
        marketSector="Equity",
        ticker="IBM",
        name="INTL BUSINESS MACHINES CORP",
        exchCode="US",
    )
    raw_item = OpenFigiResponseItem(data=[raw_result])
    response_bytes = msgspec.json.encode([raw_item])

    session_mock = MagicMock(spec=aiohttp.ClientSession)
    session_mock.closed = False
    session_mock.post.return_value = MockResponse(200, response_bytes)

    job = OpenFigiJob(id_type="TICKER", id_value="IBM", exch_code="US")
    client = OpenFigiClient(session=session_mock)

    results = await client.map_jobs([job])
    assert len(results) == 1
    assert len(results[0]) == 1
    record = results[0][0]
    assert record.figi == "BBG000BLNNH6"
    assert record.ticker == "IBM"
    assert record.exch_code == "US"
    assert record.security_type == "Common Stock"

    # Verify job was cached
    cached = await client.cache.get(job)
    assert cached == [record]


@pytest.mark.asyncio
async def test_client_mapping_caching_first() -> None:
    """Test that client queries cache first and only requests missing jobs from API."""
    session_mock = MagicMock(spec=aiohttp.ClientSession)
    session_mock.closed = False

    client = OpenFigiClient(session=session_mock)

    # Pre-populate cache for AAPL
    job_cached = OpenFigiJob(id_type="TICKER", id_value="AAPL", exch_code="US")
    record = FigiRecord(figi="BBG000B9XVV8", ticker="AAPL", exch_code="US")
    await client.cache.set(job_cached, [record])

    # We query two jobs: AAPL (cached) and MSFT (not cached)
    job_new = OpenFigiJob(id_type="TICKER", id_value="MSFT", exch_code="US")

    raw_result = OpenFigiResult(figi="BBG000BPH4D1", ticker="MSFT", exchCode="US")
    raw_item = OpenFigiResponseItem(data=[raw_result])
    response_bytes = msgspec.json.encode([raw_item])
    session_mock.post.return_value = MockResponse(200, response_bytes)

    results = await client.map_jobs([job_cached, job_new])
    assert len(results) == 2
    assert results[0] == [record]
    assert results[1][0].figi == "BBG000BPH4D1"

    # Verify post was called with only the non-cached job (MSFT)
    session_mock.post.assert_called_once()
    _args, kwargs = session_mock.post.call_args
    assert len(kwargs["json"]) == 1
    assert kwargs["json"][0]["idValue"] == "MSFT"


@pytest.mark.asyncio
async def test_client_rate_limiting_429_retry() -> None:
    """Test that HTTP 429 response triggers backoff and retry."""
    session_mock = MagicMock(spec=aiohttp.ClientSession)
    session_mock.closed = False

    raw_result = OpenFigiResult(figi="BBG000BLNNH6", ticker="IBM", exchCode="US")
    response_bytes = msgspec.json.encode([OpenFigiResponseItem(data=[raw_result])])

    # First request returns 429, second returns 200
    mock_responses = [
        MockResponse(429, b"Too Many Requests", headers={"Retry-After": "0.1"}),
        MockResponse(200, response_bytes),
    ]

    class SequentialPost:
        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            res = mock_responses[self.calls]
            self.calls += 1
            return res

    session_mock.post.side_effect = SequentialPost()

    client = OpenFigiClient(session=session_mock)
    # Speed up refill rate so the test finishes immediately
    client.rate_limiter._refill_rate = 1000.0

    job = OpenFigiJob(id_type="TICKER", id_value="IBM", exch_code="US")
    results = await client.map_jobs([job])

    assert len(results) == 1
    assert results[0][0].figi == "BBG000BLNNH6"
    assert session_mock.post.call_count == 2
