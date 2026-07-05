"""Tests for the SEC EDGAR provider implementation."""

from __future__ import annotations

import json
import tempfile
import time
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from stockodile.providers.sec_edgar import SecEdgarClient
from stockodile.ratelimit import TokenBucketLimiter
from stockodile.schema.records import Filing, Fundamental


def test_normalize_cik() -> None:
    """Test CIK normalization logic."""
    assert SecEdgarClient.normalize_cik(320193) == "0000320193"
    assert SecEdgarClient.normalize_cik("320193") == "0000320193"
    assert SecEdgarClient.normalize_cik("CIK0000320193") == "0000320193"

    with pytest.raises(ValueError, match="Invalid CIK"):
        SecEdgarClient.normalize_cik("abc")


@pytest.mark.asyncio
async def test_token_bucket_limiter() -> None:
    """Test async TokenBucketLimiter rate limiting behavior."""
    limiter = TokenBucketLimiter(rate=100.0, capacity=2.0)

    # Acquire 2 tokens immediately
    start = time.monotonic()
    await limiter.acquire(1.0)
    await limiter.acquire(1.0)
    assert time.monotonic() - start < 0.05

    # Third token should block/sleep because capacity is 2
    await limiter.acquire(1.0)
    # At rate=100/s, 1 token takes 0.01s.
    assert time.monotonic() - start >= 0.008


@pytest.mark.asyncio
async def test_fetch_ticker_map() -> None:
    """Test fetching and building the ticker mapping."""
    mock_response = {
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
    }

    client = SecEdgarClient()
    with patch.object(client, "_request_json", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = mock_response

        await client.fetch_ticker_map()

        mock_req.assert_called_once_with("https://www.sec.gov/files/company_tickers.json")
        assert client._ticker_to_cik["AAPL"] == 320193
        assert client._ticker_to_cik["MSFT"] == 789019
        assert client._cik_to_primary_ticker[320193] == "AAPL"
        assert client._cik_to_primary_ticker[789019] == "MSFT"


@pytest.mark.asyncio
async def test_get_filings() -> None:
    """Test get_filings with mock data."""
    mock_submissions = {
        "cik": "0000320193",
        "entityName": "Apple Inc.",
        "tickers": ["AAPL"],
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-24-000006"],
                "form": ["10-Q"],
                "filingDate": ["2024-02-01"],
                "reportDate": ["2023-12-30"],
                "primaryDocument": ["aapl-20231230.htm"],
                "isXBRL": [1],
            },
            "files": [],
        },
    }

    client = SecEdgarClient()
    # Pre-populate map to avoid HTTP fetch
    client._ticker_to_cik["AAPL"] = 320193
    client._cik_to_primary_ticker[320193] = "AAPL"

    with patch.object(client, "fetch_submissions", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = mock_submissions

        filings = await client.get_filings("AAPL")

        mock_fetch.assert_called_once_with(320193)
        assert len(filings) == 1
        f = filings[0]
        assert isinstance(f, Filing)
        assert f.symbol == "AAPL"
        assert f.form == "10-Q"
        assert f.filing_date == "2024-02-01"
        assert f.report_date == "2023-12-30"
        assert f.accession_number == "0000320193-24-000006"
        assert (
            f.document_url
            == "https://www.sec.gov/Archives/edgar/data/320193/000032019324000006/aapl-20231230.htm"
        )
        assert f.is_xbrl is True


@pytest.mark.asyncio
async def test_get_fundamentals() -> None:
    """Test get_fundamentals and deduplication logic."""
    mock_facts = {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "label": "Revenues",
                    "description": "Revenues...",
                    "units": {
                        "USD": [
                            {
                                "val": 1000000.0,
                                "end": "2020-09-30",
                                "fy": 2020,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2020-10-30",
                                "accn": "0000320193-20-000096",
                                "frame": "CY2020",
                            },
                            {
                                "val": 1200000.0,
                                "end": "2020-09-30",
                                "fy": 2020,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2021-10-30",  # Restated later
                                "accn": "0000320193-21-000100",
                                "frame": "CY2020",
                            },
                        ]
                    },
                }
            }
        },
    }

    client = SecEdgarClient()
    client._ticker_to_cik["AAPL"] = 320193
    client._cik_to_primary_ticker[320193] = "AAPL"

    with patch.object(client, "fetch_company_facts", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = mock_facts

        # Test with deduplication
        facts = await client.get_fundamentals("AAPL", deduplicate=True)
        assert len(facts) == 1
        assert facts[0].val == 1200000.0

        # Test without deduplication
        facts_all = await client.get_fundamentals("AAPL", deduplicate=False)
        assert len(facts_all) == 2


@pytest.mark.asyncio
async def test_parse_company_facts_zip() -> None:
    """Test parsing of bulk ZIP company facts."""
    mock_facts = {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "label": "Revenues",
                    "description": "Revenues...",
                    "units": {
                        "USD": [
                            {
                                "val": 1000000.0,
                                "end": "2020-09-30",
                                "fy": 2020,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2020-10-30",
                                "accn": "0000320193-20-000096",
                                "frame": "CY2020",
                            }
                        ]
                    },
                }
            }
        },
    }

    client = SecEdgarClient()
    client._cik_to_primary_ticker[320193] = "AAPL"

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "companyfacts.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("CIK0000320193.json", json.dumps(mock_facts))

        fundamentals = []
        async for f in client.parse_company_facts_zip(zip_path, deduplicate=True):
            fundamentals.append(f)
        assert len(fundamentals) == 1
        f = fundamentals[0]
        assert isinstance(f, Fundamental)
        assert f.symbol == "AAPL"
        assert f.taxonomy == "us-gaap"
        assert f.tag == "Revenues"
        assert f.val == 1000000.0
