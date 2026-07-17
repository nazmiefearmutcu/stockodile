import asyncio

import aiohttp
import pytest

pytest.importorskip("web3")


@pytest.mark.asyncio
async def test_cors_headers(api_server: str) -> None:
    async with aiohttp.ClientSession() as session:
        # Check CORS on GET /api/v1/market-data with an Origin header
        headers = {"Origin": "http://example.com"}
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=AAPL", headers=headers
        ) as resp:
            # We expect 402, but CORS headers should be present
            assert resp.headers.get("access-control-allow-origin") == "*"

        # Check CORS on OPTIONS preflight
        preflight_headers = {
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Payment-Signature",
            "Origin": "http://example.com",
        }
        async with session.options(
            f"{api_server}/api/v1/market-data", headers=preflight_headers
        ) as resp:
            assert resp.status == 200
            assert resp.headers.get("access-control-allow-origin") == "*"
            assert "GET" in resp.headers.get("access-control-allow-methods", "")


@pytest.mark.asyncio
async def test_rate_limiting_market_data(api_server: str) -> None:
    async with aiohttp.ClientSession() as session:
        # We make 100 requests to /api/v1/market-data concurrently -> all should be 402
        # But not 429.
        # Then the 101st request should be 429.

        async def make_req() -> int:
            async with session.get(f"{api_server}/api/v1/market-data?symbol=AAPL") as resp:
                return resp.status

        statuses = await asyncio.gather(*(make_req() for _ in range(100)))
        for status in statuses:
            assert status == 402

        # 101st request should be rate-limited
        async with session.get(f"{api_server}/api/v1/market-data?symbol=AAPL") as resp:
            assert resp.status == 429
            data = await resp.json()
            assert data["detail"] == "Too Many Requests"


@pytest.mark.asyncio
async def test_rate_limiting_simulate_payment(api_server: str) -> None:
    async with aiohttp.ClientSession() as session:
        # Generate a dummy payload
        payload = {"payment_id": "dummy", "tx_hash": "0xhash", "signature": "0x" + "0" * 130}

        # 100 requests to /api/v1/simulate-payment concurrently.
        # Since it is a dummy payment, it will fail validation or return 404, 400, etc.
        # but not 429.
        async def make_req() -> int:
            async with session.post(f"{api_server}/api/v1/simulate-payment", json=payload) as resp:
                return resp.status

        statuses = await asyncio.gather(*(make_req() for _ in range(100)))
        for status in statuses:
            # The response can be 400 or 404, but definitely not 429
            assert status in (400, 404)

        # 101st request should be 429
        async with session.post(f"{api_server}/api/v1/simulate-payment", json=payload) as resp:
            assert resp.status == 429
            data = await resp.json()
            assert data["detail"] == "Too Many Requests"
