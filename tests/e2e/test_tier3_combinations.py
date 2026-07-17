import asyncio
import json

import aiohttp
import pytest

pytest.importorskip("web3")

from stockodile.exchanges.base_onchain.connector import (
    BaseOnchainTransport,
)
from stockodile.schema.records import BookSnapshot

# =====================================================================
# Tier 3 E2E Cross-Feature Combination Tests (>=6 tests)
# =====================================================================


# 1. Pagination + Rate Limiting
@pytest.mark.asyncio
async def test_t3_pagination_plus_rate_limiting(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96,
        "tick": 0,
        "liquidity": 1000,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        # Block difference of 1001 blocks (requires 3 slices)
        await session.post(f"{rpc_url}/control/block", json={"block_number": 2001})
        # Mock intermittent rate limit (HTTP 429 once)
        await session.post(
            f"{rpc_url}/control/behavior", json={"status_code": 429, "error_count": 1}
        )

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    transport._last_blocks["cbBTC-USDC"] = 1000
    await transport.connect()
    try:
        await asyncio.sleep(0.5)
    finally:
        await transport.close()

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rpc_url}/control/history") as resp:
            history = (await resp.json())["history"]

    get_logs_calls = [r for r in history if r["method"] == "eth_getLogs"]
    assert len(get_logs_calls) >= 3


# 2. Custom Symbol + Retries
@pytest.mark.asyncio
async def test_t3_custom_symbol_plus_retries(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    from stockodile.exchanges.base_onchain import connector

    connector.POOL_SPECS["CUSTOM_RETRY-USDC"] = {
        "type": "uniswap_v3",
        "token0": "cbBTC",
        "token1": "USDC",
        "fee": 500,
        "decimals0": 8,
        "decimals1": 6,
    }
    pool_data = {
        "address": "0x0000000000000000000000000000000000000010",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96,
        "tick": 0,
        "liquidity": 1000,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        # Timeout error count = 1
        await session.post(
            f"{rpc_url}/control/behavior", json={"status_code": 500, "error_count": 1}
        )

    transport = BaseOnchainTransport(rpc_url, ["CUSTOM_RETRY-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["pool"] == "CUSTOM_RETRY-USDC"
            break
    finally:
        await transport.close()


# 3. x402 Payment Gating + Fast Block Production
@pytest.mark.asyncio
async def test_t3_payment_gating_plus_fast_blocks(mock_rpc, api_server) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        # Step 1: Initial call to retrieve payment ID
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            assert resp.status == 402
            pid = (await resp.json())["payment_required"]["payment_id"]

    from eth_account import Account
    from eth_account.messages import encode_defunct

    private_key = "0x" + "1" * 64
    account = Account.from_key(private_key)
    msg = encode_defunct(text=pid)
    sig = account.sign_message(msg).signature.hex()
    if not sig.startswith("0x"):
        sig = "0x" + sig

    tx_hash = "0x" + "e" * 64
    usdc_contract = "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913"
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    recipient_padded = "0x" + "70997970c51812dc3a010c7d01b50e0d17dc79c8".zfill(64)
    amount_padded = (1000).to_bytes(32, "big").hex()

    receipt_data = {
        "transactionHash": tx_hash,
        "status": 1,
        "from": account.address,
        "logs": [
            {
                "address": usdc_contract,
                "topics": [transfer_topic, "0x" + "a" * 64, recipient_padded],
                "data": "0x" + amount_padded,
            }
        ],
    }

    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 10000000000,
    }

    async with aiohttp.ClientSession() as session:
        # Seed the receipt
        await session.post(f"{rpc_url}/control/receipt", json=receipt_data)
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

        # Advance block height to simulate fast block production
        await session.post(f"{rpc_url}/control/block", json={"block_number": 1200})

        sig_payload = {"payment_id": pid, "tx_hash": tx_hash, "signature": sig}
        headers = {"Payment-Signature": json.dumps(sig_payload)}

        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "success"


# 4. MCP Price fetching + RPC Rate Limiting
@pytest.mark.asyncio
async def test_t3_mcp_price_fetching_plus_rate_limiting(mcp_server_client, mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    proc = mcp_server_client

    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 10000000000,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        # Intermittent 429 rate limit
        await session.post(
            f"{rpc_url}/control/behavior", json={"status_code": 429, "error_count": 1}
        )

    req = {
        "jsonrpc": "2.0",
        "id": 105,
        "method": "tools/call",
        "params": {"name": "get_onchain_price", "arguments": {"symbol": "cbBTC-USDC"}},
    }
    proc.stdin.write(json.dumps(req) + "\n")
    proc.stdin.flush()

    loop = asyncio.get_running_loop()
    response_line = await loop.run_in_executor(None, proc.stdout.readline)
    assert response_line

    resp_data = json.loads(response_line.strip())
    assert resp_data["jsonrpc"] == "2.0"
    content = resp_data["result"]["content"][0]["text"]
    result = json.loads(content)
    assert "error" not in result
    assert result["price"] == 25.0


# 5. Synthetic Depth + Custom Decimal Pool
@pytest.mark.asyncio
async def test_t3_synthetic_depth_plus_custom_decimal_pool() -> None:
    from stockodile.exchanges.base_onchain.normalize import normalize_onchain_update

    msg = {
        "type": "onchain_update",
        "block": 1000,
        "pool": "CUSTOM-DECIMALS",
        "pool_type": "uniswap_v3",
        "timestamp": 1700000000,
        "state": {
            "price": 10.0,
            "reserve0": 1.0,
            "reserve1": 10.0,
            "decimals0": 6,  # base has 6 decimals
            "decimals1": 18,  # quote has 18 decimals
            "liquidity": 10**20,
            "tick": 0,
            "tick_spacing": 10,
        },
        "swaps": [],
    }
    records = list(normalize_onchain_update(msg, 123456789))
    snapshots = [r for r in records if isinstance(r, BookSnapshot)]
    assert len(snapshots) > 0
    snap = snapshots[0]
    assert len(snap.bids) == 5
    assert len(snap.asks) == 5
    # Check that prices are formatted around the base price of 10.0
    assert snap.asks[0][0] > 10.0
    assert snap.bids[0][0] < 10.0


# 6. Re-org + Pagination
@pytest.mark.asyncio
async def test_t3_reorg_plus_pagination(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96,
        "tick": 0,
        "liquidity": 1000,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    transport._last_blocks["cbBTC-USDC"] = 1005  # Stale higher block number (re-org scenario)
    await transport.connect()
    try:
        # Triggers reset and pagination starting at head_block - 20 (980) to 1000
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["block"] == 1000
            break
    finally:
        await transport.close()
