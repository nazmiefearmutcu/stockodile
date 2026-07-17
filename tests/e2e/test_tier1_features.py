import asyncio
import json

import aiohttp
import pytest

pytest.importorskip("web3")

from web3 import AsyncHTTPProvider, AsyncWeb3

from stockodile.exchanges.base_onchain.connector import (
    BaseOnchainTransport,
)
from stockodile.schema.records import BookSnapshot, BookTicker

# =====================================================================
# Tier 1 E2E Feature Isolation Tests (F1-F6)
# =====================================================================


# 1. F1-Uniswap V3 Pool Resolution
@pytest.mark.asyncio
async def test_f1_uniswap_v3_pool_resolution(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
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
        async with session.post(f"{rpc_url}/control/pool", json=pool_data) as rpc_resp:
            assert rpc_resp.status == 200

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["type"] == "onchain_update"
            assert msg["pool"] == "cbBTC-USDC"
            assert msg["pool_type"] == "uniswap_v3"
            break
    finally:
        await transport.close()


# 2. F1-Aerodrome Pool Resolution
@pytest.mark.asyncio
async def test_f1_aerodrome_pool_resolution(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000002",
        "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
        "token0": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",  # AERO
        "token1": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",  # USDbC
        "stable": False,
        "reserve0": 1000000000000000000,
        "reserve1": 1000000,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{rpc_url}/control/pool", json=pool_data) as rpc_resp:
            assert rpc_resp.status == 200

    transport = BaseOnchainTransport(rpc_url, ["AERO-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["type"] == "onchain_update"
            assert msg["pool"] == "AERO-USDC"
            assert msg["pool_type"] == "aerodrome_v2"
            break
    finally:
        await transport.close()


# 3. F1-Uniswap V3 Slot0 Calculation
@pytest.mark.asyncio
async def test_f1_uniswap_v3_slot0_calculation(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 10**10,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            state = msg["state"]
            assert state["price"] == 25.0
            assert state["reserve0"] == 200.0
            assert state["reserve1"] == 5000.0
            break
    finally:
        await transport.close()


# 4. F1-Aerodrome Reserves Retrieval
@pytest.mark.asyncio
async def test_f1_aerodrome_reserves_retrieval(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000002",
        "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
        "token0": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
        "token1": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "stable": False,
        "reserve0": 100 * 10**18,
        "reserve1": 10 * 10**6,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["AERO-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            state = msg["state"]
            assert state["price"] == 0.1
            assert state["reserve0"] == 100.0
            assert state["reserve1"] == 10.0
            break
    finally:
        await transport.close()


# 5. F1-Swap Log Processing (Uniswap V3)
@pytest.mark.asyncio
async def test_f1_swap_log_processing_uniswap_v3(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 10**10,
    }
    log_entry = {
        "address": "0x0000000000000000000000000000000000000001",
        "blockNumber": hex(1001),
        "transactionHash": "0x" + "a" * 64,
        "logIndex": "0x1",
        "topics": [
            "0xc42079f94a6350d7e6235f29174924f9287a20ac8e91c97b870daEE5297F6e85",
        ],
        "data": "0x"
        + (-100 * 10**8).to_bytes(32, "big", signed=True).hex()
        + (2500 * 10**6).to_bytes(32, "big", signed=True).hex(),
    }

    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        await session.post(f"{rpc_url}/control/logs", json={"logs": [log_entry]})
        await session.post(f"{rpc_url}/control/block", json={"block_number": 1001})

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    transport._last_blocks["cbBTC-USDC"] = 1000
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            swaps = msg["swaps"]
            if swaps:
                assert len(swaps) == 1
                sw = swaps[0]
                assert sw["tx_hash"] == "a" * 64
                assert sw["price"] == 400.0
                assert sw["amount"] == 25.0
                assert sw["is_buy"] is False
                break
    finally:
        await transport.close()


# 6. F1-Swap Log Processing (Aerodrome V2)
@pytest.mark.asyncio
async def test_f1_swap_log_processing_aerodrome_v2(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000002",
        "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
        "token0": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
        "token1": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "stable": False,
        "reserve0": 100 * 10**18,
        "reserve1": 10 * 10**6,
    }
    log_entry = {
        "address": "0x0000000000000000000000000000000000000002",
        "blockNumber": hex(1001),
        "transactionHash": "0x" + "b" * 64,
        "logIndex": "0x2",
        "topics": [
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
        ],
        "data": "0x"
        + (0).to_bytes(32, "big").hex()
        + (10 * 10**6).to_bytes(32, "big").hex()
        + (100 * 10**18).to_bytes(32, "big").hex()
        + (0).to_bytes(32, "big").hex(),
    }

    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        await session.post(f"{rpc_url}/control/logs", json={"logs": [log_entry]})
        await session.post(f"{rpc_url}/control/block", json={"block_number": 1001})

    transport = BaseOnchainTransport(rpc_url, ["AERO-USDC"], poll_interval=0.1)
    transport._last_blocks["AERO-USDC"] = 1000
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            swaps = msg["swaps"]
            if swaps:
                assert len(swaps) == 1
                sw = swaps[0]
                assert sw["tx_hash"] == "b" * 64
                assert sw["price"] == 0.1
                assert sw["amount"] == 100.0
                assert sw["is_buy"] is True
                break
    finally:
        await transport.close()


# 7. F1-Liveness Sentinel Check
@pytest.mark.asyncio
async def test_f1_liveness_sentinel_check(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    await transport.connect()
    await transport.close()

    terminated = False
    try:
        async for _msg in transport:
            pass
        terminated = True
    except Exception:
        pass
    assert terminated is True


# 8. F2-MCP tool list
@pytest.mark.asyncio
async def test_f2_mcp_tool_list(mcp_server_client) -> None:
    proc = mcp_server_client
    req = {"jsonrpc": "2.0", "id": 100, "method": "tools/list", "params": {}}
    proc.stdin.write(json.dumps(req) + "\n")
    proc.stdin.flush()

    loop = asyncio.get_running_loop()
    response_line = await loop.run_in_executor(None, proc.stdout.readline)
    assert response_line

    resp_data = json.loads(response_line.strip())
    assert resp_data["jsonrpc"] == "2.0"
    assert resp_data["id"] == 100

    tools = resp_data["result"]["tools"]
    tool_names = [t["name"] for t in tools]
    assert "get_onchain_price" in tool_names
    assert "query_market_data" in tool_names
    assert "get_funding_apr" in tool_names


# 9. F2-MCP query get_onchain_price
@pytest.mark.asyncio
async def test_f2_mcp_query_get_onchain_price(mcp_server_client, mock_rpc) -> None:
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

    req = {
        "jsonrpc": "2.0",
        "id": 101,
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
    assert resp_data["id"] == 101

    content = resp_data["result"]["content"][0]["text"]
    result = json.loads(content)

    assert "error" not in result
    assert result["symbol"] == "cbBTC-USDC"
    assert result["pool_address"].lower() == "0x0000000000000000000000000000000000000001"
    assert result["price"] == 25.0


# 10. F2-MCP query get_onchain_price (Aerodrome)
@pytest.mark.asyncio
async def test_f2_mcp_query_get_onchain_price_aerodrome(mcp_server_client, mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    proc = mcp_server_client

    pool_data = {
        "address": "0x0000000000000000000000000000000000000002",
        "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
        "token0": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
        "token1": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "stable": False,
        "reserve0": 100 * 10**18,
        "reserve1": 10 * 10**6,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    req = {
        "jsonrpc": "2.0",
        "id": 102,
        "method": "tools/call",
        "params": {"name": "get_onchain_price", "arguments": {"symbol": "AERO-USDC"}},
    }
    proc.stdin.write(json.dumps(req) + "\n")
    proc.stdin.flush()

    loop = asyncio.get_running_loop()
    response_line = await loop.run_in_executor(None, proc.stdout.readline)
    assert response_line

    resp_data = json.loads(response_line.strip())
    assert resp_data["jsonrpc"] == "2.0"
    assert resp_data["id"] == 102

    content = resp_data["result"]["content"][0]["text"]
    result = json.loads(content)

    assert "error" not in result
    assert result["symbol"] == "AERO-USDC"
    assert result["price"] == 0.1


# 11. F2-MCP non-existing symbol
@pytest.mark.asyncio
async def test_f2_mcp_non_existing_symbol(mcp_server_client) -> None:
    proc = mcp_server_client
    req = {
        "jsonrpc": "2.0",
        "id": 103,
        "method": "tools/call",
        "params": {"name": "get_onchain_price", "arguments": {"symbol": "INVALID-SYMBOL"}},
    }
    proc.stdin.write(json.dumps(req) + "\n")
    proc.stdin.flush()

    loop = asyncio.get_running_loop()
    response_line = await loop.run_in_executor(None, proc.stdout.readline)
    assert response_line

    resp_data = json.loads(response_line.strip())
    content = resp_data["result"]["content"][0]["text"]
    result = json.loads(content)

    assert "error" in result
    assert "not supported" in result["error"]


# 12. F3-Pagination Check
@pytest.mark.asyncio
async def test_f3_pagination_check(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 10**10,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        await session.post(f"{rpc_url}/control/block", json={"block_number": 2000})

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    transport._last_blocks["cbBTC-USDC"] = 1000
    await transport.connect()

    try:
        await asyncio.sleep(0.3)
    finally:
        await transport.close()

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rpc_url}/control/history") as resp:
            data = await resp.json()
            history = data["history"]

    get_logs_calls = [req for req in history if req["method"] == "eth_getLogs"]
    assert len(get_logs_calls) >= 2
    for call in get_logs_calls:
        filter_obj = call["params"][0]
        from_blk = (
            int(filter_obj["fromBlock"], 16)
            if isinstance(filter_obj["fromBlock"], str)
            else filter_obj["fromBlock"]
        )
        to_blk = (
            int(filter_obj["toBlock"], 16)
            if isinstance(filter_obj["toBlock"], str)
            else filter_obj["toBlock"]
        )
        assert to_blk - from_blk + 1 <= 500


# 13. F3-Pagination boundaries
@pytest.mark.asyncio
async def test_f3_pagination_boundaries(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 10**10,
    }

    # Boundary 1: Exactly 500 blocks
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/reset")
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        await session.post(f"{rpc_url}/control/block", json={"block_number": 1500})

    t1 = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    t1._last_blocks["cbBTC-USDC"] = 1000
    await t1.connect()
    try:
        await asyncio.sleep(0.3)
    finally:
        await t1.close()

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rpc_url}/control/history") as resp:
            h1 = (await resp.json())["history"]
    logs_t1 = [r for r in h1 if r["method"] == "eth_getLogs"]
    assert len(logs_t1) > 0
    for call in logs_t1:
        filter_obj = call["params"][0]
        from_blk = (
            int(filter_obj["fromBlock"], 16)
            if isinstance(filter_obj["fromBlock"], str)
            else filter_obj["fromBlock"]
        )
        to_blk = (
            int(filter_obj["toBlock"], 16)
            if isinstance(filter_obj["toBlock"], str)
            else filter_obj["toBlock"]
        )
        assert to_blk - from_blk + 1 <= 500

    # Boundary 2: 501 blocks
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/reset")
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        await session.post(f"{rpc_url}/control/block", json={"block_number": 1501})

    t2 = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    t2._last_blocks["cbBTC-USDC"] = 1000
    await t2.connect()
    try:
        await asyncio.sleep(0.3)
    finally:
        await t2.close()

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rpc_url}/control/history") as resp:
            h2 = (await resp.json())["history"]
    logs_t2 = [r for r in h2 if r["method"] == "eth_getLogs"]
    assert len(logs_t2) > 0
    has_large_query = False
    for call in logs_t2:
        filter_obj = call["params"][0]
        from_blk = (
            int(filter_obj["fromBlock"], 16)
            if isinstance(filter_obj["fromBlock"], str)
            else filter_obj["fromBlock"]
        )
        to_blk = (
            int(filter_obj["toBlock"], 16)
            if isinstance(filter_obj["toBlock"], str)
            else filter_obj["toBlock"]
        )
        if to_blk - from_blk + 1 > 500:
            has_large_query = True
    assert has_large_query is False


# 14. F3-Backoff Success
@pytest.mark.asyncio
async def test_f3_backoff_success(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 10**10,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        await session.post(
            f"{rpc_url}/control/behavior", json={"status_code": 429, "error_count": 1}
        )

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["type"] == "onchain_update"
            break
    finally:
        await transport.close()


# 15. F4-Uniswap V3 Synthetic depth calculation
@pytest.mark.asyncio
async def test_f4_uniswap_v3_synthetic_depth_calculation(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 10**10,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            from stockodile.exchanges.base_onchain.normalize import normalize_onchain_update

            records = list(normalize_onchain_update(msg, 123456789))
            snapshots = [r for r in records if isinstance(r, BookSnapshot)]
            assert len(snapshots) > 0
            snap = snapshots[0]
            assert len(snap.bids) == 5
            assert len(snap.asks) == 5
            break
    finally:
        await transport.close()


# 16. F4-Orderbook size enforcement
@pytest.mark.asyncio
async def test_f4_orderbook_size_enforcement(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 1,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            from stockodile.exchanges.base_onchain.normalize import normalize_onchain_update

            records = list(normalize_onchain_update(msg, 123456789))
            tickers = [r for r in records if isinstance(r, BookTicker)]
            assert len(tickers) > 0
            tick = tickers[0]
            assert tick.bid_sz >= 0.0001
            assert tick.ask_sz >= 0.0001
            break
    finally:
        await transport.close()


# 17. F5-x402 Micropayment 402 code
@pytest.mark.asyncio
async def test_f5_x402_micropayment_402_code(api_server) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            assert resp.status == 402
            data = await resp.json()
            assert data["status"] == "payment_required"
            assert "Payment-Required" in resp.headers
            req_payload = json.loads(resp.headers["Payment-Required"])
            assert req_payload["price"] == "0.001"
            assert req_payload["currency"] == "USDC"


# 18. F5-x402 Verify valid payment
@pytest.mark.asyncio
async def test_f5_x402_verify_valid_payment(mock_rpc, api_server) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
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

    tx_hash = "0x" + "c" * 64
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
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        await session.post(f"{rpc_url}/control/receipt", json=receipt_data)

        sig_payload = {"payment_id": pid, "tx_hash": tx_hash, "signature": sig}
        headers = {"Payment-Signature": json.dumps(sig_payload)}
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            if resp.status != 200:
                print("FAILED VERIFY VALID PAYMENT:", await resp.text())
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "success"


# 19. F5-x402 Receipt lookup fail
@pytest.mark.asyncio
async def test_f5_x402_receipt_lookup_fail(api_server) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            pid = (await resp.json())["payment_required"]["payment_id"]

        sig_payload = {
            "payment_id": pid,
            "tx_hash": "0xnonexistenttxhash",
            "signature": "0xmocksig",
        }
        headers = {"Payment-Signature": json.dumps(sig_payload)}
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status == 400


# 20. F5-x402 Wrong recipient
@pytest.mark.asyncio
async def test_f5_x402_wrong_recipient(mock_rpc, api_server) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            pid = (await resp.json())["payment_required"]["payment_id"]

    tx_hash = "0x" + "c" * 64
    usdc_contract = "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913"
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    recipient_padded = "0x" + "1111222233334444555566667777888899990000".zfill(64)
    amount_padded = (1000).to_bytes(32, "big").hex()

    receipt_data = {
        "transactionHash": tx_hash,
        "status": 1,
        "logs": [
            {
                "address": usdc_contract,
                "topics": [transfer_topic, "0x" + "a" * 64, recipient_padded],
                "data": "0x" + amount_padded,
            }
        ],
    }

    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/receipt", json=receipt_data)
        sig_payload = {"payment_id": pid, "tx_hash": tx_hash, "signature": "0xmock"}
        headers = {"Payment-Signature": json.dumps(sig_payload)}
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status in (400, 402)


# 21. F5-x402 Wrong transfer amount
@pytest.mark.asyncio
async def test_f5_x402_wrong_transfer_amount(mock_rpc, api_server) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            pid = (await resp.json())["payment_required"]["payment_id"]

    tx_hash = "0x" + "c" * 64
    usdc_contract = "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913"
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    recipient_padded = "0x" + "70997970c51812dc3a010c7d01b50e0d17dc79c8".zfill(64)
    amount_padded = (999).to_bytes(32, "big").hex()

    receipt_data = {
        "transactionHash": tx_hash,
        "status": 1,
        "logs": [
            {
                "address": usdc_contract,
                "topics": [transfer_topic, "0x" + "a" * 64, recipient_padded],
                "data": "0x" + amount_padded,
            }
        ],
    }

    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/receipt", json=receipt_data)
        sig_payload = {"payment_id": pid, "tx_hash": tx_hash, "signature": "0xmock"}
        headers = {"Payment-Signature": json.dumps(sig_payload)}
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status in (400, 402)


# 22. F5-x402 Wrong ERC-20 contract
@pytest.mark.asyncio
async def test_f5_x402_wrong_erc20_contract(mock_rpc, api_server) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            pid = (await resp.json())["payment_required"]["payment_id"]

    tx_hash = "0x" + "c" * 64
    wrong_contract = "0x4200000000000000000000000000000000000006"
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    recipient_padded = "0x" + "70997970c51812dc3a010c7d01b50e0d17dc79c8".zfill(64)
    amount_padded = (1000).to_bytes(32, "big").hex()

    receipt_data = {
        "transactionHash": tx_hash,
        "status": 1,
        "logs": [
            {
                "address": wrong_contract,
                "topics": [transfer_topic, "0x" + "a" * 64, recipient_padded],
                "data": "0x" + amount_padded,
            }
        ],
    }

    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/receipt", json=receipt_data)
        sig_payload = {"payment_id": pid, "tx_hash": tx_hash, "signature": "0xmock"}
        headers = {"Payment-Signature": json.dumps(sig_payload)}
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status in (400, 402)


# 23. F5-x402 Failed transaction status
@pytest.mark.asyncio
async def test_f5_x402_failed_transaction_status(mock_rpc, api_server) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            pid = (await resp.json())["payment_required"]["payment_id"]

    tx_hash = "0x" + "c" * 64
    usdc_contract = "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913"
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    recipient_padded = "0x" + "70997970c51812dc3a010c7d01b50e0d17dc79c8".zfill(64)
    amount_padded = (1000).to_bytes(32, "big").hex()

    receipt_data = {
        "transactionHash": tx_hash,
        "status": 0,
        "logs": [
            {
                "address": usdc_contract,
                "topics": [transfer_topic, "0x" + "a" * 64, recipient_padded],
                "data": "0x" + amount_padded,
            }
        ],
    }

    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/receipt", json=receipt_data)
        sig_payload = {"payment_id": pid, "tx_hash": tx_hash, "signature": "0xmock"}
        headers = {"Payment-Signature": json.dumps(sig_payload)}
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status in (400, 402)


# 24. F6-Custom Symbol Registration
@pytest.mark.asyncio
async def test_f6_custom_symbol_registration(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    from stockodile.exchanges.base_onchain import connector

    connector.POOL_SPECS["CUSTOM-USDC"] = {
        "type": "uniswap_v3",
        "token0": "CUSTOM",
        "token1": "USDC",
        "fee": 500,
        "decimals0": 18,
        "decimals1": 6,
    }
    connector.TOKENS["CUSTOM"] = "0x1234567890123456789012345678901234567890"

    pool_data = {
        "address": "0x0000000000000000000000000000000000000005",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0x1234567890123456789012345678901234567890",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96,
        "tick": 0,
        "liquidity": 1000,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["CUSTOM-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["pool"] == "CUSTOM-USDC"
            assert msg["state"]["price"] > 0
            break
    finally:
        await transport.close()


# 25. F6-Custom Symbol Decimals
@pytest.mark.asyncio
async def test_f6_custom_symbol_decimals(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    from stockodile.exchanges.base_onchain import connector

    connector.POOL_SPECS["CUSTOM_18-WETH"] = {
        "type": "uniswap_v3",
        "token0": "CUSTOM_18",
        "token1": "WETH",
        "fee": 3000,
        "decimals0": 18,
        "decimals1": 18,
    }
    connector.TOKENS["CUSTOM_18"] = "0x1111111111111111111111111111111111111111"

    pool_data = {
        "address": "0x0000000000000000000000000000000000000006",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0x1111111111111111111111111111111111111111",
        "token1": "0x4200000000000000000000000000000000000006",
        "fee": 3000,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 10**10,
    }

    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["CUSTOM_18-WETH"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["state"]["price"] == 4.0
            break
    finally:
        await transport.close()


# 26. F6-Custom Uniswap fee tier
@pytest.mark.asyncio
async def test_f6_custom_uniswap_fee_tier(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    from stockodile.exchanges.base_onchain import connector

    connector.POOL_SPECS["CUSTOM_FEE-USDC"] = {
        "type": "uniswap_v3",
        "token0": "cbBTC",
        "token1": "USDC",
        "fee": 10000,
        "decimals0": 8,
        "decimals1": 6,
    }
    pool_data = {
        "address": "0x0000000000000000000000000000000000000007",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 10000,
        "sqrtPriceX96": 2**96,
        "tick": 0,
        "liquidity": 1000,
    }

    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["CUSTOM_FEE-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for _msg in transport:
            break
    finally:
        await transport.close()

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rpc_url}/control/history") as resp:
            history = (await resp.json())["history"]

    get_pool_calls = [
        r for r in history if r["method"] == "eth_call" and "0x1698ee" in r["params"][0]["data"]
    ]
    assert len(get_pool_calls) > 0
    found_fee = False
    for call in get_pool_calls:
        data = call["params"][0]["data"]
        if "2710" in data:
            found_fee = True
    assert found_fee is True


# 27. F6-Custom Aerodrome stable
@pytest.mark.asyncio
async def test_f6_custom_aerodrome_stable(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    from stockodile.exchanges.base_onchain import connector

    connector.POOL_SPECS["CUSTOM_STABLE-USDC"] = {
        "type": "aerodrome_v2",
        "token0": "AERO",
        "token1": "USDbC",
        "stable": True,
        "decimals0": 18,
        "decimals1": 6,
    }
    pool_data = {
        "address": "0x0000000000000000000000000000000000000008",
        "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
        "token0": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
        "token1": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "stable": True,
        "reserve0": 1000,
        "reserve1": 1000,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["CUSTOM_STABLE-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for _msg in transport:
            break
    finally:
        await transport.close()

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rpc_url}/control/history") as resp:
            history = (await resp.json())["history"]

    get_pool_calls = [
        r for r in history if r["method"] == "eth_call" and "0x79bc57d5" in r["params"][0]["data"]
    ]
    assert len(get_pool_calls) > 0
    found_stable = False
    for call in get_pool_calls:
        data = call["params"][0]["data"]
        if data.endswith("1"):
            found_stable = True
    assert found_stable is True


# 28. F2-MCP custom symbol lookup
@pytest.mark.asyncio
async def test_f2_mcp_custom_symbol_lookup(mcp_server_client, mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    proc = mcp_server_client
    from stockodile.exchanges.base_onchain import connector

    connector.POOL_SPECS["CUSTOM_MCP-USDC"] = {
        "type": "uniswap_v3",
        "token0": "cbBTC",
        "token1": "USDC",
        "fee": 500,
        "decimals0": 8,
        "decimals1": 6,
    }
    from stockodile.exchanges.base_onchain.connector import _write_ipc_to_file
    _write_ipc_to_file("POOL_SPECS", dict(connector.POOL_SPECS))
    await asyncio.sleep(0.2)

    pool_data = {
        "address": "0x0000000000000000000000000000000000000009",
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

    req = {
        "jsonrpc": "2.0",
        "id": 104,
        "method": "tools/call",
        "params": {"name": "get_onchain_price", "arguments": {"symbol": "CUSTOM_MCP-USDC"}},
    }
    proc.stdin.write(json.dumps(req) + "\n")
    proc.stdin.flush()

    loop = asyncio.get_running_loop()
    response_line = await loop.run_in_executor(None, proc.stdout.readline)
    assert response_line

    resp_data = json.loads(response_line.strip())
    content = resp_data["result"]["content"][0]["text"]
    result = json.loads(content)

    assert "error" not in result
    assert result["symbol"] == "CUSTOM_MCP-USDC"


# 29. F1-Block Cache Hit
@pytest.mark.asyncio
async def test_f1_block_cache_hit(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    try:
        ts1 = await transport._get_block_timestamp(w3, 1000)
        assert ts1 == 1700000000

        async with aiohttp.ClientSession() as session:
            await session.post(f"{rpc_url}/control/reset")

        ts2 = await transport._get_block_timestamp(w3, 1000)
        assert ts2 == 1700000000

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{rpc_url}/control/history") as resp:
                history = (await resp.json())["history"]

        get_block_calls = [r for r in history if r["method"] == "eth_getBlockByNumber"]
        assert len(get_block_calls) == 0
    finally:
        await w3.provider.disconnect()


# 30. F1-Block Cache eviction
@pytest.mark.asyncio
async def test_f1_block_cache_eviction(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    try:
        for i in range(1001):
            transport._block_cache[i] = 1700000000 + i

        async with aiohttp.ClientSession() as session:
            await session.post(f"{rpc_url}/control/reset")

        ts = await transport._get_block_timestamp(w3, 2000)
        assert ts == 1700000000

        assert len(transport._block_cache) == 1
        assert 2000 in transport._block_cache
        assert 1000 not in transport._block_cache
    finally:
        await w3.provider.disconnect()
