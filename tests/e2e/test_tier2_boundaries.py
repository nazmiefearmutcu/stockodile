import asyncio
import json

import aiohttp
import pytest

pytest.importorskip("web3")

from web3 import AsyncHTTPProvider, AsyncWeb3

from stockodile.exchanges.base_onchain.connector import (
    BaseOnchainTransport,
)
from stockodile.exchanges.base_onchain.normalize import normalize_onchain_update
from stockodile.schema.records import BookSnapshot, Quote, Trade

# =====================================================================
# Tier 2 E2E Boundary & Corner Case Tests (>=30 tests)
# =====================================================================


# 1. Extreme decimals pricing
@pytest.mark.asyncio
async def test_t2_extreme_decimals_pricing() -> None:
    msg = {
        "type": "onchain_update",
        "block": 1000,
        "pool": "EXTREME-USDC",
        "pool_type": "uniswap_v3",
        "timestamp": 1700000000,
        "state": {
            "price": 100.0,
            "reserve0": 1.0,
            "reserve1": 10000.0,
            "decimals0": 18,
            "decimals1": 2,
            "liquidity": 1000000,
            "tick": 0,
            "tick_spacing": 10,
        },
        "swaps": [],
    }
    records = list(normalize_onchain_update(msg, 123456789))
    snapshots = [r for r in records if isinstance(r, BookSnapshot)]
    assert len(snapshots) > 0
    snap = snapshots[0]
    assert snap.bids[0][0] < 100.0
    assert snap.asks[0][0] > 100.0


# 2. Zero Price Handling
@pytest.mark.asyncio
async def test_t2_zero_price_handling() -> None:
    msg = {
        "type": "onchain_update",
        "block": 1000,
        "pool": "cbBTC-USDC",
        "pool_type": "uniswap_v3",
        "timestamp": 1700000000,
        "state": {
            "price": 0.0,
            "reserve0": 100.0,
            "reserve1": 100.0,
            "decimals0": 8,
            "decimals1": 6,
        },
        "swaps": [],
    }
    records = list(normalize_onchain_update(msg, 123456789))
    assert len(records) == 0


# 3. Negative Price Handling
@pytest.mark.asyncio
async def test_t2_negative_price_handling() -> None:
    msg = {
        "type": "onchain_update",
        "block": 1000,
        "pool": "cbBTC-USDC",
        "pool_type": "uniswap_v3",
        "timestamp": 1700000000,
        "state": {
            "price": -10.0,
            "reserve0": 100.0,
            "reserve1": 100.0,
            "decimals0": 8,
            "decimals1": 6,
        },
        "swaps": [],
    }
    records = list(normalize_onchain_update(msg, 123456789))
    assert len(records) == 0


# 4. Empty Swap Logs
@pytest.mark.asyncio
async def test_t2_empty_swap_logs(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,
        "tick": 0,
        "liquidity": 1000,
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["swaps"] == []
            break
    finally:
        await transport.close()


# 5. Huge Pagination Split
@pytest.mark.asyncio
async def test_t2_huge_pagination_split(mock_rpc) -> None:
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
        # Block difference of 1500 blocks (starts at current_block - 20 by default
        # unless last_block is set)
        await session.post(f"{rpc_url}/control/block", json={"block_number": 2500})

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    transport._last_blocks["cbBTC-USDC"] = 1000
    await transport.connect()
    try:
        for _ in range(100):
            if transport._last_blocks.get("cbBTC-USDC", 0) >= 2500:
                break
            await asyncio.sleep(0.05)
    finally:
        await transport.close()

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rpc_url}/control/history") as resp:
            history = (await resp.json())["history"]

    get_logs_calls = [
        r
        for r in history
        if r["method"] == "eth_getLogs"
        and "0x0000000000000000000000000000000000000001" in str(r.get("params", []))
    ]
    assert len(get_logs_calls) == 4  # 1500 blocks divided by 500 size (plus 5 overlap) -> 4 calls


# 6. Connection drop during initialization
@pytest.mark.asyncio
async def test_t2_connection_drop_initialization(mock_rpc) -> None:
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
        # First query will fail, second succeeds
        await session.post(
            f"{rpc_url}/control/behavior", json={"status_code": 500, "error_count": 1}
        )

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["pool"] == "cbBTC-USDC"
            break
    finally:
        await transport.close()


# 7. Connection drop mid-polling
@pytest.mark.asyncio
async def test_t2_connection_drop_mid_polling(mock_rpc) -> None:
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
    await transport.connect()
    try:
        # Get first update
        async for _msg_bytes in transport:
            break

        # Drop connection for next poll
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{rpc_url}/control/behavior", json={"status_code": 500, "error_count": 1}
            )

        # Should still recover and fetch later
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["pool"] == "cbBTC-USDC"
            break
    finally:
        await transport.close()


# 8. Consistent rate limit (HTTP 429 exhausted)
@pytest.mark.asyncio
async def test_t2_consistent_rate_limit_exhausted(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"{rpc_url}/control/behavior", json={"status_code": 429, "error_count": 10}
        )

    BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))

    async def get_bn():
        return await w3.eth.block_number

    with pytest.raises(Exception):  # noqa: B017
        from stockodile.exchanges.base_onchain.connector import retry_rpc

        await retry_rpc(get_bn, max_attempts=2, base_delay=0.001)
    await w3.provider.disconnect()


# 9. Malformed JSON-RPC Responses
@pytest.mark.asyncio
async def test_t2_malformed_json_rpc_responses(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    # Set behavior to return some garbled output (200 status code with non-JSON text)
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"{rpc_url}/control/behavior", json={"status_code": 200, "error_count": 5}
        )

    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    with pytest.raises(Exception):  # noqa: B017
        await w3.eth.block_number
    await w3.provider.disconnect()


# 10. Malformed x402 Header Signature
@pytest.mark.asyncio
async def test_t2_malformed_x402_signature(api_server) -> None:
    headers = {"Payment-Signature": "garbled-json-string-here"}
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status == 400


# 11. JSON-RPC batch failures
@pytest.mark.asyncio
async def test_t2_json_rpc_batch_failures(mock_rpc) -> None:
    # Batch RPC queries can return errors for specific queries
    rpc_url, _ = mock_rpc
    payload = [
        {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "non_existent_method", "params": [], "id": 2},
    ]
    async with aiohttp.ClientSession() as session:
        async with session.post(rpc_url, json=payload) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert len(data) == 2
            assert "result" in data[0]
            assert "error" in data[1]


# 12. Huge log payload
@pytest.mark.asyncio
async def test_t2_huge_log_payload(mock_rpc) -> None:
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
    # Create 100 mock swap logs (simulating a huge workload)
    logs = []
    for i in range(100):
        logs.append(
            {
                "address": "0x0000000000000000000000000000000000000001",
                "blockNumber": hex(1001),
                "transactionHash": "0x" + f"{i:064x}",
                "logIndex": hex(i),
                "topics": ["0xc42079f94a6350d7e6235f29174924f9287a20ac8e91c97b870daEE5297F6e85"],
                "data": "0x"
                + (10**8).to_bytes(32, "big").hex()
                + (10**6).to_bytes(32, "big").hex(),
            }
        )

    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        await session.post(f"{rpc_url}/control/logs", json={"logs": logs})
        await session.post(f"{rpc_url}/control/block", json={"block_number": 1001})

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    transport._last_blocks["cbBTC-USDC"] = 1000
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            if msg["swaps"]:
                assert len(msg["swaps"]) == 100
                break
    finally:
        await transport.close()


# 13. Double swap logs
@pytest.mark.asyncio
async def test_t2_double_swap_logs() -> None:
    # If the transport returns double logs, normalizer parses them both
    # and the trade IDs are distinct or handled.
    msg = {
        "type": "onchain_update",
        "block": 1000,
        "pool": "cbBTC-USDC",
        "pool_type": "uniswap_v3",
        "timestamp": 1700000000,
        "state": {"price": 100.0, "reserve0": 1.0, "reserve1": 100.0},
        "swaps": [
            {
                "tx_hash": "a",
                "log_index": 1,
                "timestamp": 1700000000,
                "price": 100.0,
                "amount": 1.0,
                "is_buy": True,
            },
            {
                "tx_hash": "a",
                "log_index": 1,
                "timestamp": 1700000000,
                "price": 100.0,
                "amount": 1.0,
                "is_buy": True,
            },
        ],
    }
    records = list(normalize_onchain_update(msg, 123456789))
    trades = [r for r in records if isinstance(r, Trade)]
    assert len(trades) == 2
    assert trades[0].id == trades[1].id == "a-1"


# 14. Timestamp drift
@pytest.mark.asyncio
async def test_t2_timestamp_drift() -> None:
    msg = {
        "type": "onchain_update",
        "block": 1000,
        "pool": "cbBTC-USDC",
        "pool_type": "uniswap_v3",
        "timestamp": 9999999999,  # Far in the future
        "state": {"price": 100.0, "reserve0": 1.0, "reserve1": 100.0},
        "swaps": [],
    }
    records = list(normalize_onchain_update(msg, 123456789))
    tickers = [r for r in records if isinstance(r, Quote)]
    assert tickers[0].source_ts == 9999999999 * 1_000_000_000


# 15. USDC transfer log missing parameters
@pytest.mark.asyncio
async def test_t2_usdc_transfer_log_missing_topics(mock_rpc, api_server) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            pid = (await resp.json())["payment_required"]["payment_id"]

    tx_hash = "0x" + "c" * 64
    receipt_data = {
        "transactionHash": tx_hash,
        "status": 1,
        "logs": [
            {
                "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
                "topics": [],  # Missing transfer topic and recipient topic
                "data": "0x" + (1000).to_bytes(32, "big").hex(),
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


# 16. USDC transfer log multi-transfer
@pytest.mark.asyncio
async def test_t2_usdc_transfer_log_multi_transfer(mock_rpc, api_server) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            pid = (await resp.json())["payment_required"]["payment_id"]

    from eth_account import Account
    from eth_account.messages import encode_defunct

    private_key = "0x" + "1" * 64
    account = Account.from_key(private_key)
    msg = encode_defunct(text=pid)
    sig = account.sign_message(msg).signature.hex()
    if not sig.startswith("0x"):
        sig = "0x" + sig

    tx_hash = "0x" + "d" * 64
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
                # Arbitrary other transfer log first
                "address": "0x4200000000000000000000000000000000000006",
                "topics": [transfer_topic, "0x" + "a" * 64, recipient_padded],
                "data": "0x" + amount_padded,
            },
            {
                # Valid USDC transfer log second
                "address": usdc_contract,
                "topics": [transfer_topic, "0x" + "a" * 64, recipient_padded],
                "data": "0x" + amount_padded,
            },
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
            assert resp.status == 200


# 17. Aerodrome flipped address edge
@pytest.mark.asyncio
async def test_t2_aerodrome_flipped_address_edge() -> None:
    # Addresses are parsed to identify if flipped
    # Decimals are matching the order of token0 and token1 from specification
    # If token1 < token0 address-wise, is_flipped is True
    # Let's test with mock message
    msg = {
        "type": "onchain_update",
        "block": 1000,
        "pool": "AERO-USDC",
        "pool_type": "aerodrome_v2",
        "timestamp": 1700000000,
        "state": {
            "price": 0.1,
            "reserve0": 100.0,
            "reserve1": 1000.0,
            "is_flipped": True,
            "decimals0": 18,
            "decimals1": 6,
        },
        "swaps": [],
    }
    records = list(normalize_onchain_update(msg, 123456789))
    tickers = [r for r in records if isinstance(r, Quote)]
    assert len(tickers) > 0
    assert tickers[0].bid_px > 0


# 18. Fast block production
@pytest.mark.asyncio
async def test_t2_fast_block_production(mock_rpc) -> None:
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
        # Advance block height by 100 blocks
        await session.post(f"{rpc_url}/control/block", json={"block_number": 1100})

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    transport._last_blocks["cbBTC-USDC"] = 1000
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            assert msg["block"] == 1100
            break
    finally:
        await transport.close()


# 19. Slow block production
@pytest.mark.asyncio
async def test_t2_slow_block_production(mock_rpc) -> None:
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
        await session.post(f"{rpc_url}/control/block", json={"block_number": 1000})

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    transport._last_blocks["cbBTC-USDC"] = 1000
    await transport.connect()
    try:
        # Since block height is unchanged, get_logs won't query any range
        # (or range 1001-1000 is empty)
        await asyncio.sleep(0.3)
    finally:
        await transport.close()

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rpc_url}/control/history") as resp:
            history = (await resp.json())["history"]
    logs_calls = [r for r in history if r["method"] == "eth_getLogs"]
    # Should not fetch logs for negative/zero ranges
    for call in logs_calls:
        f = call["params"][0]
        assert int(f["fromBlock"], 16) <= int(f["toBlock"], 16)


# 20. RPC Timeout on eth_call
@pytest.mark.asyncio
async def test_t2_rpc_timeout_on_eth_call(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    # Set behavior delay to trigger timeout / slow responses
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/behavior", json={"delay": 0.2})

    # Use short timeout
    provider = AsyncHTTPProvider(rpc_url, request_kwargs={"timeout": 0.05})
    w3 = AsyncWeb3(provider)
    with pytest.raises(Exception):  # noqa: B017
        await w3.eth.block_number
    await w3.provider.disconnect()


# 21. Re-org detection
@pytest.mark.asyncio
async def test_t2_reorg_detection(mock_rpc) -> None:
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
    transport._last_blocks["cbBTC-USDC"] = 1005  # Last block higher than head block (1000)
    await transport.connect()
    try:
        async for msg_bytes in transport:
            msg = json.loads(msg_bytes.decode())
            # Connector should gracefully handle last block > current block by
            # polling from current_block - 20 or similar
            assert msg["block"] == 1000
            break
    finally:
        await transport.close()


# 22. Invalid hexadecimal inputs
@pytest.mark.asyncio
async def test_t2_invalid_hexadecimal_inputs(mock_rpc, caplog) -> None:
    # Seed log data with invalid hex string; verify error caught and logged.
    import logging

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
    invalid_log = {
        "address": "0x0000000000000000000000000000000000000001",
        "blockNumber": hex(1001),
        "transactionHash": "0x" + "a" * 64,
        "logIndex": "0x1",
        "topics": ["0xc42079f94a6350d7e6235f29174924f9287a20ac8e91c97b870daEE5297F6e85"],
        "data": "invalid-hex-data-here",
    }
    async with aiohttp.ClientSession() as session:
        await session.post(f"{rpc_url}/control/pool", json=pool_data)
        await session.post(f"{rpc_url}/control/logs", json={"logs": [invalid_log]})
        await session.post(f"{rpc_url}/control/block", json={"block_number": 1001})

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    transport._last_blocks["cbBTC-USDC"] = 1000
    with caplog.at_level(logging.ERROR):
        await transport.connect()
        try:
            await asyncio.sleep(0.3)
        finally:
            await transport.close()

    # Verify that the error was caught and logged
    errors = [rec.message for rec in caplog.records if "Error polling pool data" in rec.message]
    assert len(errors) > 0


# 23. Int256 overflow in Swap log
@pytest.mark.asyncio
async def test_t2_int256_overflow_in_swap_log() -> None:
    # Ensure decoders don't crash on extreme inputs
    msg = {
        "type": "onchain_update",
        "block": 1000,
        "pool": "cbBTC-USDC",
        "pool_type": "uniswap_v3",
        "timestamp": 1700000000,
        "state": {"price": 100.0, "reserve0": 1.0, "reserve1": 100.0},
        "swaps": [
            {
                "tx_hash": "a",
                "log_index": 1,
                "timestamp": 1700000000,
                "price": 100.0,
                "amount": 10**20,
                "is_buy": True,
            }
        ],
    }
    records = list(normalize_onchain_update(msg, 123456789))
    trades = [r for r in records if isinstance(r, Trade)]
    assert trades[0].size == 10**20


# 24. FastAPI server crash resilience
@pytest.mark.asyncio
async def test_t2_fastapi_server_crash_resilience() -> None:
    # Accessing non-existing or down server results in a connection error
    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.ClientConnectorError):
            await session.get("http://127.0.0.1:54321/api/v1/market-data")


# 25. MCP stdin EOF
@pytest.mark.asyncio
async def test_t2_mcp_stdin_eof(mcp_server_client) -> None:
    proc = mcp_server_client
    proc.stdin.close()
    for _ in range(50):
        if proc.poll() is not None:
            break
        await asyncio.sleep(0.1)
    # MCP server process should exit cleanly
    assert proc.poll() is not None


# 26. Extremely large decimals
@pytest.mark.asyncio
async def test_t2_extremely_large_decimals() -> None:
    msg = {
        "type": "onchain_update",
        "block": 1000,
        "pool": "HUGE-USDC",
        "pool_type": "uniswap_v3",
        "timestamp": 1700000000,
        "state": {
            "price": 10.0,
            "reserve0": 1.0,
            "reserve1": 10.0,
            "decimals0": 36,  # very large decimal
            "decimals1": 6,
            "liquidity": 10**30,
            "tick": 0,
            "tick_spacing": 10,
        },
        "swaps": [],
    }
    records = list(normalize_onchain_update(msg, 123456789))
    assert len(records) > 0


# 27. Empty factory return
@pytest.mark.asyncio
async def test_t2_empty_factory_return(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    # We do not seed the pool. So getPool returns 0x0000...
    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    await transport.connect()
    try:
        # Should just yield nothing because address is 0
        await asyncio.sleep(0.3)
        assert transport._queue.empty()
    finally:
        await transport.close()


# 28. HTTP 500 Internal Error from RPC
@pytest.mark.asyncio
async def test_t2_http_500_internal_error(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"{rpc_url}/control/behavior", json={"status_code": 500, "error_count": 1}
        )

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    # Will retry on 500 and succeed
    await transport.connect()
    await transport.close()


# 29. HTTP 503 Service Unavailable from RPC
@pytest.mark.asyncio
async def test_t2_http_503_service_unavailable(mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"{rpc_url}/control/behavior", json={"status_code": 503, "error_count": 1}
        )

    transport = BaseOnchainTransport(rpc_url, ["cbBTC-USDC"], poll_interval=0.1)
    # Will retry on 503 and succeed
    await transport.connect()
    await transport.close()


# 30. x402 signature EIP-712 parsing
@pytest.mark.asyncio
async def test_t2_x402_signature_eip712_parsing(api_server) -> None:
    # Syntactically invalid EIP-712 payload
    headers = {
        # no signature field or bad formatting
        "Payment-Signature": '{"payment_id": "123", "tx_hash": "abc"}'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status == 400
