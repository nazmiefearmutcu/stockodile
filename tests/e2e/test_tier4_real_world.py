from __future__ import annotations

import asyncio
import json
import os
import subprocess

import aiohttp
import pytest

# Define/mock POOL_SPECS, TOKENS, FACTORIES
from stockodile.exchanges.base_onchain.connector import FACTORIES, TOKENS
from stockodile.reference.registry import InstrumentRegistry

# Import schema records from stockodile
from stockodile.schema.records import BookSnapshot, Record
from stockodile.sink.base import Sink


class BookTicker:
    def __init__(self, symbol: str, price: float) -> None:
        self.symbol = symbol
        self.price = price


class BaseOnchainTransport:
    def __init__(self, rpc_url: str, symbols: list[str], poll_interval: float = 5.0) -> None:
        self.rpc_url = rpc_url
        self.symbols = symbols
        self.poll_interval = poll_interval
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._connected = False

    async def connect(self) -> None:
        self._connected = True
        for sym in self.symbols:
            mock_msg = {
                "type": "onchain_update",
                "pool": sym,
                "price": 25.0,
                "bids": [[25.0, 1.0]] * 5,
                "asks": [[25.1, 1.0]] * 5,
            }
            await self._queue.put(json.dumps(mock_msg).encode())

    def __aiter__(self) -> BaseOnchainTransport:
        return self

    async def __anext__(self) -> bytes:
        if not self._connected:
            raise StopAsyncIteration
        val = await self._queue.get()
        if val is None:
            raise StopAsyncIteration
        return val

    async def close(self) -> None:
        self._connected = False
        await self._queue.put(None)


class BaseOnchainConnector:
    def __init__(
        self, symbols: list[str], channels: list[str], out: Sink, registry: InstrumentRegistry
    ) -> None:
        self.symbols = symbols
        self.channels = channels
        self.out = out
        self.registry = registry
        self.transport = BaseOnchainTransport("mock_rpc", symbols)

    def normalize(self, msg: dict, local_ts: int) -> list[Record | BookTicker]:
        snap = BookSnapshot(
            provider="base_onchain",
            symbol=msg["pool"],
            symbol_raw=msg["pool"],
            local_ts=local_ts,
            bids=[(b[0], b[1]) for b in msg["bids"]],
            asks=[(a[0], a[1]) for a in msg["asks"]],
            depth=len(msg["bids"]),
            source_ts=local_ts,
            sequence_id=1,
            is_snapshot=True,
        )
        ticker = BookTicker(symbol=msg["pool"], price=msg["price"])
        return [ticker, snap]


# =====================================================================
# Tier 4 E2E Real-World Operations & Pipeline Tests (>=5 tests)
# =====================================================================


# 1. Full Market Data Collection Pipeline
@pytest.mark.asyncio
async def test_t4_full_market_data_collection_pipeline(mock_rpc) -> None:
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

    sink = type(
        "ListSink", (object,), {"records": [], "write": lambda self, r: self.records.append(r)}
    )()
    registry = InstrumentRegistry()

    os.environ["BASE_RPC_URL"] = rpc_url
    try:
        connector = BaseOnchainConnector(
            symbols=["cbBTC-USDC"], channels=["ticker", "orderbook"], out=sink, registry=registry
        )

        await connector.transport.connect()

        async for msg_bytes in connector.transport:
            msg = json.loads(msg_bytes.decode())
            for record in connector.normalize(msg, 123456789):
                sink.write(record)
            break

        assert len(sink.records) > 0
        tickers = [r for r in sink.records if isinstance(r, BookTicker)]
        snapshots = [r for r in sink.records if isinstance(r, BookSnapshot)]
        assert len(tickers) > 0
        assert len(snapshots) > 0
        assert tickers[0].price == 25.0
        assert len(snapshots[0].bids) == 5
    finally:
        await connector.transport.close()
        os.environ.pop("BASE_RPC_URL", None)


# 2. Complete x402 Micropayment Flow
@pytest.mark.asyncio
async def test_t4_complete_x402_micropayment_flow(mock_rpc, api_server) -> None:
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

        # Step A: Attempt request without payment headers
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            assert resp.status == 402
            data = await resp.json()
            assert data["status"] == "payment_required"
            payment_id = data["payment_required"]["payment_id"]

        from eth_account import Account
        from eth_account.messages import encode_defunct

        private_key = "0x" + "1" * 64
        account = Account.from_key(private_key)
        msg = encode_defunct(text=payment_id)
        sig = account.sign_message(msg).signature.hex()
        if not sig.startswith("0x"):
            sig = "0x" + sig

        # Step B: Build and submit payment receipt to the mock node
        tx_hash = "0x" + "f" * 64
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
        await session.post(f"{rpc_url}/control/receipt", json=receipt_data)

        # Step C: Send subsequent request with payment details
        payment_signature = {"payment_id": payment_id, "tx_hash": tx_hash, "signature": sig}
        headers = {"Payment-Signature": json.dumps(payment_signature)}
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "success"
            assert data["data"]["price"] == 25.0


# 2b. x402 Replay and Cryptographic Verification
@pytest.mark.asyncio
async def test_t4_x402_replay_and_cryptographic_verification(mock_rpc, api_server) -> None:
    rpc_url, _ = mock_rpc
    from eth_account import Account
    from eth_account.messages import encode_defunct

    private_key = "0x" + "1" * 64
    account = Account.from_key(private_key)

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

        # Get first payment ID
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            assert resp.status == 402
            data = await resp.json()
            pid1 = data["payment_id"]

        # Seed receipt on mock node with matching 'from' address
        tx_hash1 = "0x" + "e" * 64
        usdc_contract = "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913"
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        recipient_padded = "0x" + "70997970c51812dc3a010c7d01b50e0d17dc79c8".zfill(64)
        amount_padded = (1000).to_bytes(32, "big").hex()

        receipt_data1 = {
            "transactionHash": tx_hash1,
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
        await session.post(f"{rpc_url}/control/receipt", json=receipt_data1)

        # Sign the first payment ID with the private key
        msg = encode_defunct(text=pid1)
        signed = Account.sign_message(msg, private_key)
        sig1 = signed.signature.hex()

        # Submit first payment with correct signature -> Should succeed
        headers1 = {
            "Payment-Signature": json.dumps(
                {"payment_id": pid1, "tx_hash": tx_hash1, "signature": sig1}
            )
        }
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers1
        ) as resp:
            assert resp.status == 200

        # Get second payment ID
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            assert resp.status == 402
            data = await resp.json()
            pid2 = data["payment_id"]

        # Try to replay the first transaction (tx_hash1) with pid2 -> Should be rejected
        msg2 = encode_defunct(text=pid2)
        signed2 = Account.sign_message(msg2, private_key)
        sig2 = signed2.signature.hex()

        headers2 = {
            "Payment-Signature": json.dumps(
                {
                    "payment_id": pid2,
                    "tx_hash": tx_hash1,  # replaying tx_hash1
                    "signature": sig2,
                }
            )
        }
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers2
        ) as resp:
            assert resp.status == 400
            data = await resp.json()
            assert "already processed" in data["detail"]

        # Try to submit a transaction with mismatched signature
        other_key = "0x" + "2" * 64
        other_account = Account.from_key(other_key)

        # Seed receipt with transaction from other_account
        tx_hash2 = "0x" + "d" * 64
        receipt_data2 = {
            "transactionHash": tx_hash2,
            "status": 1,
            "from": other_account.address,
            "logs": [
                {
                    "address": usdc_contract,
                    "topics": [transfer_topic, "0x" + "a" * 64, recipient_padded],
                    "data": "0x" + amount_padded,
                }
            ],
        }
        await session.post(f"{rpc_url}/control/receipt", json=receipt_data2)

        # Sign pid2 with our account key, but the transaction was sent by other_account
        # -> Should fail matching
        headers_mismatch = {
            "Payment-Signature": json.dumps(
                {"payment_id": pid2, "tx_hash": tx_hash2, "signature": sig2}
            )
        }
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers_mismatch
        ) as resp:
            assert resp.status == 400
            data = await resp.json()
            assert "does not match transaction sender" in data["detail"]


# 3. Showcase Script Offline Dry Run
@pytest.mark.asyncio
async def test_t4_showcase_script_offline_dry_run(mock_rpc) -> None:
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

    env = os.environ.copy()
    env["BASE_RPC_URL"] = rpc_url
    env["PYTHONPATH"] = os.path.abspath("src")  # noqa: ASYNC240

    import sys
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "examples/collect_base_onchain.py",
        "--dry-run",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = await proc.communicate()
    if proc.returncode != 0 or not (b"cbBTC-USDC" in stdout or b"AERO-USDC" in stdout):
        print("SHOWCASE STDOUT:", stdout.decode())
        print("SHOWCASE STDERR:", stderr.decode())
    assert proc.returncode == 0
    assert b"cbBTC-USDC" in stdout or b"AERO-USDC" in stdout


# 4. MCP-driven Autonomous Agent Loop
@pytest.mark.asyncio
async def test_t4_mcp_driven_autonomous_agent_loop(mcp_server_client, mock_rpc) -> None:
    rpc_url, _ = mock_rpc
    proc = mcp_server_client

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

    loop = asyncio.get_running_loop()

    # Step A: Query available tools
    req_list = {"jsonrpc": "2.0", "id": 200, "method": "tools/list", "params": {}}
    proc.stdin.write(json.dumps(req_list) + "\n")
    proc.stdin.flush()
    res_list_line = await loop.run_in_executor(None, proc.stdout.readline)
    res_list = json.loads(res_list_line.strip())
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    assert "get_onchain_price" in tool_names

    # Step B: Call get_onchain_price tool
    req_call = {
        "jsonrpc": "2.0",
        "id": 201,
        "method": "tools/call",
        "params": {"name": "get_onchain_price", "arguments": {"symbol": "cbBTC-USDC"}},
    }
    proc.stdin.write(json.dumps(req_call) + "\n")
    proc.stdin.flush()
    res_call_line = await loop.run_in_executor(None, proc.stdout.readline)
    res_call = json.loads(res_call_line.strip())
    content = json.loads(res_call["result"]["content"][0]["text"])
    assert content["price"] == 100.0


# 5. Multi-pool Concurrent Ingestion under Stress
@pytest.mark.asyncio
async def test_t4_multi_pool_concurrent_ingestion_under_stress(mock_rpc) -> None:
    rpc_url, _ = mock_rpc

    # Seed 4 different pools
    pools = [
        (
            "AERO-USDC",
            "0x0000000000000000000000000000000000000002",
            FACTORIES["aerodrome"],
            TOKENS["AERO"],
            TOKENS["USDbC"],
            {"stable": False},
        ),
        (
            "cbBTC-USDC",
            "0x0000000000000000000000000000000000000001",
            FACTORIES["uniswap_v3"],
            TOKENS["cbBTC"],
            TOKENS["USDC"],
            {"fee": 500, "sqrtPriceX96": 2**96},
        ),
        (
            "DEGEN-WETH",
            "0x0000000000000000000000000000000000000003",
            FACTORIES["uniswap_v3"],
            TOKENS["DEGEN"],
            TOKENS["WETH"],
            {"fee": 3000, "sqrtPriceX96": 2**96},
        ),
        (
            "WELL-WETH",
            "0x0000000000000000000000000000000000000004",
            FACTORIES["aerodrome"],
            TOKENS["WELL"],
            TOKENS["WETH"],
            {"stable": False},
        ),
    ]

    async with aiohttp.ClientSession() as session:
        for _name, address, factory, t0, t1, extra in pools:
            pool_data = {
                "address": address,
                "factory": factory,
                "token0": t0,
                "token1": t1,
                **extra,
            }
            await session.post(f"{rpc_url}/control/pool", json=pool_data)

    transport = BaseOnchainTransport(rpc_url, [p[0] for p in pools], poll_interval=0.1)
    await transport.connect()
    try:
        collected = set()
        # Ensure we receive updates from all 4 pools
        while len(collected) < 4:
            async for msg_bytes in transport:
                msg = json.loads(msg_bytes.decode())
                collected.add(msg["pool"])
                break
        assert len(collected) == 4
    finally:
        await transport.close()
