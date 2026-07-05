import asyncio
import json

import aiohttp
import pytest
import subprocess


@pytest.mark.asyncio
async def test_mock_rpc_server_query(mock_rpc: tuple[str, int]) -> None:
    """Verify the Mock RPC server can be queried via JSON-RPC."""
    rpc_url, _ = mock_rpc

    payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}

    async with aiohttp.ClientSession() as session:
        async with session.post(rpc_url, json=payload) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["jsonrpc"] == "2.0"
            assert data["id"] == 1
            assert data["result"] == "0x3e8"  # Hex of 1000


@pytest.mark.asyncio
async def test_api_server_payment_flow(mock_rpc: tuple[str, int], api_server: str) -> None:
    """Verify that the api_server gates requests and supports simulation payment flow."""
    rpc_url, _ = mock_rpc

    # 1. Seed the Mock RPC server with a Uniswap V3 Pool for cbBTC-USDC
    # cbBTC: 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf
    # USDC: 0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913
    # Factory: 0x33128a8fC17869897dcE68Ed026d694621f6FDfD
    pool_data = {
        "address": "0x0000000000000000000000000000000000000001",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "token0": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "token1": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "fee": 500,
        "sqrtPriceX96": 2**96 * 2,  # Corresponds to a mock ratio
        "tick": 0,
        "liquidity": 10000000000,
    }

    async with aiohttp.ClientSession() as session:
        # Seed pool state in mock RPC server
        async with session.post(f"{rpc_url}/control/pool", json=pool_data) as rpc_resp:
            assert rpc_resp.status == 200
            text = await rpc_resp.text()
            assert text == "Pool seeded"

        # 2. Query market data without payment signature -> Expected 402
        async with session.get(f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC") as resp:
            assert resp.status == 402
            data = await resp.json()
            assert data["status"] == "payment_required"
            payment_req = data["payment_required"]
            payment_id = payment_req["payment_id"]

        # 3. Simulate payment on API server
        from eth_account import Account
        from eth_account.messages import encode_defunct

        private_key = "0x" + "1" * 64
        account = Account.from_key(private_key)
        msg = encode_defunct(text=payment_id)
        sig = account.sign_message(msg).signature.hex()
        if not sig.startswith("0x"):
            sig = "0x" + sig
        sim_payload = {"payment_id": payment_id, "tx_hash": "0xmocktxhash", "signature": sig}
        async with session.post(f"{api_server}/api/v1/simulate-payment", json=sim_payload) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "success"

        # 4. Query market data again with payment signature -> Expected 200
        headers = {"Payment-Signature": json.dumps(sim_payload)}
        async with session.get(
            f"{api_server}/api/v1/market-data?symbol=cbBTC-USDC", headers=headers
        ) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "success"
            assert data["data"]["symbol"] == "cbBTC-USDC"
            assert data["data"]["pool_address"] == "0x0000000000000000000000000000000000000001"


@pytest.mark.asyncio
async def test_mcp_server_launch(mcp_server_client: subprocess.Popen[str]) -> None:
    """Verify the MCP server can be launched and queried via stdio JSON-RPC."""
    proc = mcp_server_client

    # Send initialize message to stdin
    init_msg = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}

    proc.stdin.write(json.dumps(init_msg) + "\n")
    proc.stdin.flush()

    # Read response from stdout
    loop = asyncio.get_running_loop()
    response_line = await loop.run_in_executor(None, proc.stdout.readline)
    assert response_line, "MCP server closed stdout without response"

    resp_data = json.loads(response_line.strip())
    assert resp_data["jsonrpc"] == "2.0"
    assert resp_data["id"] == 1
    assert "capabilities" in resp_data["result"]
    assert resp_data["result"]["serverInfo"]["name"] == "stockodile-mcp"
