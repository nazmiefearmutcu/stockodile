# ruff: noqa: E501, E402, I001
from __future__ import annotations

import asyncio
import json
import sys
import traceback
from pathlib import Path
from typing import Any, cast

import web3
from web3 import AsyncHTTPProvider


class AsyncWeb3(web3.AsyncWeb3[Any]):
    async def __aenter__(self) -> AsyncWeb3:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            provider = getattr(self, "provider", None)
            if provider is not None:
                disconnect_fn = getattr(provider, "disconnect", None)
                if disconnect_fn is not None:
                    import inspect

                    res = disconnect_fn()
                    if inspect.isawaitable(res):
                        await res
        except (AttributeError, Exception):
            pass


from stockodile import __version__
import os

DEFAULT_RPC_URL = os.getenv("BASE_RPC_URL", "https://base-rpc.publicnode.com")

from stockodile.exchanges.base_onchain.connector import FACTORIES, TOKENS, POOL_SPECS, _load_ipc

# Minimal ABIs for slot0 and getReserves
POOL_V3_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"type": "uint128"}],
        "stateMutability": "view",
        "type": "function",
    },
]

POOL_V2_ABI = [
    {
        "inputs": [],
        "name": "getReserves",
        "outputs": [
            {"name": "_reserve0", "type": "uint256"},
            {"name": "_reserve1", "type": "uint256"},
            {"name": "_blockTimestampLast", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

# Factory ABIs
FACTORY_V3_ABI = [
    {
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
            {"name": "fee", "type": "uint24"},
        ],
        "name": "getPool",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
]

FACTORY_AERO_ABI = [
    {
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
            {"name": "stable", "type": "bool"},
        ],
        "name": "getPool",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
]


def _get_rpc_urls() -> list[str]:
    urls_str = os.getenv("BASE_RPC_URLS", "")
    if urls_str:
        return [u.strip() for u in urls_str.split(",") if u.strip()]
    fallback = os.getenv("BASE_RPC_URL", "https://base-rpc.publicnode.com")
    return [fallback]


import random


async def execute_with_retry_and_failover(rpc_url_arg: str, callback: Any) -> Any:
    """
    Executes a callback that takes an AsyncWeb3 instance.
    If the call fails due to connection or rate limit errors,
    retries with exponential backoff and failover to other RPC URLs.
    """
    if rpc_url_arg == DEFAULT_RPC_URL:
        urls = _get_rpc_urls()
    else:
        pool_urls = _get_rpc_urls()
        urls = [rpc_url_arg] + [u for u in pool_urls if u != rpc_url_arg]

    if not urls:
        urls = [DEFAULT_RPC_URL]

    max_attempts_per_url = 3
    base_delay = 0.5
    max_delay = 5.0
    last_exception = None

    for url in urls:
        for attempt in range(max_attempts_per_url):
            try:
                async with AsyncWeb3(AsyncHTTPProvider(url)) as w3:
                    return await callback(w3)
            except Exception as e:
                err_str = str(e).lower()
                is_retryable = (
                    "429" in err_str
                    or "rate limit" in err_str
                    or any(
                        kw in err_str
                        for kw in [
                            "connection",
                            "timeout",
                            "connect",
                            "refused",
                            "disconnected",
                            "502",
                            "503",
                            "504",
                            "http status",
                            "http error",
                            "status code 429",
                        ]
                    )
                )
                if not is_retryable:
                    raise e

                last_exception = e
                delay = min(max_delay, base_delay * (2**attempt))
                delay = delay * random.uniform(0.5, 1.5)
                sys.stderr.write(
                    f"RPC error on {url} (attempt {attempt + 1}/{max_attempts_per_url}): {e}. "
                    f"Retrying in {delay:.2f}s...\n"
                )
                sys.stderr.flush()
                await asyncio.sleep(delay)

    raise last_exception if last_exception else Exception("RPC failover exhausted without success")


async def get_onchain_price(symbol: str, rpc_url: str = DEFAULT_RPC_URL) -> dict[str, Any]:
    """Helper to fetch price and reserve stats from Base mainnet or stock tickers from Yahoo Finance."""
    try:
        await _load_ipc()
    except Exception:
        pass
    spec = cast(dict[str, Any], POOL_SPECS.get(symbol))
    if not spec and "CUSTOM" in symbol:
        spec = {
            "type": "uniswap_v3",
            "token0": "cbBTC",
            "token1": "USDC",
            "fee": 500,
            "decimals0": 8,
            "decimals1": 6,
        }
        POOL_SPECS[symbol] = spec
    if not spec:
        # Fallback to Yahoo Finance for stock tickers (e.g. AAPL, AAPL-USD, AAPL/USD)
        clean_symbol = symbol.replace("/", "-").upper()
        ticker_sym = clean_symbol.split("-")[0]
        if ticker_sym in {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "NFLX", "META"}:
            try:
                import yfinance as yf

                ticker = yf.Ticker(ticker_sym)
                history = ticker.history(period="1d", timeout=2)
                if history.empty:
                    return {
                        "error": f"Symbol {symbol} not supported. Supported: {list(POOL_SPECS.keys())}"
                    }
                price = float(history["Close"].iloc[-1])
                return {
                    "symbol": symbol,
                    "pool_address": "equity_feed",
                    "price": price,
                    "reserve0": 0.0,
                    "reserve1": 0.0,
                    "pool_type": "equity_market",
                    "block": 0,
                }
            except Exception as e:
                return {"error": f"Failed fetching stock state for {symbol}: {e}"}

        return {
            "error": f"Symbol {symbol} not supported. Supported: {list(POOL_SPECS.keys())}"
        }

    async def query_price(w3: AsyncWeb3) -> dict[str, Any]:
        t0_addr = AsyncWeb3.to_checksum_address(TOKENS[str(spec["token0"])])
        t1_addr = AsyncWeb3.to_checksum_address(TOKENS[str(spec["token1"])])

        # 1. Resolve pool address
        if spec["type"] == "uniswap_v3":
            sorted_t0, sorted_t1 = sorted([t0_addr, t1_addr], key=lambda x: int(x, 16))
            factory = w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(FACTORIES["uniswap_v3"]), abi=FACTORY_V3_ABI
            )
            pool_addr = await factory.functions.getPool(
                sorted_t0, sorted_t1, int(spec["fee"])
            ).call()
        else:
            factory = w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(FACTORIES["aerodrome"]), abi=FACTORY_AERO_ABI
            )
            pool_addr = await factory.functions.getPool(
                t0_addr, t1_addr, bool(spec["stable"])
            ).call()

        if pool_addr == "0x0000000000000000000000000000000000000000":
            return {"error": f"Pool for {symbol} not found on Base mainnet."}

        # 2. Query pool state
        price = 0.0
        reserve0 = 0.0
        reserve1 = 0.0
        is_flipped = int(t1_addr, 16) < int(t0_addr, 16)

        if spec["type"] == "uniswap_v3":
            pool_contract = w3.eth.contract(address=pool_addr, abi=POOL_V3_ABI)
            slot0 = await pool_contract.functions.slot0().call()
            liquidity = await pool_contract.functions.liquidity().call()
            sqrtPriceX96 = slot0[0]
            price_ratio = (sqrtPriceX96 / (2**96)) ** 2

            dec_diff = int(spec["decimals0"]) - int(spec["decimals1"])
            if not is_flipped:
                price = price_ratio * (10**dec_diff)
            else:
                price = (1.0 / price_ratio) * (10**dec_diff) if price_ratio > 0 else 0.0

            # Calculate virtual reserves
            sqrtP = sqrtPriceX96 / (2**96)
            x_virtual = liquidity / sqrtP if sqrtP > 0 else 0
            y_virtual = liquidity * sqrtP

            if not is_flipped:
                reserve0 = x_virtual / (10 ** int(spec["decimals0"]))
                reserve1 = y_virtual / (10 ** int(spec["decimals1"]))
            else:
                reserve0 = y_virtual / (10 ** int(spec["decimals0"]))
                reserve1 = x_virtual / (10 ** int(spec["decimals1"]))
        else:
            pool_contract = w3.eth.contract(address=pool_addr, abi=POOL_V2_ABI)
            res = await pool_contract.functions.getReserves().call()
            if not is_flipped:
                reserve0 = res[0] / (10 ** int(spec["decimals0"]))
                reserve1 = res[1] / (10 ** int(spec["decimals1"]))
            else:
                reserve0 = res[1] / (10 ** int(spec["decimals0"]))
                reserve1 = res[0] / (10 ** int(spec["decimals1"]))
            price = reserve1 / reserve0 if reserve0 > 0 else 0.0

        import inspect

        raw_block = w3.eth.block_number
        if inspect.isawaitable(raw_block):
            block_num = await raw_block
        else:
            block_num = raw_block
        return {
            "symbol": symbol,
            "pool_address": pool_addr,
            "price": price,
            "reserve0": reserve0,
            "reserve1": reserve1,
            "pool_type": spec["type"],
            "block": block_num,
        }

    try:
        return cast(dict[str, Any], await execute_with_retry_and_failover(rpc_url, query_price))
    except Exception as e:
        return {"error": f"Failed fetching pool state: {e}"}


async def get_base_market_data(token_pair: str, rpc_url: str = DEFAULT_RPC_URL) -> dict[str, Any]:
    """Fetch real-time price, reserves, and 1-hour volume for a token pair or stock ticker."""
    try:
        await _load_ipc()
    except Exception:
        pass
    symbol = token_pair.replace("/", "-").upper()

    spec = cast(dict[str, Any], POOL_SPECS.get(symbol))
    if not spec:
        # Fallback to Yahoo Finance for stock tickers (e.g. AAPL/USD, MSFT/USD)
        ticker_sym = symbol.split("-")[0]
        if ticker_sym in {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "NFLX", "META"}:
            try:
                import yfinance as yf

                ticker = yf.Ticker(ticker_sym)
                history = ticker.history(period="1d", timeout=2)
                if history.empty:
                    return {"error": f"Symbol {symbol} not supported."}
                price = float(history["Close"].iloc[-1])
                volume = float(history["Volume"].iloc[-1])
                volume_1h = volume / 7.0 if volume else 0.0
                return {
                    "symbol": symbol,
                    "pool_address": "equity_feed",
                    "price": price,
                    "reserve0": 0.0,
                    "reserve1": 0.0,
                    "pool_type": "equity_market",
                    "block": 0,
                    "volume_1h_base": volume_1h,
                    "volume_1h_quote": volume_1h * price,
                    "volume_1h_timeframe_blocks": 0,
                    "num_swaps_1h": 0,
                }
            except Exception as e:
                return {"error": f"Failed fetching stock state: {e}"}

        return {"error": f"Symbol {symbol} not supported."}

    state_res = await get_onchain_price(symbol, rpc_url)
    if "error" in state_res:
        return state_res

    async def query_volume(w3: AsyncWeb3) -> dict[str, Any]:
        t0_addr = AsyncWeb3.to_checksum_address(TOKENS[str(spec["token0"])])
        t1_addr = AsyncWeb3.to_checksum_address(TOKENS[str(spec["token1"])])
        pool_addr = state_res["pool_address"]
        is_flipped = int(t1_addr, 16) < int(t0_addr, 16)

        latest_block = await w3.eth.block_number
        from_block = max(0, latest_block - 1800)  # ~1h of blocks

        swap_topic = (
            "0xc42079f94a6350d7e6235f29174924f9287a20ac8e91c97b870daEE5297F6e85"
            if spec["type"] == "uniswap_v3"
            else "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        )

        logs = await w3.eth.get_logs(
            {
                "address": pool_addr,
                "topics": [swap_topic],
                "fromBlock": from_block,
                "toBlock": latest_block,
            }
        )

        volume_1h_base = 0.0
        volume_1h_quote = 0.0

        for lg in logs:
            data = lg["data"]
            if spec["type"] == "uniswap_v3":
                amount0 = int.from_bytes(data[0:32], byteorder="big", signed=True)
                amount1 = int.from_bytes(data[32:64], byteorder="big", signed=True)

                if not is_flipped:
                    abs_base = abs(amount0) / (10 ** int(spec["decimals0"]))
                    abs_quote = abs(amount1) / (10 ** int(spec["decimals1"]))
                else:
                    abs_base = abs(amount1) / (10 ** int(spec["decimals0"]))
                    abs_quote = abs(amount0) / (10 ** int(spec["decimals1"]))
            else:  # aerodrome_v2
                amt0_in = int.from_bytes(data[0:32], byteorder="big", signed=False)
                amt1_in = int.from_bytes(data[32:64], byteorder="big", signed=False)
                amt0_out = int.from_bytes(data[64:96], byteorder="big", signed=False)
                amt1_out = int.from_bytes(data[96:128], byteorder="big", signed=False)

                if not is_flipped:
                    abs_base = (amt0_in if amt0_in > 0 else amt0_out) / (
                        10 ** int(spec["decimals0"])
                    )
                    abs_quote = (amt1_in if amt1_in > 0 else amt1_out) / (
                        10 ** int(spec["decimals1"])
                    )
                else:
                    abs_base = (amt1_in if amt1_in > 0 else amt1_out) / (
                        10 ** int(spec["decimals0"])
                    )
                    abs_quote = (amt0_in if amt0_in > 0 else amt0_out) / (
                        10 ** int(spec["decimals1"])
                    )

            volume_1h_base += abs_base
            volume_1h_quote += abs_quote

        res = dict(state_res)
        res["volume_1h_base"] = volume_1h_base
        res["volume_1h_quote"] = volume_1h_quote
        res["volume_1h_timeframe_blocks"] = latest_block - from_block
        res["num_swaps_1h"] = len(logs)
        return res

    try:
        return cast(dict[str, Any], await execute_with_retry_and_failover(rpc_url, query_volume))
    except Exception as e:
        return {"error": f"Failed fetching 1h volume: {e}"}


# List of tools exposed by the MCP server
TOOLS = [
    {
        "name": "get_base_market_data",
        "description": (
            "Fetch real-time market data (price, reserves, and 1-hour volume) for a "
            "token pair or stock ticker. Supported crypto: AERO/USDC, WETH/USDC, "
            "cbBTC/USDC, DEGEN/WETH, WELL/WETH. Supported stocks: AAPL/USD, MSFT/USD, etc."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "token_pair": {
                    "type": "string",
                    "description": "The token pair or stock ticker symbol (e.g. 'WETH/USDC', 'AAPL/USD').",
                }
            },
            "required": ["token_pair"],
        },
    },
    {
        "name": "get_onchain_price",
        "description": (
            "Fetch real-time price, reserves, and pool stats from Base mainnet "
            "DEX (Uniswap V3 or Aerodrome) or stock price feed. Supported crypto: AERO-USDC, "
            "cbBTC-USDC, DEGEN-WETH, WELL-WETH, WETH-USDC. Supported stocks: AAPL, MSFT, etc."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": (
                        "Symbol name (e.g. 'AERO-USDC', 'cbBTC-USDC', "
                        "'DEGEN-WETH', 'WELL-WETH', 'WETH-USDC', or stock ticker 'AAPL')."
                    ),
                }
            },
            "required": ["symbol"],
        },
    },
    {
        "name": "query_market_data",
        "description": (
            "Execute a DuckDB SQL query against the Stockodile parquet data lake. "
            "Replayed tables: trade, book_snapshot, book_ticker, ohlcv, "
            "funding, basis."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "DuckDB SQL query to execute."}
            },
            "required": ["sql"],
        },
    },
    {
        "name": "get_funding_apr",
        "description": (
            "Analyze perpetual funding rates and print per-event funding APR "
            "and cumulative funding."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": ("Canonical perpetual symbol (e.g., deribit:BTC-PERPETUAL)"),
                },
                "start": {"type": "integer", "description": "Start timestamp in nanoseconds UTC"},
                "end": {"type": "integer", "description": "End timestamp in nanoseconds UTC"},
            },
            "required": ["symbol", "start", "end"],
        },
    },
]


async def serve_stdio(data_dir: Path = Path("data")) -> None:
    """Run the MCP JSON-RPC loop over stdin/stdout."""
    import logging

    logging.basicConfig(stream=sys.stderr, level=logging.INFO, force=True)
    for handler in logging.root.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream is sys.stdout:
            handler.stream = sys.stderr

    from stockodile.client.client import StockodileClient

    client = StockodileClient(data_dir=data_dir)
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        line = await reader.readline()
        if not line:
            break

        try:
            req = json.loads(line.decode())
            if not isinstance(req, dict) or "method" not in req:
                continue

            method = req["method"]
            req_id = req.get("id")

            if method == "initialize":
                resp = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "stockodile-mcp", "version": __version__},
                    },
                }
            elif method == "tools/list":
                resp = {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}
            elif method == "tools/call":
                params = req.get("params", {})
                tool_name = params.get("name")
                arguments = params.get("arguments", {})

                tool_result: Any = None
                if tool_name == "get_base_market_data":
                    pair = arguments.get("token_pair", "")
                    tool_result = await get_base_market_data(pair)
                elif tool_name == "get_onchain_price":
                    sym = arguments.get("symbol", "")
                    tool_result = await get_onchain_price(sym)
                elif tool_name == "query_market_data":
                    sql = arguments.get("sql", "")
                    try:
                        # Network-facing: enforce read-only allowlist
                        if hasattr(client, "query"):
                            try:
                                df = client.query(sql, readonly=True)
                            except TypeError:
                                # Older client without readonly kwarg
                                from stockodile.store.catalog import assert_readonly_sql

                                assert_readonly_sql(sql)
                                df = client.query(sql)
                        else:
                            raise RuntimeError("query not supported")
                        # Convert polars/pandas DataFrame to dict list
                        tool_result = (
                            df.to_dicts()
                            if hasattr(df, "to_dicts")
                            else cast(Any, df).to_dict(orient="records")
                        )
                    except Exception as e:
                        tool_result = {"error": f"SQL execution failed: {e}"}
                elif tool_name == "get_funding_apr":
                    sym = arguments.get("symbol", "")
                    start = arguments.get("start", 0)
                    end = arguments.get("end", 0)
                    try:
                        if hasattr(client, "funding_apr"):
                            df = client.funding_apr(sym, start, end)
                        else:
                            # Parity fallback: return an empty DataFrame with expected columns
                            import polars as pl

                            df = pl.DataFrame(
                                schema={
                                    "funding_ts": pl.Int64,
                                    "funding_rate": pl.Float64,
                                    "interval_hours": pl.Float64,
                                    "apr": pl.Float64,
                                    "cumulative_funding": pl.Float64,
                                }
                            )
                        tool_result = (
                            df.to_dicts()
                            if hasattr(df, "to_dicts")
                            else cast(Any, df).to_dict(orient="records")
                        )
                    except Exception as e:
                        tool_result = {"error": f"Funding APR analysis failed: {e}"}
                else:
                    tool_result = {"error": f"Tool {tool_name} not found"}

                resp = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(tool_result, indent=2)}]
                    },
                }
            else:
                resp = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Method {method} not found"},
                }

            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()
        except Exception as e:
            err_resp = {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {e}",
                    "data": traceback.format_exc(),
                },
            }
            sys.stdout.write(json.dumps(err_resp) + "\n")
            sys.stdout.flush()
