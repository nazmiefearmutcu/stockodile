from __future__ import annotations

import asyncio
import logging
from typing import Any

from aiohttp import web

log = logging.getLogger("mock_rpc_server")


class MockRPCServer:
    def __init__(self) -> None:
        self.block_number: int = 1000
        self.block_timestamp: int = 1700000000
        self.pools: dict[str, dict[str, int]] = {}  # pool_address -> pool_state dict
        self.factories: dict[
            tuple[str, str, str, int | bool], str
        ] = {}  # (factory_address, token0, token1, fee_or_stable) -> pool_address
        self.logs: list[dict[str, Any]] = []  # list of raw log objects
        self.receipts: dict[str, dict[str, Any]] = {}  # tx_hash -> receipt dict
        self.behavior: dict[str, Any] = {"status_code": 200, "error_count": 0, "delay": 0.0}
        self.history: list[dict[str, Any]] = []

    def reset(self) -> None:
        self.block_number = 1000
        self.block_timestamp = 1700000000
        self.pools.clear()
        self.factories.clear()
        self.logs.clear()
        self.receipts.clear()
        self.behavior = {"status_code": 200, "error_count": 0, "delay": 0.0}
        self.history.clear()

    async def handle_rpc(self, request: web.Request) -> web.Response:
        if self.behavior.get("delay", 0.0) > 0:
            await asyncio.sleep(self.behavior["delay"])

        if self.behavior.get("error_count", 0) > 0:
            self.behavior["error_count"] -= 1
            return web.Response(status=int(self.behavior["status_code"]), text="Simulated Failure")

        try:
            body = await request.json()
        except Exception:
            return web.json_response(
                {"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}}, status=400
            )

        if isinstance(body, list):
            results = [await self._process_single_rpc(req) for req in body]
            return web.json_response(results)
        else:
            result = await self._process_single_rpc(body)
            return web.json_response(result)

    async def _process_single_rpc(self, req: dict[str, Any]) -> dict[str, Any]:
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", [])
        self.history.append({"method": method, "params": params})

        result: Any = None
        error: dict[str, Any] | None = None

        try:
            if method == "eth_blockNumber":
                result = hex(self.block_number)

            elif method == "eth_chainId":
                result = "0x2105"  # Base mainnet chain ID (8453)

            elif method == "net_version":
                result = "8453"

            elif method == "eth_getBlockByNumber":
                if len(params) > 0:
                    tag = params[0]
                    if tag in ("latest", "pending"):
                        blk_num = self.block_number
                    else:
                        blk_num = int(tag, 16)
                else:
                    blk_num = self.block_number
                result = {
                    "number": hex(blk_num),
                    "timestamp": hex(self.block_timestamp),
                    "hash": "0x" + str(blk_num).zfill(64),
                    "transactions": [],
                }

            elif method == "eth_getLogs":
                filter_obj = params[0] if len(params) > 0 else {}
                from_blk_val = filter_obj.get("fromBlock")
                if isinstance(from_blk_val, int):
                    from_blk = from_blk_val
                elif isinstance(from_blk_val, str):
                    from_blk = int(from_blk_val, 16)
                else:
                    from_blk = 0

                to_blk_val = filter_obj.get("toBlock")
                if isinstance(to_blk_val, int):
                    to_blk = to_blk_val
                elif isinstance(to_blk_val, str):
                    if to_blk_val in ("latest", "pending"):
                        to_blk = self.block_number
                    else:
                        to_blk = int(to_blk_val, 16)
                else:
                    to_blk = self.block_number

                addr = filter_obj.get("address")
                topics = filter_obj.get("topics", [])

                matched = []
                for lg in self.logs:
                    lg_blk_val = lg.get("blockNumber", 0)
                    lg_blk = lg_blk_val if isinstance(lg_blk_val, int) else int(lg_blk_val, 16)
                    if from_blk <= lg_blk <= to_blk:
                        addr_matched = True
                        if addr:
                            if isinstance(addr, list):
                                addr_matched = lg.get("address", "").lower() in [
                                    a.lower() for a in addr
                                ]
                            else:
                                addr_matched = lg.get("address", "").lower() == addr.lower()
                        if addr_matched:
                            if not topics or not lg.get("topics") or lg["topics"][0] == topics[0]:
                                matched.append(lg)
                result = matched

            elif method == "eth_getTransactionReceipt":
                tx_hash = params[0] if len(params) > 0 else ""
                result = self.receipts.get(tx_hash)

            elif method == "eth_getTransactionByHash":
                tx_hash = params[0] if len(params) > 0 else ""
                receipt = self.receipts.get(tx_hash)
                if receipt:
                    result = {
                        "hash": tx_hash,
                        "from": receipt.get("from", "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"),
                        "to": receipt.get("to", "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913"),
                        "value": "0x0",
                        "input": "0x",
                    }
                else:
                    result = None

            elif method == "eth_call":
                tx_obj = params[0] if len(params) > 0 else {}
                to_addr = tx_obj.get("to", "").lower()
                data = tx_obj.get("data", "")

                result = await self._handle_eth_call(to_addr, data)

            else:
                error = {"code": -32601, "message": f"Method {method} not supported"}
        except Exception as e:
            error = {"code": -32603, "message": f"Execution error: {e!s}"}

        response = {"jsonrpc": "2.0", "id": req_id}
        if error:
            response["error"] = error
        else:
            response["result"] = result
        return response

    async def _handle_eth_call(self, to_addr: str, data: str) -> str:
        selector = data[:10]
        if selector in ("0x1698ee43", "0x1698ee82", "0x990f1d5d", "0x79bc57d5"):
            t0 = "0x" + data[34:74].lower()
            t1 = "0x" + data[98:138].lower()
            if selector in ("0x1698ee43", "0x1698ee82"):
                fee = int(data[138:202], 16)
                for (f_addr, tok0, tok1, param), pool in self.factories.items():
                    if f_addr.lower() == to_addr:
                        if (tok0.lower(), tok1.lower()) in ((t0, t1), (t1, t0)) and param == fee:
                            return "0x" + pool[2:].zfill(64)
            else:
                stable = bool(int(data[138:202], 16))
                for (f_addr, tok0, tok1, param), pool in self.factories.items():
                    if f_addr.lower() == to_addr:
                        if (tok0.lower(), tok1.lower()) in ((t0, t1), (t1, t0)) and bool(
                            param
                        ) == stable:
                            return "0x" + pool[2:].zfill(64)
            return "0x" + "0".zfill(64)

        elif selector == "0x3850c7bd":
            state = self.pools.get(to_addr, {})
            sqrtPriceX96 = int(state.get("sqrtPriceX96", 0))
            tick = int(state.get("tick", 0))
            res = (
                sqrtPriceX96.to_bytes(32, "big").hex()
                + tick.to_bytes(32, "big", signed=True).hex()
                + (0).to_bytes(32, "big").hex() * 4
                + (1).to_bytes(32, "big").hex()
            )
            return "0x" + res

        elif selector in ("0x1a6828d9", "0x1a686502"):
            state = self.pools.get(to_addr, {})
            liquidity = int(state.get("liquidity", 0))
            return "0x" + liquidity.to_bytes(32, "big").hex()

        elif selector == "0x0902f1ac":
            state = self.pools.get(to_addr, {})
            r0 = int(state.get("reserve0", 0))
            r1 = int(state.get("reserve1", 0))
            ts = int(state.get("timestamp", self.block_timestamp))
            res = (
                r0.to_bytes(32, "big").hex()
                + r1.to_bytes(32, "big").hex()
                + ts.to_bytes(32, "big").hex()
            )
            return "0x" + res

        raise ValueError(f"Unknown selector {selector} on contract {to_addr}")

    async def set_block(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.block_number = int(body.get("block_number", self.block_number))
        self.block_timestamp = int(body.get("timestamp", self.block_timestamp))
        return web.Response(text="Block state updated")

    async def seed_pool(self, request: web.Request) -> web.Response:
        body = await request.json()
        addr = body["address"].lower()
        self.pools[addr] = {
            "sqrtPriceX96": int(body.get("sqrtPriceX96", 0)),
            "tick": int(body.get("tick", 0)),
            "liquidity": int(body.get("liquidity", 0)),
            "reserve0": int(body.get("reserve0", 0)),
            "reserve1": int(body.get("reserve1", 0)),
            "timestamp": int(body.get("timestamp", self.block_timestamp)),
        }
        factory_addr = body["factory"].lower()
        t0 = body["token0"].lower()
        t1 = body["token1"].lower()
        param = int(body.get("fee")) if "fee" in body else bool(body.get("stable"))

        self.factories[(factory_addr, t0, t1, param)] = addr
        return web.Response(text="Pool seeded")

    async def seed_receipt(self, request: web.Request) -> web.Response:
        body = await request.json()
        tx_hash = body["transactionHash"]
        self.receipts[tx_hash] = {
            "transactionHash": tx_hash,
            "status": hex(body.get("status", 1)),
            "blockNumber": hex(body.get("blockNumber", self.block_number)),
            "logs": body.get("logs", []),
            "from": body.get("from", "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"),
            "to": body.get("to", "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913"),
        }
        return web.Response(text="Receipt seeded")

    async def seed_logs(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.logs.extend(body.get("logs", []))
        return web.Response(text="Logs seeded")

    async def set_behavior(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.behavior.update(body)
        return web.Response(text="Behavior updated")

    async def reset_endpoint(self, request: web.Request) -> web.Response:
        self.reset()
        return web.Response(text="Reset complete")

    async def get_history(self, request: web.Request) -> web.Response:
        return web.json_response({"history": self.history})


async def start_mock_server(host: str = "127.0.0.1", port: int = 0) -> tuple[web.AppRunner, int]:
    server = MockRPCServer()
    app = web.Application()
    app.router.add_post("/", server.handle_rpc)
    app.router.add_post("/control/block", server.set_block)
    app.router.add_post("/control/pool", server.seed_pool)
    app.router.add_post("/control/receipt", server.seed_receipt)
    app.router.add_post("/control/logs", server.seed_logs)
    app.router.add_post("/control/behavior", server.set_behavior)
    app.router.add_post("/control/reset", server.reset_endpoint)
    app.router.add_get("/control/history", server.get_history)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    actual_port = runner.addresses[0][1]
    return runner, actual_port
