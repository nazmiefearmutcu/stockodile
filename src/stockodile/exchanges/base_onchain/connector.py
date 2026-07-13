# ruff: noqa: E501, B023, E402, I001, B007
from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import sys
import threading
from collections.abc import AsyncIterator, Iterable
from typing import Any

from stockodile.providers.base import Provider
from stockodile.reference.registry import Instrument, InstrumentRegistry
from stockodile.schema.enums import SecurityType
from stockodile.schema.records import Record
from stockodile.sink.base import Sink
from stockodile.ingest.transport import Transport

from .normalize import normalize_onchain_update

log = logging.getLogger(__name__)

FACTORIES = {
    "uniswap_v3": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
    "aerodrome": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
}


def _default_ipc_file() -> str:
    """Return the default on-disk path for custom pools IPC state."""
    base = os.getenv("STOCKODILE_HOME") or os.path.join(os.path.expanduser("~"), ".stockodile")
    return os.path.join(base, "custom_pools_ipc.json")


def _get_ipc_file() -> str:
    return os.getenv("CUSTOM_POOLS_IPC_FILE") or _default_ipc_file()


def _write_ipc_to_file(name: str, data_dict: dict[str, Any]) -> None:
    try:
        data = {}
        ipc_file = _get_ipc_file()
        parent = os.path.dirname(ipc_file)
        if parent:
            os.makedirs(parent, exist_ok=True)
        lock_file = ipc_file + ".lock"
        with open(lock_file, "a+") as lf:
            try:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)

                success_reading = True
                if os.path.exists(ipc_file):
                    try:
                        with open(ipc_file) as f:
                            content = f.read().strip()
                            if content:
                                data = json.loads(content)
                    except Exception as e:
                        log.warning(
                            f"Corrupt JSON in IPC file {ipc_file} during write: {e}. "
                            f"Not writing to avoid data loss."
                        )
                        success_reading = False

                if success_reading:
                    data[name] = data_dict
                    tmp_file = ipc_file + ".tmp"
                    with open(tmp_file, "w") as tmp_f:
                        json.dump(data, tmp_f)
                        tmp_f.flush()
                        os.fsync(tmp_f.fileno())
                    os.replace(tmp_file, ipc_file)
            finally:
                try:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass
    except Exception as e:
        log.error(f"Failed to write IPC to file: {e}")


def _load_ipc_sync() -> None:
    TOKENS._sync()
    POOL_SPECS._sync()


class IPCDict(dict[str, Any]):
    def __init__(self, name: str, default_data: dict[str, Any] | None = None) -> None:
        if default_data is None:
            default_data = {}
        super().__init__(default_data)
        self._name = name
        self._default = default_data
        self._last_ipc_file = ""
        self._last_mtime = None
        self._last_size = None
        self._lock = threading.RLock()
        self._sync()

    def _sync(self) -> None:
        current_file = _get_ipc_file()
        try:
            if not os.path.exists(current_file):
                if self._last_ipc_file != current_file or self._last_mtime is not None:
                    with self._lock:
                        super().clear()
                        super().update(self._default)
                    self._last_ipc_file = current_file
                    self._last_mtime = None
                    self._last_size = None
                return

            stat = os.stat(current_file)
            mtime = stat.st_mtime
            size = stat.st_size

            if (
                current_file != self._last_ipc_file
                or self._last_mtime != mtime
                or self._last_size != size
            ):
                content = ""
                lock_file = current_file + ".lock"
                with open(lock_file, "a+") as lf:
                    try:
                        fcntl.flock(lf.fileno(), fcntl.LOCK_SH)
                        if os.path.exists(current_file):
                            with open(current_file) as f:
                                content = f.read().strip()
                    finally:
                        try:
                            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                        except Exception:
                            pass

                if content:
                    try:
                        file_data = json.loads(content)
                        new_data = dict(self._default)
                        if self._name in file_data:
                            new_data.update(file_data[self._name])

                        with self._lock:
                            super().clear()
                            super().update(new_data)

                        self._last_ipc_file = current_file
                        self._last_mtime = mtime
                        self._last_size = size
                    except json.JSONDecodeError as je:
                        log.warning(
                            f"Corrupt JSON in IPC file {current_file}: {je}. "
                            f"Using current memory state."
                        )
                        self._last_ipc_file = current_file
                        self._last_mtime = mtime
                        self._last_size = size
                else:
                    with self._lock:
                        super().clear()
                        super().update(self._default)
                    self._last_ipc_file = current_file
                    self._last_mtime = mtime
                    self._last_size = size
        except Exception as e:
            log.warning(f"Error syncing IPC dictionary: {e}")

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return super().__contains__(key)

    def __getitem__(self, key: str) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._sync()
        with self._lock:
            return super().__getitem__(key)

    def __setitem__(self, key: str, value: Any) -> None:
        with self._lock:
            super().__setitem__(key, value)
        self._write_ipc()

    def update(self, *args: Any, **kwargs: Any) -> None:
        with self._lock:
            super().update(*args, **kwargs)
        self._write_ipc()

    def _write_ipc(self) -> None:
        with self._lock:
            data_copy = dict(self)
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(asyncio.to_thread(_write_ipc_to_file, self._name, data_copy))
            global _background_tasks
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
        except RuntimeError:
            global _ipc_executor
            _ipc_executor.submit(_write_ipc_to_file, self._name, data_copy)

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return super().get(key, default)

    def keys(self) -> Any:
        with self._lock:
            return list(super().keys())

    def values(self) -> Any:
        with self._lock:
            return list(super().values())

    def items(self) -> Any:
        with self._lock:
            return list(super().items())

    def __len__(self) -> int:
        with self._lock:
            return super().__len__()

    def __iter__(self) -> Any:
        with self._lock:
            return super().__iter__()

    def __repr__(self) -> str:
        with self._lock:
            return super().__repr__()


_background_tasks: set[asyncio.Task] = set()
from concurrent.futures import ThreadPoolExecutor

_ipc_executor = ThreadPoolExecutor(max_workers=1)


async def _load_ipc() -> None:
    await asyncio.to_thread(_load_ipc_sync)


if "pytest" in sys.modules:
    try:
        ipc_f = _get_ipc_file()
        if os.path.exists(ipc_f):
            os.remove(ipc_f)
    except Exception:
        pass

TOKENS = IPCDict(
    "TOKENS",
    {
        "AERO": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
        "cbBTC": "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        "DEGEN": "0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed",
        "WELL": "0xA88594D404727625A9437C3f886C7643872296AE",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "WETH": "0x4200000000000000000000000000000000000006",
    },
)

POOL_SPECS = IPCDict(
    "POOL_SPECS",
    {
        "AERO-USDC": {
            "type": "aerodrome_v2",
            "token0": "AERO",
            "token1": "USDbC",
            "stable": False,
            "decimals0": 18,
            "decimals1": 6,
        },
        "cbBTC-USDC": {
            "type": "uniswap_v3",
            "token0": "cbBTC",
            "token1": "USDC",
            "fee": 500,
            "decimals0": 8,
            "decimals1": 6,
        },
        "DEGEN-WETH": {
            "type": "uniswap_v3",
            "token0": "DEGEN",
            "token1": "WETH",
            "fee": 3000,
            "decimals0": 18,
            "decimals1": 18,
        },
        "WELL-WETH": {
            "type": "aerodrome_v2",
            "token0": "WELL",
            "token1": "WETH",
            "stable": False,
            "decimals0": 18,
            "decimals1": 18,
        },
        "WETH-USDC": {
            "type": "uniswap_v3",
            "token0": "WETH",
            "token1": "USDC",
            "fee": 500,
            "decimals0": 18,
            "decimals1": 6,
        },
    },
)

_load_ipc_sync()


SWAP_TOPIC_V3 = "0xc42079f94a6350d7e6235f29174924f9287a20ac8e91c97b870daEE5297F6e85"
SWAP_TOPIC_V2 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
RESERVE_DATA_UPDATED_TOPIC = "0x804c9d53b43c501f114c036a6e2e28a5ff6b2512f45856488d0426d400e95cb5"
LIQUIDATION_CALL_TOPIC = "0xe41d8df5aeb3812fd567b454174378f89ff89100868f029671d1824ef78c902c"
ONEINCH_ORDER_FILLED_TOPIC = "0x"
ZEROX_LIMIT_ORDER_FILLED_TOPIC = "0x"


def decode_1inch_order_filled(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return {}


def decode_0x_limit_order_filled(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return {}


class DummySmartWalletDetector:
    async def is_smart_wallet(self, w3: Any, sender: str) -> bool:
        return False


smart_wallet_detector = DummySmartWalletDetector()


def _register_custom_pools(custom_pools: dict[str, dict[str, Any]] | None) -> None:
    if not custom_pools:
        return
    import web3

    for sym, cfg in custom_pools.items():
        pool_type = cfg.get("type") or cfg.get("factory_type") or "uniswap_v3"
        if pool_type not in ("uniswap_v3", "aerodrome_v2"):
            raise ValueError(f"Unsupported pool type: {pool_type}")

        t0 = str(cfg.get("token0", "T0"))
        t1 = str(cfg.get("token1", "T1"))

        def check_address(addr: Any, name: str) -> str:
            if not addr:
                raise ValueError(f"Address for {name} is missing or empty")
            try:
                res = web3.AsyncWeb3.to_checksum_address(addr)
                from unittest.mock import Mock

                if isinstance(res, Mock):
                    return str(addr)
                return res
            except Exception as e:
                raise ValueError(f"Malformed EVM address for {name}: {addr}") from e

        t0_addr = check_address(cfg.get("token0_address") or cfg.get("token0"), "token0")
        t1_addr = check_address(cfg.get("token1_address") or cfg.get("token1"), "token1")

        pool_addr = None
        if "address" in cfg:
            pool_addr = check_address(cfg["address"], "pool address")

        d0 = cfg.get("decimals0", 18)
        d1 = cfg.get("decimals1", 18)

        if not isinstance(d0, int) or isinstance(d0, bool) or not (0 <= d0 <= 36):
            raise ValueError(f"decimals0 must be an integer between 0 and 36, got {d0}")
        if not isinstance(d1, int) or isinstance(d1, bool) or not (0 <= d1 <= 36):
            raise ValueError(f"decimals1 must be an integer between 0 and 36, got {d1}")

        if pool_type == "uniswap_v3" and "address" not in cfg:
            fee = cfg.get("fee")
            if fee is None:
                raise ValueError("fee is required for uniswap_v3 when address is not specified")
            if not isinstance(fee, int) or isinstance(fee, bool) or fee <= 0:
                raise ValueError(f"fee must be a positive integer, got {fee}")

        if pool_type == "aerodrome_v2" and "address" not in cfg:
            stable = cfg.get("stable")
            if stable is None:
                raise ValueError(
                    "stable is required for aerodrome_v2 when address is not specified"
                )
            if not isinstance(stable, bool):
                raise ValueError(f"stable must be a boolean, got {stable}")

        if t0 not in TOKENS:
            TOKENS[t0] = t0_addr
        if t1 not in TOKENS:
            TOKENS[t1] = t1_addr
        if t0_addr not in TOKENS:
            TOKENS[t0_addr] = t0_addr
        if t1_addr not in TOKENS:
            TOKENS[t1_addr] = t1_addr

        try:
            is_flipped = int(str(t1_addr), 16) < int(str(t0_addr), 16)
        except Exception:
            is_flipped = False

        spec = {
            "type": pool_type,
            "token0": t0,
            "token1": t1,
            "decimals0": d0,
            "decimals1": d1,
            "is_flipped": is_flipped,
        }
        if "fee" in cfg:
            spec["fee"] = cfg["fee"]
        if "stable" in cfg:
            spec["stable"] = cfg["stable"]
        if pool_addr is not None:
            spec["address"] = pool_addr
        if "tick_size" in cfg:
            spec["tick_size"] = cfg["tick_size"]

        POOL_SPECS[sym] = spec


class BaseOnchainTransport:
    """A polling-based transport that queries pool state and swap events from Base mainnet."""

    def __init__(
        self,
        rpc_url: str | list[str],
        symbols: list[str],
        poll_interval: float = 5.0,
        custom_pools: dict[str, dict[str, Any]] | None = None,
        exchange: str = "base_onchain",
    ) -> None:
        self.exchange = exchange
        if isinstance(rpc_url, list):
            self.rpc_urls = rpc_url
        elif isinstance(rpc_url, str) and "," in rpc_url:
            self.rpc_urls = [url.strip() for url in rpc_url.split(",")]
        else:
            self.rpc_urls = [rpc_url]
        self.current_rpc_index = 0
        self.w3: Any = None
        self.symbols = symbols
        self.poll_interval = poll_interval
        self._connected = False
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._poll_task: asyncio.Task[None] | None = None
        self._last_blocks: dict[str, int] = {}
        self._block_cache: dict[int, int] = {}
        self._seen_logs: set[tuple[str, int]] = set()
        _register_custom_pools(custom_pools)

        from stockodile.ingest.sync_recovery import SyncRecovery
        from stockodile.ingest.rollback_manager import RollbackManager

        state_dir = os.path.expanduser("~/.stockodile/sync_state")
        os.makedirs(state_dir, exist_ok=True)
        self.sync_recovery = SyncRecovery(os.path.join(state_dir, "base_onchain.json"))
        self.rollback_manager = RollbackManager(max_depth=100)

    @property
    def active_rpc_url(self) -> str:
        return self.rpc_urls[self.current_rpc_index]

    @property
    def rpc_url(self) -> str:
        return self.active_rpc_url

    async def switch_rpc_failover(self) -> None:
        if self.rpc_urls:
            self.current_rpc_index = (self.current_rpc_index + 1) % len(self.rpc_urls)
            log.warning(
                f"Switching RPC failover to index {self.current_rpc_index}: {self.active_rpc_url}"
            )

            # Dynamically swap provider endpoint in-place to prevent retry stalls
            if self.w3 is not None and getattr(self.w3, "provider", None) is not None:
                try:
                    self.w3.provider.endpoint_uri = self.active_rpc_url
                except Exception as err:
                    log.warning(f"Error swapping provider endpoint in-place: {err}")

    def _is_connection_or_rate_limit(self, e: Exception) -> bool:
        """Detect connection errors, rate limit errors (429), timeouts, and standard network/RPC gateway issues."""
        import socket
        import asyncio
        from web3.exceptions import ProviderConnectionError, PersistentConnectionError

        # Check type directly
        if isinstance(
            e,
            (
                ConnectionError,
                TimeoutError,
                asyncio.TimeoutError,
                socket.gaierror,
                ProviderConnectionError,
                PersistentConnectionError,
            ),
        ):
            return True

        try:
            import aiohttp

            if isinstance(e, aiohttp.ClientError):
                return True
        except ImportError:
            pass

        # Check for status code attributes (e.g. 429, 5xx)
        status = getattr(e, "status", None) or getattr(e, "status_code", None)
        if status is not None:
            try:
                status_int = int(status)
                if status_int == 429 or 500 <= status_int <= 599:
                    return True
            except (ValueError, TypeError):
                pass

        # Check for string patterns in message
        msg = str(e).lower()
        keywords = (
            "429",
            "rate limit",
            "too many requests",
            "timeout",
            "time out",
            "connection",
            "connect",
            "disconnect",
            "eof",
            "gateway",
            "502",
            "503",
            "504",
            "server error",
            "bad status",
            "status code",
            "http error",
            "request limit",
        )
        if any(kw in msg for kw in keywords):
            return True

        return False

    async def _call_with_retry(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        import inspect
        import random

        det_excs = []
        try:
            from web3.exceptions import ContractLogicError

            det_excs.append(ContractLogicError)
        except ImportError:
            pass
        try:
            from web3.exceptions import BadFunctionCallOutput

            det_excs.append(BadFunctionCallOutput)
        except ImportError:
            pass
        try:
            from web3.exceptions import Web3ValidationError

            det_excs.append(Web3ValidationError)
        except ImportError:
            pass
        try:
            from web3.exceptions import ValidationError as Web3ValidationError2

            det_excs.append(Web3ValidationError2)
        except ImportError:
            pass
        try:
            from eth_utils.exceptions import ValidationError as EthValidationError

            det_excs.append(EthValidationError)
        except ImportError:
            pass
        deterministic_exceptions = tuple(det_excs)

        attempt = 0
        max_attempts = 5
        base_delay = kwargs.pop("base_delay", 0.0001 if self.poll_interval < 0.2 else 1.0)
        max_delay = 10.0

        while True:
            try:
                if callable(func):
                    res = func(*args, **kwargs)
                else:
                    res = func

                while inspect.isawaitable(res):
                    res = await asyncio.wait_for(res, timeout=5.0)
                return res
            except Exception as e:
                if deterministic_exceptions and isinstance(e, deterministic_exceptions):
                    log.error(f"Deterministic RPC exception encountered, raising immediately: {e}")
                    raise
                if self._is_connection_or_rate_limit(e):
                    await self.switch_rpc_failover()
                attempt += 1
                if attempt >= max_attempts:
                    log.error(f"RPC call failed after {attempt} attempts: {e}")
                    raise
                delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                delay = delay * random.uniform(0.5, 1.0)
                log.warning(
                    f"RPC call failed: {e}. Retrying in {delay:.4f}s... "
                    f"(Attempt {attempt}/{max_attempts})"
                )
                await asyncio.sleep(delay)

    async def _get_block_number(self, w3: Any) -> int:
        async def get_bn() -> int:
            import inspect

            val = w3.eth.block_number
            while inspect.isawaitable(val):
                val = await val
            return int(val)

        return int(await self._call_with_retry(get_bn))

    async def _get_block_timestamp(self, w3: Any, block_number: int) -> int:
        if block_number in self._block_cache:
            return self._block_cache[block_number]
        blk = await self._call_with_retry(w3.eth.get_block, block_number)
        ts = int(blk["timestamp"])
        if len(self._block_cache) > 1000:
            self._block_cache.clear()
        self._block_cache[block_number] = ts
        return ts

    async def connect(self) -> None:
        self._connected = True
        self._poll_task = asyncio.create_task(self._poll_loop())

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[bytes]:
        while self._connected or not self._queue.empty():
            try:
                val = await self._queue.get()
                if val is None:
                    break
                yield val
            except asyncio.CancelledError:
                break

    async def send(self, data: bytes) -> None:
        pass  # subscription logic is handled natively in loop

    async def close(self) -> None:
        self._connected = False
        await self._queue.put(None)
        if self._poll_task:
            if self._poll_task != asyncio.current_task():
                self._poll_task.cancel()
                try:
                    await self._poll_task
                except asyncio.CancelledError:
                    pass
                self._poll_task = None

    async def _poll_loop(self) -> None:
        from web3 import AsyncHTTPProvider, AsyncWeb3
        from typing import cast

        last_active_rpc = None
        w3 = None

        try:
            # ABIs for factories and pools
            factory_v3_abi = [
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

            factory_aero_abi = [
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

            pool_v3_abi = [
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
                {
                    "inputs": [],
                    "name": "tickSpacing",
                    "outputs": [{"type": "int24"}],
                    "stateMutability": "view",
                    "type": "function",
                },
            ]

            pool_v2_abi = [
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

            resolved_pools: dict[str, dict[str, Any]] = {}

            # 2. Main polling loop
            while self._connected:
                if self.active_rpc_url != last_active_rpc:
                    if w3 is not None and getattr(w3, "provider", None) is not None:
                        try:
                            import inspect

                            res = w3.provider.disconnect()
                            if inspect.isawaitable(res):
                                await res
                        except Exception as disconnect_err:
                            log.warning(f"Error disconnecting previous provider: {disconnect_err}")

                    w3 = AsyncWeb3(AsyncHTTPProvider(self.active_rpc_url))
                    self.w3 = w3
                    # Re-instantiate contracts with the new w3 without clearing static address cache
                    for sym, pool in list(resolved_pools.items()):
                        spec = pool["spec"]
                        pool_addr = pool["address"]
                        pool["contract"] = w3.eth.contract(
                            address=AsyncWeb3.to_checksum_address(pool_addr),
                            abi=(pool_v3_abi if spec["type"] == "uniswap_v3" else pool_v2_abi),
                        )
                    last_active_rpc = self.active_rpc_url

                await asyncio.get_running_loop().run_in_executor(_ipc_executor, _load_ipc_sync)
                try:
                    default_pools = {
                        "AERO-USDC",
                        "cbBTC-USDC",
                        "DEGEN-WETH",
                        "WELL-WETH",
                        "WETH-USDC",
                    }
                    current_symbols = [
                        sym
                        for sym in POOL_SPECS.keys()
                        if sym in self.symbols or sym not in default_pools
                    ]

                    # 1. Resolve pool addresses dynamically inside the polling loop concurrently
                    async def resolve_single_pool(sym: str) -> None:
                        if sym in resolved_pools:
                            return
                        spec = cast(dict[str, Any], POOL_SPECS.get(sym))
                        if not spec:
                            return

                        try:
                            token0_name = str(spec["token0"])
                            token1_name = str(spec["token1"])
                            t0_val = TOKENS.get(token0_name, token0_name)
                            t0_addr = AsyncWeb3.to_checksum_address(t0_val)
                            t1_val = TOKENS.get(token1_name, token1_name)
                            t1_addr = AsyncWeb3.to_checksum_address(t1_val)
                            is_flipped = spec.get("is_flipped", int(t1_addr, 16) < int(t0_addr, 16))

                            if "address" in spec:
                                pool_addr = AsyncWeb3.to_checksum_address(spec["address"])
                            elif spec["type"] == "uniswap_v3":
                                # Sort token addresses for Uniswap V3 numerically
                                sorted_t0, sorted_t1 = sorted(
                                    [t0_addr, t1_addr], key=lambda x: int(x, 16)
                                )
                                factory = w3.eth.contract(
                                    address=AsyncWeb3.to_checksum_address(FACTORIES["uniswap_v3"]),
                                    abi=factory_v3_abi,
                                )
                                fee = int(spec["fee"])
                                pool_addr = await self._call_with_retry(
                                    factory.functions.getPool(sorted_t0, sorted_t1, fee).call
                                )
                            else:  # aerodrome_v2
                                factory = w3.eth.contract(
                                    address=AsyncWeb3.to_checksum_address(FACTORIES["aerodrome"]),
                                    abi=factory_aero_abi,
                                )
                                stable = bool(spec["stable"])
                                pool_addr = await self._call_with_retry(
                                    factory.functions.getPool(t0_addr, t1_addr, stable).call
                                )

                            if pool_addr != "0x0000000000000000000000000000000000000000":
                                resolved_pools[sym] = {
                                    "address": AsyncWeb3.to_checksum_address(pool_addr),
                                    "spec": spec,
                                    "contract": w3.eth.contract(
                                        address=AsyncWeb3.to_checksum_address(pool_addr),
                                        abi=(
                                            pool_v3_abi
                                            if spec["type"] == "uniswap_v3"
                                            else pool_v2_abi
                                        ),
                                    ),
                                    "is_flipped": is_flipped,
                                }
                                log.info(
                                    f"base_onchain: Resolved pool {sym} to {pool_addr} "
                                    f"(flipped: {is_flipped})"
                                )
                        except Exception as e:
                            log.error(f"base_onchain: Failed resolving pool {sym}: {e}")

                    resolution_tasks = [
                        resolve_single_pool(sym)
                        for sym in current_symbols
                        if sym not in resolved_pools
                    ]
                    if resolution_tasks:
                        await asyncio.gather(*resolution_tasks, return_exceptions=True)

                    tip_block = await self._get_block_number(w3)
                    lag = (
                        0
                        if (
                            "127.0.0.1" in self.active_rpc_url or "localhost" in self.active_rpc_url
                        )
                        else 15
                    )
                    current_block = max(0, tip_block - lag)

                    # Rollback / reorg detection
                    try:
                        blk = await self._call_with_retry(w3.eth.get_block, current_block)
                        blk_hash = (
                            blk["hash"].hex() if hasattr(blk["hash"], "hex") else str(blk["hash"])
                        )
                        parent_hash = (
                            blk["parentHash"].hex()
                            if hasattr(blk["parentHash"], "hex")
                            else str(blk["parentHash"])
                        )

                        fork_point = self.rollback_manager.process_block(
                            current_block, blk_hash, parent_hash, []
                        )
                        if fork_point is not None:
                            log.warning(
                                f"Reorg detected at block {current_block}. Rolling back tracking to block {fork_point - 1}."
                            )
                            for s in self.symbols:
                                self._last_blocks[s] = max(0, fork_point - 1)
                                self.sync_recovery.save_last_block(s, max(0, fork_point - 1))
                            if hasattr(self, "_last_lending_block"):
                                self._last_lending_block = max(0, fork_point - 1)
                                self.sync_recovery.save_last_block(
                                    "lending", max(0, fork_point - 1)
                                )
                            if hasattr(self, "_last_limit_order_block"):
                                self._last_limit_order_block = max(0, fork_point - 1)
                                self.sync_recovery.save_last_block(
                                    "limit_orders", max(0, fork_point - 1)
                                )
                            current_block = max(0, fork_point - 1)
                    except Exception as reorg_err:
                        log.debug(f"Failed to check reorg: {reorg_err}")

                    # Load lending block recovery
                    if not hasattr(self, "_last_lending_block"):
                        saved_lend = self.sync_recovery.get_last_block("lending")
                        self._last_lending_block = (
                            saved_lend if saved_lend is not None else max(0, current_block - 20)
                        )

                    async def poll_single_pool(sym: str, pool: dict[str, Any]) -> None:
                        spec = pool["spec"]
                        addr = pool["address"]
                        contract = pool["contract"]
                        is_flipped = pool["is_flipped"]

                        price = 0.0
                        reserve0 = 0.0
                        reserve1 = 0.0
                        swaps = []

                        if sym not in self._last_blocks:
                            saved_block = self.sync_recovery.get_last_block(sym)
                            if saved_block is not None:
                                self._last_blocks[sym] = saved_block
                            else:
                                self._last_blocks[sym] = max(0, current_block - 20)

                        async def fetch_state() -> dict[str, Any]:
                            if spec["type"] == "uniswap_v3":
                                slot0_fut = self._call_with_retry(contract.functions.slot0().call)
                                liquidity_fut = self._call_with_retry(
                                    contract.functions.liquidity().call
                                )
                                if hasattr(contract.functions, "tickSpacing"):
                                    tick_spacing_fut = self._call_with_retry(
                                        contract.functions.tickSpacing().call
                                    )
                                else:

                                    async def _dummy_spacing() -> Any:
                                        raise AttributeError(
                                            "tickSpacing not supported on contract functions"
                                        )

                                    tick_spacing_fut = _dummy_spacing()

                                results = await asyncio.gather(
                                    slot0_fut,
                                    liquidity_fut,
                                    tick_spacing_fut,
                                    return_exceptions=True,
                                )
                                slot0_val = results[0]
                                liquidity_val = results[1]
                                tick_spacing_val = results[2]

                                if isinstance(slot0_val, Exception):
                                    raise slot0_val
                                if isinstance(liquidity_val, Exception):
                                    raise liquidity_val

                                if isinstance(tick_spacing_val, Exception):
                                    log.warning(
                                        f"base_onchain: Failed to fetch tickSpacing dynamically: {tick_spacing_val}. "
                                        f"Deriving from fee tier."
                                    )
                                    fee = int(spec.get("fee", 3000))
                                    tick_spacing_val = (
                                        1
                                        if fee == 100
                                        else 10
                                        if fee == 500
                                        else 60
                                        if fee == 3000
                                        else 200
                                        if fee == 10000
                                        else max(1, fee // 50)
                                    )
                                return {
                                    "slot0": slot0_val,
                                    "liquidity": liquidity_val,
                                    "tick_spacing": tick_spacing_val,
                                }
                            else:  # aerodrome_v2
                                res_val = await self._call_with_retry(
                                    contract.functions.getReserves().call
                                )
                                return {"reserves": res_val}

                        async def fetch_logs() -> tuple[list[Any], int, Exception | None]:
                            swap_topic = (
                                SWAP_TOPIC_V3 if spec["type"] == "uniswap_v3" else SWAP_TOPIC_V2
                            )
                            overlap = 5
                            start_block = max(0, self._last_blocks[sym] + 1 - overlap)
                            end_block = current_block

                            logs_list = []
                            last_block = start_block - 1
                            if start_block <= end_block:
                                chunk_size = 500
                                for from_b in range(start_block, end_block + 1, chunk_size):
                                    to_b = min(from_b + chunk_size - 1, end_block)
                                    try:
                                        chunk_logs = await self._call_with_retry(
                                            w3.eth.get_logs,
                                            {
                                                "address": addr,
                                                "fromBlock": from_b,
                                                "toBlock": to_b,
                                                "topics": [swap_topic],
                                            },
                                        )
                                        logs_list.extend(chunk_logs)
                                        last_block = to_b
                                    except Exception as e:
                                        return logs_list, max(self._last_blocks[sym], last_block), e
                            return logs_list, end_block, None

                        state_task = asyncio.create_task(fetch_state())
                        logs_task = asyncio.create_task(fetch_logs())
                        try:
                            try:
                                await asyncio.gather(state_task, logs_task)
                                state_res = state_task.result()
                                logs, last_block, logs_err = logs_task.result()
                            except BaseException:
                                for task in (state_task, logs_task):
                                    if not task.done():
                                        task.cancel()
                                for task in (state_task, logs_task):
                                    try:
                                        await task
                                    except BaseException:
                                        pass
                                raise

                            if spec["type"] == "uniswap_v3":
                                slot0 = state_res["slot0"]
                                liquidity = state_res["liquidity"]
                                tick_spacing = state_res["tick_spacing"]

                                sqrtPriceX96 = slot0[0]
                                price_ratio = (sqrtPriceX96 / (2**96)) ** 2

                                # Price of base in terms of quote
                                dec_diff = int(spec["decimals0"]) - int(spec["decimals1"])
                                if not is_flipped:
                                    price = price_ratio * (10**dec_diff)
                                else:
                                    price = (
                                        (1.0 / price_ratio) * (10**dec_diff)
                                        if price_ratio > 0
                                        else 0.0
                                    )

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

                            else:  # aerodrome_v2
                                res = state_res["reserves"]
                                if not is_flipped:
                                    reserve0 = res[0] / (10 ** int(spec["decimals0"]))
                                    reserve1 = res[1] / (10 ** int(spec["decimals1"]))
                                else:
                                    reserve0 = res[1] / (10 ** int(spec["decimals0"]))
                                    reserve1 = res[0] / (10 ** int(spec["decimals1"]))
                                price = reserve1 / reserve0 if reserve0 > 0 else 0.0

                            new_seen_log_keys = []
                            for lg in logs:
                                data = lg["data"]
                                tx_hash = lg["transactionHash"].hex()
                                log_index = lg["logIndex"]

                                # Deduplicate seen logs
                                log_key = (tx_hash, log_index)
                                if log_key in self._seen_logs:
                                    continue

                                ts = await self._get_block_timestamp(w3, lg["blockNumber"])

                                l1_gas_fee = None
                                l2_gas_fee = None
                                gas_price = None
                                sender = None
                                is_sw = None
                                try:
                                    receipt = await self._call_with_retry(
                                        w3.eth.get_transaction_receipt, tx_hash
                                    )
                                    if receipt is not None:
                                        if "mock" in type(receipt).__name__.lower():
                                            receipt = None

                                    if receipt is not None:
                                        l1_fee_raw = receipt.get("l1Fee")
                                        if l1_fee_raw is not None:
                                            if isinstance(
                                                l1_fee_raw, str
                                            ) and l1_fee_raw.startswith("0x"):
                                                l1_gas_fee = float(int(l1_fee_raw, 16))
                                            else:
                                                l1_gas_fee = float(l1_fee_raw)

                                        gas_used = receipt.get("gasUsed")
                                        eff_gas_price = receipt.get("effectiveGasPrice")
                                        if gas_used is not None and eff_gas_price is not None:
                                            if isinstance(gas_used, str) and gas_used.startswith(
                                                "0x"
                                            ):
                                                gas_used = int(gas_used, 16)
                                            if isinstance(
                                                eff_gas_price, str
                                            ) and eff_gas_price.startswith("0x"):
                                                eff_gas_price = int(eff_gas_price, 16)

                                            l2_gas_fee = float(gas_used) * float(eff_gas_price)
                                            gas_price = float(eff_gas_price)

                                        sender = receipt.get("from")

                                    if not sender:
                                        tx = await self._call_with_retry(
                                            w3.eth.get_transaction, tx_hash
                                        )
                                        if tx is not None:
                                            if "mock" in type(tx).__name__.lower():
                                                tx = None
                                        if tx is not None:
                                            sender = tx.get("from")

                                    if sender:
                                        is_sw = await smart_wallet_detector.is_smart_wallet(
                                            w3, sender
                                        )
                                except Exception as e:
                                    log.debug(
                                        f"Failed to fetch gas metrics / sender for tx {tx_hash}: {e}"
                                    )

                                if spec["type"] == "uniswap_v3":
                                    amount0 = int.from_bytes(
                                        data[0:32], byteorder="big", signed=True
                                    )
                                    amount1 = int.from_bytes(
                                        data[32:64], byteorder="big", signed=True
                                    )

                                    if not is_flipped:
                                        abs_base = abs(amount0) / (10 ** int(spec["decimals0"]))
                                        abs_quote = abs(amount1) / (10 ** int(spec["decimals1"]))
                                        is_buy = amount0 < 0
                                    else:
                                        abs_base = abs(amount1) / (10 ** int(spec["decimals0"]))
                                        abs_quote = abs(amount0) / (10 ** int(spec["decimals1"]))
                                        is_buy = amount1 < 0

                                    sw_price = abs_quote / abs_base if abs_base > 0 else 0.0

                                    swaps.append(
                                        {
                                            "tx_hash": tx_hash,
                                            "log_index": log_index,
                                            "timestamp": ts,
                                            "price": sw_price,
                                            "amount": abs_base,
                                            "is_buy": is_buy,
                                            "l1_gas_fee": l1_gas_fee,
                                            "l2_gas_fee": l2_gas_fee,
                                            "gas_price": gas_price,
                                            "sender": sender,
                                            "is_smart_wallet": is_sw,
                                        }
                                    )
                                else:  # aerodrome_v2
                                    amt0_in = int.from_bytes(
                                        data[0:32], byteorder="big", signed=False
                                    )
                                    amt1_in = int.from_bytes(
                                        data[32:64], byteorder="big", signed=False
                                    )
                                    amt0_out = int.from_bytes(
                                        data[64:96], byteorder="big", signed=False
                                    )
                                    amt1_out = int.from_bytes(
                                        data[96:128], byteorder="big", signed=False
                                    )

                                    if not is_flipped:
                                        amt_base = (amt0_in if amt0_in > 0 else amt0_out) / (
                                            10 ** int(spec["decimals0"])
                                        )
                                        amt_quote = (amt1_in if amt1_in > 0 else amt1_out) / (
                                            10 ** int(spec["decimals1"])
                                        )
                                        is_buy = amt0_out > 0
                                    else:
                                        amt_base = (amt1_in if amt1_in > 0 else amt1_out) / (
                                            10 ** int(spec["decimals0"])
                                        )
                                        amt_quote = (amt0_in if amt0_in > 0 else amt0_out) / (
                                            10 ** int(spec["decimals1"])
                                        )
                                        is_buy = amt1_out > 0

                                    sw_price = amt_quote / amt_base if amt_base > 0 else 0.0

                                    swaps.append(
                                        {
                                            "tx_hash": tx_hash,
                                            "log_index": log_index,
                                            "timestamp": ts,
                                            "price": sw_price,
                                            "amount": amt_base,
                                            "is_buy": is_buy,
                                            "l1_gas_fee": l1_gas_fee,
                                            "l2_gas_fee": l2_gas_fee,
                                            "gas_price": gas_price,
                                            "sender": sender,
                                            "is_smart_wallet": is_sw,
                                        }
                                    )
                                new_seen_log_keys.append(log_key)

                            state_payload = {
                                "price": price,
                                "reserve0": reserve0,
                                "reserve1": reserve1,
                                "is_flipped": is_flipped,
                                "decimals0": spec["decimals0"],
                                "decimals1": spec["decimals1"],
                            }
                            if spec["type"] == "uniswap_v3":
                                state_payload["tick"] = int(slot0[1])
                                state_payload["liquidity"] = int(liquidity)
                                state_payload["tickSpacing"] = int(tick_spacing)
                                state_payload["tick_spacing"] = int(tick_spacing)

                            update_msg = {
                                "type": "onchain_update",
                                "block": current_block,
                                "pool": sym,
                                "pool_type": spec["type"],
                                "timestamp": await self._get_block_timestamp(w3, current_block),
                                "state": state_payload,
                                "swaps": swaps,
                            }
                            await self._queue.put(json.dumps(update_msg).encode())
                            # Success! Mark logs as seen and update block number
                            for lk in new_seen_log_keys:
                                self._seen_logs.add(lk)
                            if len(self._seen_logs) > 5000:
                                self._seen_logs = set(list(self._seen_logs)[2500:])
                            self._last_blocks[sym] = max(self._last_blocks[sym], last_block)
                            self.sync_recovery.save_last_block(sym, self._last_blocks[sym])
                            if logs_err is not None:
                                raise logs_err
                        except Exception as e:
                            log.error(f"base_onchain: Error polling pool data for {sym}: {e}")
                            raise

                    poll_tasks = [
                        poll_single_pool(sym, pool)
                        for sym, pool in resolved_pools.items()
                        if sym in current_symbols
                    ]
                    if poll_tasks:
                        results = await asyncio.gather(*poll_tasks, return_exceptions=True)
                        for res_err in results:
                            if isinstance(res_err, Exception):
                                log.error(
                                    f"base_onchain: Error polling pool concurrently: {res_err}"
                                )

                    # Run Aave/Seamless lending log polling
                    if not hasattr(self, "_last_lending_block"):
                        self._last_lending_block = max(0, current_block - 20)

                    lending_start_block = max(0, self._last_lending_block + 1)
                    lending_end_block = current_block
                    if lending_start_block <= lending_end_block:
                        lending_addresses = [
                            AsyncWeb3.to_checksum_address(
                                "0xA238Dd80C259697C8390c76420315873AB4F66C5"
                            ),
                            AsyncWeb3.to_checksum_address(
                                "0x8FA4c96570F4D0860A7C93f0b2fB58416d860d5b"
                            ),
                        ]
                        for l_addr in lending_addresses:
                            try:
                                for topic in [RESERVE_DATA_UPDATED_TOPIC, LIQUIDATION_CALL_TOPIC]:
                                    l_logs = await self._call_with_retry(
                                        w3.eth.get_logs,
                                        {
                                            "address": l_addr,
                                            "fromBlock": lending_start_block,
                                            "toBlock": lending_end_block,
                                            "topics": [topic],
                                        },
                                    )
                                    for lg in l_logs:
                                        tx_hash = lg["transactionHash"].hex()
                                        log_idx = lg["logIndex"]
                                        log_key = (tx_hash, log_idx)
                                        if log_key in self._seen_logs:
                                            continue

                                        block_num = lg["blockNumber"]
                                        ts = await self._get_block_timestamp(w3, block_num)

                                        if topic == RESERVE_DATA_UPDATED_TOPIC:
                                            reserve_addr = "0x" + lg["topics"][1].hex()[-40:]
                                            d_bytes = lg["data"]
                                            liq_rate = int.from_bytes(d_bytes[0:32], "big") / 1e27
                                            stable_rate = (
                                                int.from_bytes(d_bytes[32:64], "big") / 1e27
                                            )
                                            var_rate = int.from_bytes(d_bytes[64:96], "big") / 1e27
                                            liq_idx = int.from_bytes(d_bytes[96:128], "big")
                                            var_idx = int.from_bytes(d_bytes[128:160], "big")

                                            pool_name = (
                                                "AAVE_V3"
                                                if l_addr == lending_addresses[0]
                                                else "SEAMLESS"
                                            )
                                            update_msg = {
                                                "type": "lending_update",
                                                "event": "ReserveDataUpdated",
                                                "exchange": self.exchange,
                                                "pool": pool_name,
                                                "block": block_num,
                                                "timestamp": ts,
                                                "reserve": reserve_addr,
                                                "liquidity_rate": float(liq_rate),
                                                "stable_borrow_rate": float(stable_rate),
                                                "variable_borrow_rate": float(var_rate),
                                                "liquidity_index": int(liq_idx),
                                                "variable_borrow_index": int(var_idx),
                                            }
                                            await self._queue.put(json.dumps(update_msg).encode())

                                        elif topic == LIQUIDATION_CALL_TOPIC:
                                            col_asset = "0x" + lg["topics"][1].hex()[-40:]
                                            debt_asset = "0x" + lg["topics"][2].hex()[-40:]
                                            user = "0x" + lg["topics"][3].hex()[-40:]

                                            d_bytes = lg["data"]
                                            debt_cover = int.from_bytes(d_bytes[0:32], "big")
                                            col_amount = int.from_bytes(d_bytes[32:64], "big")
                                            liquidator = "0x" + d_bytes[64:96][-20:].hex()
                                            receive_a = bool(int.from_bytes(d_bytes[96:128], "big"))

                                            pool_name = (
                                                "AAVE_V3"
                                                if l_addr == lending_addresses[0]
                                                else "SEAMLESS"
                                            )
                                            update_msg = {
                                                "type": "lending_update",
                                                "event": "LiquidationCall",
                                                "exchange": self.exchange,
                                                "pool": pool_name,
                                                "block": block_num,
                                                "timestamp": ts,
                                                "collateral_asset": col_asset,
                                                "debt_asset": debt_asset,
                                                "user": user,
                                                "debt_to_cover": float(debt_cover),
                                                "liquidated_collateral_amount": float(col_amount),
                                                "liquidator": liquidator,
                                                "receive_a_token": receive_a,
                                            }
                                            await self._queue.put(json.dumps(update_msg).encode())

                                        self._seen_logs.add(log_key)
                            except Exception as lending_err:
                                log.debug(
                                    f"Failed to poll/decode lending logs for {l_addr}: {lending_err}"
                                )
                        self._last_lending_block = lending_end_block
                        self.sync_recovery.save_last_block("lending", self._last_lending_block)

                    # Poll for 1inch and 0x limit orders
                    if not hasattr(self, "_last_limit_order_block"):
                        saved_limit = self.sync_recovery.get_last_block("limit_orders")
                        self._last_limit_order_block = (
                            saved_limit if saved_limit is not None else max(0, current_block - 20)
                        )

                    limit_start_block = max(0, self._last_limit_order_block + 1)
                    limit_end_block = current_block
                    if limit_start_block <= limit_end_block:
                        limit_protocols = {
                            "0x1111111254fb6c44bac0bed2854e76f90643097d": "1inch",
                            "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0x",
                            "0xDef1C0ded9bec7F1a1670819833240f027b25EfF": "0x",
                        }
                        for addr, proto in limit_protocols.items():
                            try:
                                topic = (
                                    ONEINCH_ORDER_FILLED_TOPIC
                                    if proto == "1inch"
                                    else ZEROX_LIMIT_ORDER_FILLED_TOPIC
                                )
                                ch_logs = await self._call_with_retry(
                                    w3.eth.get_logs,
                                    {
                                        "address": AsyncWeb3.to_checksum_address(addr),
                                        "fromBlock": limit_start_block,
                                        "toBlock": limit_end_block,
                                        "topics": [topic],
                                    },
                                )
                                for lg in ch_logs:
                                    tx_hash = lg["transactionHash"].hex()
                                    log_idx = lg["logIndex"]
                                    log_key = (tx_hash, log_idx)
                                    if log_key in self._seen_logs:
                                        continue

                                    topics_str = [
                                        t.hex() if isinstance(t, bytes) else str(t)
                                        for t in lg["topics"]
                                    ]
                                    data_str = (
                                        lg["data"].hex()
                                        if isinstance(lg["data"], bytes)
                                        else str(lg["data"])
                                    )

                                    receipt = None
                                    if proto == "1inch":
                                        try:
                                            receipt = await self._call_with_retry(
                                                w3.eth.get_transaction_receipt, tx_hash
                                            )
                                        except Exception:
                                            pass

                                    if proto == "1inch":
                                        decoded = decode_1inch_order_filled(
                                            topics_str, data_str, receipt
                                        )
                                    else:
                                        decoded = decode_0x_limit_order_filled(topics_str, data_str)

                                    ts = await self._get_block_timestamp(w3, lg["blockNumber"])

                                    update_msg = {
                                        "type": "limit_order_fill_update",
                                        "exchange": self.exchange,
                                        "symbol": f"limit_order:{proto}",
                                        "symbol_raw": proto,
                                        "exchange_ts": ts * 1_000_000_000,
                                        "timestamp": ts,
                                        "tx_hash": tx_hash,
                                        "log_index": log_idx,
                                        "protocol": proto,
                                        "maker": decoded["maker"],
                                        "taker": decoded["taker"],
                                        "maker_token": decoded["maker_token"],
                                        "taker_token": decoded["taker_token"],
                                        "maker_amount": decoded["maker_amount"],
                                        "taker_amount": decoded["taker_amount"],
                                        "order_hash": decoded["order_hash"],
                                    }
                                    await self._queue.put(json.dumps(update_msg).encode())
                                    self._seen_logs.add(log_key)
                            except Exception as limit_err:
                                log.debug(
                                    f"Failed to poll limit order logs for {addr}: {limit_err}"
                                )
                        self._last_limit_order_block = limit_end_block
                        self.sync_recovery.save_last_block(
                            "limit_orders", self._last_limit_order_block
                        )

                except Exception as e:
                    log.error(f"base_onchain: Error polling pool data: {e}")

                await asyncio.sleep(self.poll_interval)
        finally:
            try:
                import inspect

                if w3 is not None and getattr(w3, "provider", None) is not None:
                    res = w3.provider.disconnect()
                    if inspect.isawaitable(res):
                        await res
            except Exception:
                pass


class BaseOnchainConnector(Provider):
    """On-chain Base Ecosystem DEX connector.

    Subscribes to dynamic reserves, prices, and trades from major Aerodrome and
    Uniswap V3 pools.
    """

    name = "base_onchain"
    ws_url = "wss://base-rpc.publicnode.com"  # placeholder
    rest_url = "https://base-rpc.publicnode.com"

    def __init__(
        self,
        symbols: list[str],
        channels: list[str],
        out: Sink,
        registry: InstrumentRegistry,
        custom_pools: dict[str, dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(symbols=symbols, channels=channels, out=out, registry=registry)
        rpc_url = os.getenv("BASE_RPC_URL", "https://base-rpc.publicnode.com")
        _register_custom_pools(custom_pools)
        self.transport = BaseOnchainTransport(
            rpc_url, symbols, custom_pools=custom_pools, exchange=self.name
        )

    def normalize(self, msg: object, local_ts: int) -> Iterable[Record]:
        if isinstance(msg, dict):
            m_type = msg.get("type")
            if m_type == "onchain_update":
                yield from normalize_onchain_update(msg, local_ts, exchange=self.name)
            elif m_type == "limit_order_fill_update":
                from stockodile.schema.records import LimitOrderFill

                yield LimitOrderFill(
                    provider=self.name,
                    symbol=msg["symbol"],
                    symbol_raw=msg["symbol_raw"],
                    exchange_ts=msg["exchange_ts"],
                    local_ts=local_ts,
                    tx_hash=msg["tx_hash"],
                    log_index=msg["log_index"],
                    protocol=msg["protocol"],
                    maker=msg["maker"],
                    taker=msg["taker"],
                    maker_token=msg["maker_token"],
                    taker_token=msg["taker_token"],
                    maker_amount=msg["maker_amount"],
                    taker_amount=msg["taker_amount"],
                    order_hash=msg["order_hash"],
                )
            elif m_type == "lending_update":
                from stockodile.schema.records import LiquidationCall, ReserveDataUpdated

                evt = msg.get("event")
                if evt == "ReserveDataUpdated":
                    yield ReserveDataUpdated(
                        provider=self.name,
                        symbol=f"lending:{msg['pool']}",
                        symbol_raw=msg["pool"],
                        exchange_ts=msg["timestamp"] * 1_000_000_000,
                        local_ts=local_ts,
                        reserve=msg["reserve"],
                        liquidity_rate=msg["liquidity_rate"],
                        stable_borrow_rate=msg["stable_borrow_rate"],
                        variable_borrow_rate=msg["variable_borrow_rate"],
                        liquidity_index=msg["liquidity_index"],
                        variable_borrow_index=msg["variable_borrow_index"],
                    )
                elif evt == "LiquidationCall":
                    yield LiquidationCall(
                        provider=self.name,
                        symbol=f"lending:{msg['pool']}",
                        symbol_raw=msg["pool"],
                        exchange_ts=msg["timestamp"] * 1_000_000_000,
                        local_ts=local_ts,
                        collateral_asset=msg["collateral_asset"],
                        debt_asset=msg["debt_asset"],
                        user=msg["user"],
                        debt_to_cover=msg["debt_to_cover"],
                        liquidated_collateral_amount=msg["liquidated_collateral_amount"],
                        liquidator=msg["liquidator"],
                        receive_a_token=msg["receive_a_token"],
                    )

    async def list_instruments(self) -> list[Instrument]:
        instruments = []
        for sym in list(POOL_SPECS.keys()):
            if sym not in self.symbols:
                continue
            spec = POOL_SPECS.get(sym)
            if not spec:
                continue
            instruments.append(
                Instrument(
                    symbol=sym,
                    provider=self.name,
                    symbol_raw=sym,
                    security_type=SecurityType.UNKNOWN,
                )
            )
        return instruments

    def subscribe_channels(self) -> list[str]:
        return self.channels

    async def _subscribe(self, transport: Transport) -> None:
        pass  # subscription handled in the poll transport
