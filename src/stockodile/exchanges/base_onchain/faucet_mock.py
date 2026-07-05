import asyncio
import logging
import random
import time

from web3 import Web3

log = logging.getLogger(__name__)

class BaseSepoliaFaucetMockStream:
    """Simulates Sepolia testnet faucet transfer transactions to feed local ingestion streams."""

    def __init__(
        self,
        queue: asyncio.Queue[bytes],
        interval_min: float = 1.0,
        interval_max: float = 5.0
    ) -> None:
        self.queue = queue
        self.interval_min = interval_min
        self.interval_max = interval_max
        self._task: asyncio.Task[None] | None = None
        self._running = False

    def start(self) -> None:
        """Start the mock faucet stream background task."""
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run_loop())
            log.info("BaseSepoliaFaucetMockStream started.")

    async def stop(self) -> None:
        """Stop the background stream task."""
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            log.info("BaseSepoliaFaucetMockStream stopped.")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                # Sleep for random interval
                sleep_time = random.uniform(self.interval_min, self.interval_max)
                await asyncio.sleep(sleep_time)

                # Generate mock transaction details
                user_addr_int = random.randint(1, 2**160 - 1)
                user_addr = Web3.to_checksum_address(f"0x{user_addr_int:040x}")
                
                tx_hash_int = random.randint(1, 2**256 - 1)
                tx_hash = f"0x{tx_hash_int:064x}"
                
                amount = round(random.uniform(0.01, 0.5), 4)

                # Format as onchain update structure or serialize directly
                # To match connector queues, let's serialize the trade or put it as JSON
                # Since ingest/normalize parses connector updates, we can serialize the Trade
                # directly or format as json dict.
                # Let's put a normalized onchain update msg:
                update_msg = {
                    "type": "onchain_update",
                    "block": random.randint(100000, 200000),
                    "pool": "ETH-FAUCET",
                    "pool_type": "faucet",
                    "timestamp": int(time.time()),
                    "swaps": [{
                        "tx_hash": tx_hash,
                        "log_index": 0,
                        "timestamp": int(time.time()),
                        "price": 0.0,
                        "amount": amount,
                        "is_buy": True,
                        "sender": user_addr,
                        "is_smart_wallet": False,
                    }],
                    "state": {
                        "price": 0.0,
                        "reserve0": 10000.0,
                        "reserve1": 10000.0,
                        "is_flipped": False,
                        "decimals0": 18,
                        "decimals1": 18,
                    }
                }
                import json
                await self.queue.put(json.dumps(update_msg).encode())
                log.debug(f"FaucetMockStream yielded drip of {amount} ETH to {user_addr}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Error in FaucetMockStream loop: {e}")
                await asyncio.sleep(1.0)
