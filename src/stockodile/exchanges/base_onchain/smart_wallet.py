import json
import logging
import os
from typing import Any

from web3 import Web3

log = logging.getLogger(__name__)

# Coinbase Smart Wallet factory deployments (CREATE2 — same address across chains)
# https://github.com/coinbase/smart-wallet#deployments
FACTORY_ADDRESSES = (
    "0x0BA5ED0c6AA8c49038F819E587E2633c4A9F428a",  # v1
    "0xBA5ED110eFDBa3D005bfC882d75358ACBbB85842",  # v1.1
)
# Back-compat single constant (v1)
FACTORY_ADDRESS = FACTORY_ADDRESSES[0]
# AccountCreated(address indexed account, ...) — used when available; may be None
ACCOUNT_CREATED_TOPIC: str | None = None

class CoinbaseSmartWalletDetector:
    """Detects if an address is a Coinbase Smart Wallet with disk caching."""

    def __init__(self, cache_path: str = ".smart_wallet_cache.json") -> None:
        self.cache_path = cache_path
        self.cache: dict[str, bool] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path) as f:
                    self.cache = json.load(f)
            except Exception as e:
                log.warning(f"Failed to load smart wallet cache: {e}")

    def _save_cache(self) -> None:
        try:
            with open(self.cache_path, "w") as f:
                json.dump(self.cache, f)
        except Exception as e:
            log.warning(f"Failed to save smart wallet cache: {e}")

    async def is_smart_wallet(self, w3: Any, address: str) -> bool:
        """Asynchronously checks if the address is a Coinbase Smart Wallet."""
        if not address:
            return False
        
        checksum_address = Web3.to_checksum_address(address)
        if checksum_address in self.cache:
            return self.cache[checksum_address]

        # Standard check: must be a contract (bytecode must be non-empty)
        is_wallet = False
        try:
            bytecode = await w3.eth.get_code(checksum_address)
            if bytecode and len(bytecode) > 2: # non-empty bytecode
                # Perform proxy bytecode checks or check if factory created it
                # For Coinbase Smart Wallet, typical proxy contract bytecode starts
                # with ERC-1967/minimal proxy patterns
                # or we can query logs of the factory.
                # Prefer bytecode heuristics first (cheap); factory logs are
                # best-effort with a recent block window only.
                bytecode_hex = bytecode.hex()
                if "363d3d37" in bytecode_hex or "5f5f365f" in bytecode_hex:
                    is_wallet = True
                else:
                    topic_addr = "0x" + checksum_address[2:].lower().zfill(64)
                    try:
                        latest = int(await w3.eth.block_number)
                        # Avoid full-chain eth_getLogs (rejected by most RPCs)
                        from_block = max(0, latest - 200_000)
                        for factory in FACTORY_ADDRESSES:
                            # Indexed account topic is typically topics[1]
                            topics: list[Any]
                            if ACCOUNT_CREATED_TOPIC:
                                topics = [ACCOUNT_CREATED_TOPIC, topic_addr]
                            else:
                                topics = [None, topic_addr]
                            logs = await w3.eth.get_logs(
                                {
                                    "address": Web3.to_checksum_address(factory),
                                    "topics": topics,
                                    "fromBlock": from_block,
                                    "toBlock": "latest",
                                }
                            )
                            if logs:
                                is_wallet = True
                                break
                    except Exception as log_err:
                        log.debug(
                            "Factory log lookup failed for %s: %s",
                            address,
                            log_err,
                        )
        except Exception as e:
            log.debug(f"Error checking bytecode / logs for {address}: {e}")
            # If offline / RPC error, do not poison permanent negative cache
            return False

        # Only permanently cache positive results; negatives may be RPC limits
        if is_wallet:
            self.cache[checksum_address] = True
            self._save_cache()
        return is_wallet
