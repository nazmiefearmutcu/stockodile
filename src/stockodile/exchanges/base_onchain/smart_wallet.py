import json
import logging
import os
from typing import Any

from web3 import Web3

log = logging.getLogger(__name__)

# Coinbase Smart Wallet Factory Address
FACTORY_ADDRESS = "0x00000000003b26925905180037a35368a55e206b"
# AccountCreated(address indexed account, address indexed owner)
# Placeholder or actual signature
ACCOUNT_CREATED_TOPIC = (
    "0xe9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9"
)

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
                # Let's search logs from the factory to see if this account was created
                topic_addr = "0x" + checksum_address[2:].lower().zfill(64)
                logs = await w3.eth.get_logs({
                    "address": Web3.to_checksum_address(FACTORY_ADDRESS),
                    "topics": [None, topic_addr], # account is the first indexed param
                    "fromBlock": 0,
                    "toBlock": "latest"
                })
                if logs:
                    is_wallet = True
                else:
                    # Fallback check: if it has bytecode and is not a known exchange contract
                    # we can analyze proxy pattern or just classify it based on bytecode signature.
                    # Coinbase Smart Wallet is typically a proxy pointing to implementation.
                    # If bytecode contains specific proxy pattern, tag it.
                    bytecode_hex = bytecode.hex()
                    # Standard ERC-1967 proxy or custom Coinbase Smart Wallet
                    # bytecode signature elements
                    if "363d3d37" in bytecode_hex or "5f5f365f" in bytecode_hex:
                        is_wallet = True
        except Exception as e:
            log.debug(f"Error checking bytecode / logs for {address}: {e}")
            # If offline / RPC error, keep cache unchanged or fallback to False
            return False

        # Cache the result
        self.cache[checksum_address] = is_wallet
        self._save_cache()
        return is_wallet
