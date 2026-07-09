import logging

from typing import Any
from web3 import AsyncWeb3

from stockodile.schema.records import PoRUpdate

log = logging.getLogger(__name__)

class ProofOfReserveSyncer:
    def __init__(self, w3: AsyncWeb3[Any], feed_address: str, token_address: str) -> None:
        self.w3 = w3
        self.feed_address = AsyncWeb3.to_checksum_address(feed_address)
        self.token_address = AsyncWeb3.to_checksum_address(token_address)
        self._feed_abi = [
            {
                "inputs": [],
                "name": "latestRoundData",
                "outputs": [
                    {"name": "roundId", "type": "uint80"},
                    {"name": "answer", "type": "int256"},
                    {"name": "startedAt", "type": "uint256"},
                    {"name": "updatedAt", "type": "uint256"},
                    {"name": "answeredInRound", "type": "uint80"}
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "stateMutability": "view",
                "type": "function"
            }
        ]
        self._token_abi = [
            {
                "inputs": [],
                "name": "totalSupply",
                "outputs": [{"name": "", "type": "uint256"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "stateMutability": "view",
                "type": "function"
            }
        ]
        self.feed_contract = self.w3.eth.contract(address=self.feed_address, abi=self._feed_abi)
        self.token_contract = self.w3.eth.contract(address=self.token_address, abi=self._token_abi)
        self._feed_decimals: int | None = None
        self._token_decimals: int | None = None

    async def get_feed_decimals(self) -> int:
        if self._feed_decimals is not None:
            return self._feed_decimals
        try:
            feed_decimals = await self.feed_contract.functions.decimals().call()
            self._feed_decimals = int(feed_decimals)
        except Exception:
            self._feed_decimals = 8
        return self._feed_decimals

    async def get_token_decimals(self) -> int:
        if self._token_decimals is not None:
            return self._token_decimals
        try:
            token_decimals = await self.token_contract.functions.decimals().call()
            self._token_decimals = int(token_decimals)
        except Exception:
            self._token_decimals = 18
        return self._token_decimals

    async def sync_por(
        self, block_number: int, local_ts: int, provider: str = "base_onchain"
    ) -> PoRUpdate:
        feed_dec = await self.get_feed_decimals()
        token_dec = await self.get_token_decimals()
        
        round_data = await self.feed_contract.functions.latestRoundData().call(
            block_identifier=block_number
        )
        raw_reserves = round_data[1]
        reserves = float(raw_reserves) / (10 ** feed_dec)
        
        raw_supply = await self.token_contract.functions.totalSupply().call(
            block_identifier=block_number
        )
        total_supply = float(raw_supply) / (10 ** token_dec)
        
        if total_supply > 0:
            backing_ratio = reserves / total_supply
        else:
            backing_ratio = 1.0 if reserves >= 0 else 0.0
            
        is_backed = backing_ratio >= 1.0
        
        return PoRUpdate(
            provider=provider,
            symbol=f"por:{self.token_address}",
            symbol_raw=self.token_address,
            exchange_ts=int(round_data[3] * 1_000_000_000),
            local_ts=local_ts,
            feed_address=self.feed_address,
            token_address=self.token_address,
            reserves=reserves,
            total_supply=total_supply,
            backing_ratio=backing_ratio,
            is_backed=is_backed
        )
