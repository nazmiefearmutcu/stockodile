import logging
from typing import Any

from web3 import AsyncWeb3

from stockodile.schema.records import BalanceCorrection

log = logging.getLogger(__name__)

class RebaseValidator:
    def __init__(self, w3: AsyncWeb3[Any], token_address: str, holders: list[str]) -> None:
        self.w3 = w3
        self.token_address = AsyncWeb3.to_checksum_address(token_address)
        self.holders = [AsyncWeb3.to_checksum_address(h) for h in holders]
        self._decimals: int | None = None
        self._abi = [
            {
                "inputs": [{"name": "account", "type": "address"}],
                "name": "balanceOf",
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
        self.contract = self.w3.eth.contract(address=self.token_address, abi=self._abi)

    async def get_decimals(self) -> int:
        if self._decimals is not None:
            return self._decimals
        try:
            decimals = await self.contract.functions.decimals().call()
            self._decimals = int(decimals)
        except Exception:
            self._decimals = 18
        return self._decimals

    async def validate_balances(
        self,
        local_balances: dict[str, float],
        block_number: int,
        local_ts: int,
        provider: str = "base_onchain"
    ) -> list[BalanceCorrection]:
        decimals = await self.get_decimals()
        factor = 10 ** decimals
        corrections = []
        
        for holder in self.holders:
            holder_key = holder.lower()
            local_bal = local_balances.get(holder_key, 0.0)
            
            try:
                raw_bal = await self.contract.functions.balanceOf(holder).call(
                    block_identifier=block_number
                )
                onchain_bal = float(raw_bal) / factor
                
                diff = onchain_bal - local_bal
                threshold = 1.0 / factor
                
                if abs(diff) > threshold:
                    corrections.append(
                        BalanceCorrection(
                            provider=provider,
                            symbol=f"correction:{self.token_address}",
                            symbol_raw=self.token_address,
                            exchange_ts=None,
                            local_ts=local_ts,
                            holder_address=holder,
                            token_address=self.token_address,
                            local_balance=local_bal,
                            onchain_balance=onchain_bal,
                            correction_amount=diff
                        )
                    )
            except Exception as e:
                log.warning(
                    f"Failed to validate balance for holder {holder} "
                    f"on token {self.token_address}: {e}"
                )
                
        return corrections
