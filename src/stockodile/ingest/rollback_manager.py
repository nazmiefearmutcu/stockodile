import logging
from typing import Any

log = logging.getLogger(__name__)

class RollbackManager:
    def __init__(self, max_depth: int = 100) -> None:
        self.max_depth = max_depth
        # block_num -> {"hash": block_hash, "parent_hash": parent_hash, "records": list}
        self.buffer: dict[int, dict[str, Any]] = {}

    def process_block(
        self, block_number: int, block_hash: str, parent_hash: str, records: list[Any]
    ) -> int | None:
        """
        Adds block to buffer and detects reorgs.
        If a reorg is detected, returns the fork block number to rollback to.
        Otherwise, returns None.
        """
        if len(self.buffer) >= self.max_depth:
            keys_sorted = sorted(self.buffer.keys())
            while len(self.buffer) >= self.max_depth and keys_sorted:
                self.buffer.pop(keys_sorted.pop(0), None)

        prev_block_num = block_number - 1
        if prev_block_num in self.buffer:
            stored_hash = self.buffer[prev_block_num]["hash"]
            if stored_hash != parent_hash:
                log.warning(
                    f"Reorg detected at block {block_number}. "
                    f"Parent hash mismatch: expected {stored_hash}, got {parent_hash}."
                )
                
                # Discard all blocks from buffer starting from prev_block_num
                fork_point = prev_block_num
                keys_to_remove = [k for k in self.buffer.keys() if k >= fork_point]
                for k in keys_to_remove:
                    self.buffer.pop(k, None)
                    
                return fork_point

        self.buffer[block_number] = {
            "hash": block_hash,
            "parent_hash": parent_hash,
            "records": records
        }
        return None
