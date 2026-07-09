import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

class SyncRecovery:
    def __init__(self, state_path: str) -> None:
        self.state_path = state_path
        self.state: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path) as f:
                    self.state = json.load(f)
            except Exception as e:
                log.warning(f"Failed to load sync recovery state from {self.state_path}: {e}")
                self.state = {}

    def _save(self) -> None:
        tmp_path = self.state_path + ".tmp"
        try:
            with open(tmp_path, "w") as f:
                json.dump(self.state, f)
            os.replace(tmp_path, self.state_path)
        except Exception as e:
            log.error(f"Failed to save sync recovery state: {e}")

    def get_last_block(self, pool: str) -> int | None:
        val = self.state.get(pool)
        return int(val) if val is not None else None

    def save_last_block(self, pool: str, block: int) -> None:
        self.state[pool] = block
        self._save()
