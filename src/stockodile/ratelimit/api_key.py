import json
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ApiKeyStatus:
    """Represents the status and usage metrics of an API key."""

    key: str
    remaining: int | None = None
    limit: int | None = None
    reset_at: float = 0.0  # Monotonic time when key is usable again
    consecutive_failures: int = 0


class ApiKeyPool:
    """Manages a pool of free API keys per provider.

    Tracks remaining quotas and switches keys dynamically when throttled (HTTP 429) or exhausted.

    Supports reading keys from:
    1. A dictionary passed directly via `keys`.
    2. A specified configuration file path (JSON dict with structure {provider: [keys]}).
    3. The `STOCKODILE_API_KEYS_FILE` environment variable pointing to a config file path.
    4. The `STOCKODILE_API_KEYS` environment variable containing a JSON dictionary of keys.
    5. Provider-specific environment variables matching `{PROVIDER}_API_KEYS`
       (e.g., `TIINGO_API_KEYS=key1,key2`).
    """

    def __init__(
        self,
        keys: dict[str, list[str]] | None = None,
        config_path: str | Path | None = None,
        time_func: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize the ApiKeyPool.

        Args:
            keys: A dictionary of provider name to list of API keys.
            config_path: Path to configuration file containing API keys.
            time_func: Function providing current time in seconds (defaults to time.monotonic).
        """
        self._lock = threading.Lock()
        self._time_func = time_func
        self._pools: dict[str, list[ApiKeyStatus]] = {}
        self._current_indices: dict[str, int] = {}

        # 1. Load keys passed to init
        if keys:
            for provider, key_list in keys.items():
                self.add_keys(provider, key_list)

        # 2. Load from config path
        if config_path:
            self._load_from_file(Path(config_path))

        # 3. Load from env file path
        env_file = os.environ.get("STOCKODILE_API_KEYS_FILE")
        if env_file:
            self._load_from_file(Path(env_file))

        # 4. Load from STOCKODILE_API_KEYS env var
        env_keys_json = os.environ.get("STOCKODILE_API_KEYS")
        if env_keys_json:
            try:
                data = json.loads(env_keys_json)
                if isinstance(data, dict):
                    for provider, key_list in data.items():
                        if isinstance(key_list, list):
                            self.add_keys(provider, [str(k) for k in key_list])
            except Exception as e:
                logger.error("Failed to parse STOCKODILE_API_KEYS env var: %s", e)

        # 5. Load from provider-specific env variables
        for env_name, env_val in os.environ.items():
            if env_name.endswith("_API_KEYS") and env_name != "STOCKODILE_API_KEYS":
                provider = env_name[:-9].lower()
                key_list = [k.strip() for k in env_val.split(",") if k.strip()]
                self.add_keys(provider, key_list)

    def _load_from_file(self, path: Path) -> None:
        if not path.exists():
            logger.warning("API keys config file does not exist: %s", path)
            return
        try:
            content = path.read_text(encoding="utf-8").strip()
            if not content:
                return
            data = json.loads(content)
            if isinstance(data, dict):
                for provider, key_list in data.items():
                    if isinstance(key_list, list):
                        self.add_keys(provider, [str(k) for k in key_list])
        except Exception as e:
            logger.error("Failed to load API keys from file %s: %s", path, e)

    def add_keys(self, provider: str, keys: list[str]) -> None:
        """Add a list of keys to the pool of a specific provider.

        Args:
            provider: The API provider name.
            keys: List of keys to add.
        """
        with self._lock:
            prov = provider.lower()
            if prov not in self._pools:
                self._pools[prov] = []
                self._current_indices[prov] = 0

            existing_keys = {status.key for status in self._pools[prov]}
            for key in keys:
                key = key.strip()
                if key and key not in existing_keys:
                    self._pools[prov].append(ApiKeyStatus(key=key))

    def get_key(self, provider: str) -> str | None:
        """Get an active API key for the provider.

        If all keys are throttled or exhausted, returns the one that will be available
        the earliest, or None if no keys exist for this provider.

        Args:
            provider: The API provider name.
        """
        with self._lock:
            prov = provider.lower()
            pool = self._pools.get(prov)
            if not pool:
                return None

            now = self._time_func()

            # Step 1: Look for keys that are fully available (not throttled, not exhausted)
            available_indices = []
            for idx, status in enumerate(pool):
                is_throttled = now < status.reset_at
                is_exhausted = status.remaining is not None and status.remaining <= 0
                if not is_throttled and not is_exhausted:
                    available_indices.append(idx)

            if available_indices:
                # Pick the next available key in round-robin fashion
                start_idx = self._current_indices[prov]
                for idx in available_indices:
                    if idx >= start_idx % len(pool):
                        self._current_indices[prov] = idx + 1
                        return pool[idx].key
                # Fallback to the first available index
                chosen_idx = available_indices[0]
                self._current_indices[prov] = chosen_idx + 1
                return pool[chosen_idx].key

            # Step 2: If none are fully available, look for keys that are only throttled
            throttled_keys = [
                status for status in pool if (status.remaining is None or status.remaining > 0)
            ]
            if throttled_keys:
                best = min(throttled_keys, key=lambda s: s.reset_at)
                return best.key

            # Step 3: If all keys are exhausted, pick the one that resets the earliest
            best_exhausted = min(pool, key=lambda s: s.reset_at)
            return best_exhausted.key

    def report_success(self, provider: str, key: str) -> None:
        """Report a successful request using a specific key.

        Args:
            provider: The API provider name.
            key: The API key.
        """
        with self._lock:
            prov = provider.lower()
            pool = self._pools.get(prov)
            if not pool:
                return
            for status in pool:
                if status.key == key:
                    status.consecutive_failures = 0
                    status.reset_at = 0.0
                    break

    def report_failure(self, provider: str, key: str) -> None:
        """Report a general request failure (e.g. connection timeout) using a specific key.

        Applies exponential backoff to this key.

        Args:
            provider: The API provider name.
            key: The API key.
        """
        with self._lock:
            prov = provider.lower()
            pool = self._pools.get(prov)
            if not pool:
                return
            for status in pool:
                if status.key == key:
                    status.consecutive_failures += 1
                    backoff = min(60.0, 2.0**status.consecutive_failures)
                    status.reset_at = max(status.reset_at, self._time_func() + backoff)
                    break

    def report_throttled(self, provider: str, key: str, backoff_duration: float) -> None:
        """Report that a specific key was throttled (HTTP 429).

        Applies backoff duration to this key.

        Args:
            provider: The API provider name.
            key: The API key.
            backoff_duration: Backoff delay in seconds.
        """
        with self._lock:
            prov = provider.lower()
            pool = self._pools.get(prov)
            if not pool:
                return
            for status in pool:
                if status.key == key:
                    status.reset_at = max(status.reset_at, self._time_func() + backoff_duration)
                    break

    def report_exhausted(self, provider: str, key: str, reset_in: float = 86400.0) -> None:
        """Report that a specific key has hit its daily/monthly cap.

        Marks key as exhausted (remaining = 0) and sets usability to now + reset_in.

        Args:
            provider: The API provider name.
            key: The API key.
            reset_in: The duration in seconds until the key's quota resets.
        """
        with self._lock:
            prov = provider.lower()
            pool = self._pools.get(prov)
            if not pool:
                return
            for status in pool:
                if status.key == key:
                    status.remaining = 0
                    status.reset_at = max(status.reset_at, self._time_func() + reset_in)
                    break

    def update_quota(
        self,
        provider: str,
        key: str,
        remaining: int,
        limit: int | None = None,
        reset_at_epoch: float | None = None,
    ) -> None:
        """Update quota information for a specific key.

        Args:
            provider: The API provider name.
            key: The API key.
            remaining: Remaining calls in the current period.
            limit: Maximum allowed calls in the period.
            reset_at_epoch: Unix epoch timestamp when the quota resets.
        """
        with self._lock:
            prov = provider.lower()
            pool = self._pools.get(prov)
            if not pool:
                return
            for status in pool:
                if status.key == key:
                    status.remaining = remaining
                    if limit is not None:
                        status.limit = limit
                    if reset_at_epoch is not None:
                        time_diff = reset_at_epoch - time.time()
                        if 0 < time_diff <= 86400.0:
                            status.reset_at = self._time_func() + time_diff
                        elif time_diff > 86400.0:
                            status.reset_at = self._time_func() + 86400.0
                        else:
                            status.reset_at = 0.0
                    elif remaining > 0:
                        status.reset_at = 0.0
                    break
