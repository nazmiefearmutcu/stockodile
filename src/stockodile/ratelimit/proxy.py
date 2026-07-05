import json
import logging
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger(__name__)


class ProxyRotator:
    """Manages a pool of proxy URLs, rotating them on failure or timeout.

    Supports reading proxies from:
    1. A specified configuration file path (JSON list, JSON dict with 'proxies' key,
       or plain text with one proxy per line).
    2. The `STOCKODILE_PROXY_FILE` environment variable pointing to a config file.
    3. The `STOCKODILE_PROXIES` environment variable (comma-separated or JSON list).
    """

    def __init__(
        self,
        config_path: str | Path | None = None,
        env_var: str = "STOCKODILE_PROXIES",
        env_file_var: str = "STOCKODILE_PROXY_FILE",
        time_func: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize the ProxyRotator.

        Args:
            config_path: Path to configuration file containing proxies.
            env_var: Environment variable holding comma-separated or JSON list of proxies.
            env_file_var: Environment variable pointing to a configuration file path.
            time_func: Function providing current time in seconds.
        """
        self._lock = threading.Lock()
        self._time_func = time_func
        self._consecutive_failures: dict[str, int] = {}
        self._failed_until: dict[str, float] = {}

        self.proxies: list[str] = []
        self._current_idx: int = 0

        # Load from config path if provided
        if config_path:
            self._load_from_file(Path(config_path))

        # Load from env file path if still empty
        if not self.proxies and os.environ.get(env_file_var):
            self._load_from_file(Path(os.environ[env_file_var]))

        # Load from env variable if still empty
        if not self.proxies and os.environ.get(env_var):
            self._load_from_env_var(os.environ[env_var])

        # Clean/strip proxies & validate schemes
        valid_proxies = []
        for p in self.proxies:
            p_clean = p.strip()
            if not p_clean:
                continue
            if not (
                p_clean.startswith("http://")
                or p_clean.startswith("https://")
                or p_clean.startswith("socks5://")
                or p_clean.startswith("socks4://")
            ):
                raise ValueError(
                    f"Invalid proxy URL: '{p_clean}'. "
                    "Scheme must be http, https, socks4, or socks5."
                )
            valid_proxies.append(p_clean)
        self.proxies = valid_proxies

    def _load_from_file(self, path: Path) -> None:
        if not path.exists():
            logger.warning("Proxy config file does not exist: %s", path)
            return

        try:
            content = path.read_text(encoding="utf-8").strip()
            if not content:
                return

            # Try parsing as JSON first
            if content.startswith("[") or content.startswith("{"):
                try:
                    data = json.loads(content)
                    if isinstance(data, list):
                        self.proxies = [str(p) for p in data]
                        return
                    if isinstance(data, dict) and "proxies" in data:
                        self.proxies = [str(p) for p in data["proxies"]]
                        return
                except json.JSONDecodeError:
                    pass

            # Fallback to plain text line-by-line
            for line in content.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    self.proxies.append(line)
        except Exception as e:
            logger.error("Failed to load proxies from %s: %s", path, e)

    def _load_from_env_var(self, value: str) -> None:
        value = value.strip()
        if value.startswith("[") and value.endswith("]"):
            try:
                self.proxies = [str(p) for p in json.loads(value)]
                return
            except json.JSONDecodeError:
                pass
        self.proxies = [p.strip() for p in value.split(",") if p.strip()]

    def is_proxy_failed(self, proxy: str) -> bool:
        """Check if a proxy is currently marked as failed (in cooldown)."""
        with self._lock:
            p = proxy.strip()
            now = self._time_func()
            until = self._failed_until.get(p, 0.0)
            return now < until

    def get_proxy(self) -> str | None:
        """Get the current active proxy, or None if no proxies are configured."""
        with self._lock:
            if not self.proxies:
                return None

            now = self._time_func()
            num_proxies = len(self.proxies)

            for i in range(num_proxies):
                idx = (self._current_idx + i) % num_proxies
                proxy = self.proxies[idx]
                until = self._failed_until.get(proxy, 0.0)
                if now >= until:
                    self._current_idx = idx
                    return proxy

            # Fallback: all proxies failed, pick the one that will be available earliest
            best_proxy = min(self.proxies, key=lambda pr: self._failed_until.get(pr, 0.0))
            self._current_idx = self.proxies.index(best_proxy)
            return best_proxy

    def rotate(self) -> str | None:
        """Rotate to the next proxy and return it."""
        with self._lock:
            if not self.proxies:
                return None
            self._current_idx = (self._current_idx + 1) % len(self.proxies)

        proxy = self.get_proxy()
        logger.info("Rotated to next proxy: %s", proxy)
        return proxy

    def report_failure(self, proxy: str) -> None:
        """Report a failure or timeout for a specific proxy.

        Applies an exponential backoff cooldown to the proxy and rotates if it was the current one.
        """
        p = proxy.strip()
        with self._lock:
            if not self.proxies or p not in self.proxies:
                return

            now = self._time_func()
            failures = self._consecutive_failures.get(p, 0) + 1
            self._consecutive_failures[p] = failures
            backoff = min(60.0, 2.0**failures)
            self._failed_until[p] = now + backoff

            logger.warning(
                "Proxy failed: %s (consecutive failures: %d). Backing off for %.1fs.",
                p,
                failures,
                backoff,
            )

        current = self.get_proxy()
        if current == p:
            self.rotate()

    def report_success(self, proxy: str) -> None:
        """Report a successful request using a specific proxy."""
        p = proxy.strip()
        with self._lock:
            if p in self._consecutive_failures:
                self._consecutive_failures[p] = 0
            if p in self._failed_until:
                self._failed_until[p] = 0.0
