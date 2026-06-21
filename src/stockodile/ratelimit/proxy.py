import json
import logging
import os
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
    ) -> None:
        """Initialize the ProxyRotator.

        Args:
            config_path: Path to configuration file containing proxies.
            env_var: Environment variable holding comma-separated or JSON list of proxies.
            env_file_var: Environment variable pointing to a configuration file path.
        """
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

        # Clean/strip proxies
        self.proxies = [p.strip() for p in self.proxies if p.strip()]

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

    def get_proxy(self) -> str | None:
        """Get the current active proxy, or None if no proxies are configured."""
        if not self.proxies:
            return None
        return self.proxies[self._current_idx % len(self.proxies)]

    def rotate(self) -> str | None:
        """Rotate to the next proxy and return it."""
        if not self.proxies:
            return None
        self._current_idx = (self._current_idx + 1) % len(self.proxies)
        proxy = self.get_proxy()
        logger.info("Rotated to next proxy: %s", proxy)
        return proxy

    def report_failure(self, proxy: str) -> None:
        """Report a failure or timeout for a specific proxy.

        If the failed proxy is the current one, triggers rotation.
        """
        if not self.proxies:
            return
        current = self.get_proxy()
        if current == proxy.strip():
            logger.warning("Proxy failed: %s. Rotating proxy.", proxy)
            self.rotate()
