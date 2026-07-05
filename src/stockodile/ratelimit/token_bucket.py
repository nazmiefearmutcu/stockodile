import asyncio
import logging
import threading
import time
from collections import deque
from collections.abc import Callable

from stockodile.ratelimit.api_key import ApiKeyPool
from stockodile.ratelimit.proxy import ProxyRotator

logger = logging.getLogger(__name__)


class TokenBucket:
    """A thread-safe-ish (asyncio event loop bound) Token Bucket rate limiter.

    Supports capacity, refill rate, async acquire, handling of HTTP 429 backoff delays,
    proxy rotation, and API key pooling.
    """

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        initial_tokens: float | None = None,
        time_func: Callable[[], float] = time.monotonic,
        proxy_rotator: ProxyRotator | None = None,
        api_key_pool: ApiKeyPool | None = None,
        provider: str | None = None,
        greedy: bool = False,
    ) -> None:
        """Initialize the Token Bucket.

        Args:
            capacity: Maximum number of tokens the bucket can hold.
            refill_rate: Number of tokens added to the bucket per second.
            initial_tokens: Initial number of tokens in the bucket. Defaults to capacity.
            time_func: Function providing current time in seconds (defaults to time.monotonic).
            proxy_rotator: Optional ProxyRotator instance to manage proxy URLs.
            api_key_pool: Optional ApiKeyPool instance to manage multiple API keys.
            provider: Optional default API provider name for key lookup.
            greedy: If True, uses greedy allocation bypassing head-of-line blocking.
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        if refill_rate <= 0:
            raise ValueError("Refill rate must be positive")

        self._capacity = float(capacity)
        self._refill_rate = float(refill_rate)

        if initial_tokens is None:
            self._tokens = self._capacity
        else:
            self._tokens = float(initial_tokens)
            if self._tokens < 0.0 or self._tokens > self._capacity:
                raise ValueError("Initial tokens must be between 0 and capacity")

        self._time_func = time_func
        self._last_refill = time_func()
        self._backoff_until = 0.0
        self._waiters: deque[tuple[float, asyncio.Future[None]]] = deque()

        self._lock = threading.Lock()
        self._timers: dict[asyncio.AbstractEventLoop, asyncio.TimerHandle] = {}
        self._greedy = greedy

        self._proxy_rotator = proxy_rotator
        self._api_key_pool = api_key_pool
        self._provider = provider

    @property
    def capacity(self) -> float:
        """The maximum token capacity of the bucket."""
        return self._capacity

    @property
    def refill_rate(self) -> float:
        """The rate at which tokens are refilled per second."""
        return self._refill_rate

    @property
    def tokens(self) -> float:
        """The current number of available tokens, accounting for refill and backoff."""
        with self._lock:
            now = self._time_func()
            if now < self._backoff_until:
                return 0.0
            if now <= self._last_refill:
                return self._tokens
            elapsed = now - self._last_refill
            return min(self._capacity, self._tokens + elapsed * self._refill_rate)

    @property
    def backoff_remaining(self) -> float:
        """The remaining backoff duration in seconds."""
        with self._lock:
            now = self._time_func()
            if now >= self._backoff_until:
                return 0.0
            return self._backoff_until - now

    @property
    def is_backed_off(self) -> bool:
        """Whether the bucket is currently under a backoff delay."""
        with self._lock:
            return self._time_func() < self._backoff_until

    @property
    def proxy_rotator(self) -> ProxyRotator | None:
        """The ProxyRotator associated with this limiter."""
        return self._proxy_rotator

    @property
    def api_key_pool(self) -> ApiKeyPool | None:
        """The ApiKeyPool associated with this limiter."""
        return self._api_key_pool

    @property
    def provider(self) -> str | None:
        """The default API provider name associated with this limiter."""
        return self._provider

    def get_proxy(self) -> str | None:
        """Get the current active proxy from the rotator."""
        if self._proxy_rotator:
            return self._proxy_rotator.get_proxy()
        return None

    def rotate_proxy(self) -> str | None:
        """Force rotate to the next proxy."""
        if self._proxy_rotator:
            return self._proxy_rotator.rotate()
        return None

    def report_proxy_failure(self, proxy: str) -> None:
        """Report a failure or timeout for a specific proxy."""
        if self._proxy_rotator:
            self._proxy_rotator.report_failure(proxy)

    def get_api_key(self, provider: str | None = None) -> str | None:
        """Get an active API key from the pool.

        Args:
            provider: The API provider name. Defaults to the limiter's default provider.
        """
        p = provider or self._provider
        if not p:
            logger.warning("No provider specified or set on the limiter.")
            return None
        if self._api_key_pool:
            return self._api_key_pool.get_key(p)
        return None

    def report_key_success(self, key: str, provider: str | None = None) -> None:
        """Report a successful request with a key."""
        p = provider or self._provider
        if p and self._api_key_pool:
            self._api_key_pool.report_success(p, key)

    def report_key_failure(self, key: str, provider: str | None = None) -> None:
        """Report a general failure with a key."""
        p = provider or self._provider
        if p and self._api_key_pool:
            self._api_key_pool.report_failure(p, key)

    def report_key_throttled(
        self,
        key: str,
        backoff_duration: float,
        provider: str | None = None,
    ) -> None:
        """Report that a key was throttled."""
        p = provider or self._provider
        if p and self._api_key_pool:
            self._api_key_pool.report_throttled(p, key, backoff_duration)

    def report_key_exhausted(
        self,
        key: str,
        reset_in: float = 86400.0,
        provider: str | None = None,
    ) -> None:
        """Report that a key has hit its daily/monthly cap."""
        p = provider or self._provider
        if p and self._api_key_pool:
            self._api_key_pool.report_exhausted(p, key, reset_in)

    def update_key_quota(
        self,
        key: str,
        remaining: int,
        limit: int | None = None,
        reset_at_epoch: float | None = None,
        provider: str | None = None,
    ) -> None:
        """Update quota information for a key."""
        p = provider or self._provider
        if p and self._api_key_pool:
            self._api_key_pool.update_quota(p, key, remaining, limit, reset_at_epoch)

    def update_backoff(
        self,
        delay: float,
        key: str | None = None,
        proxy: str | None = None,
        provider: str | None = None,
    ) -> None:
        """Temporarily pause request acquisition by setting an HTTP 429 backoff delay.

        During the backoff period, the bucket behaves as if it has 0 tokens, and any pending
        or new acquisitions will be delayed until the backoff expires.

        Args:
            delay: The backoff delay in seconds.
            key: If provided, reports the API key as throttled in the API key pool.
            proxy: If provided, reports the proxy as failed in the proxy rotator.
            provider: Override or specify the provider for the API key pool.
        """
        if delay < 0:
            raise ValueError("Backoff delay must be non-negative")

        with self._lock:
            now = self._time_func()

            # Handle API key throttling if key is provided
            has_other_keys = False
            p = provider or self._provider
            if key and self._api_key_pool and p:
                self._api_key_pool.report_throttled(p, key, delay)
                pool = self._api_key_pool._pools.get(p.lower())
                if pool:
                    other_available = 0
                    for status in pool:
                        if status.key != key:
                            is_throttled = now < status.reset_at
                            is_exhausted = status.remaining is not None and status.remaining <= 0
                            if not is_throttled and not is_exhausted:
                                other_available += 1
                    if other_available > 0:
                        has_other_keys = True

            # Handle proxy failure/rotation if proxy is provided
            has_other_proxies = False
            if proxy and self._proxy_rotator:
                self._proxy_rotator.report_failure(proxy)
                active_proxies = [
                    pr
                    for pr in self._proxy_rotator.proxies
                    if not self._proxy_rotator.is_proxy_failed(pr)
                ]
                if len(active_proxies) > 0:
                    has_other_proxies = True

            should_backoff_bucket = True
            if key and has_other_keys:
                should_backoff_bucket = False
            if proxy and has_other_proxies:
                should_backoff_bucket = False

            if should_backoff_bucket:
                self._backoff_until = max(self._backoff_until, now + delay)
                self._tokens = 0.0
                self._last_refill = max(self._last_refill, self._backoff_until)

        # Trigger process queue in all event loops with active waiters
        with self._lock:
            loops = {w[1].get_loop() for w in self._waiters if not w[1].done()}
        for loop in loops:
            try:
                loop.call_soon_threadsafe(self._process_queue)
            except RuntimeError:
                pass

        try:
            current_loop = asyncio.get_running_loop()
            if current_loop not in loops and current_loop.is_running():
                current_loop.call_soon(self._process_queue)
        except RuntimeError:
            pass

    async def acquire(self, tokens: float = 1.0) -> None:
        """Acquire the specified number of tokens, blocking asynchronously if necessary.

        Args:
            tokens: Number of tokens to acquire. Must be non-negative and <= capacity.
        """
        if tokens < 0:
            raise ValueError("Requested tokens must be non-negative")
        if tokens > self._capacity:
            raise ValueError(f"Requested tokens {tokens} exceeds bucket capacity {self._capacity}")

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        with self._lock:
            self._waiters.append((tokens, future))
            future.add_done_callback(self._waiter_done)

        self._process_queue()
        await future

    def _waiter_done(self, future: asyncio.Future[None]) -> None:
        """Callback triggered when a waiter future is resolved or cancelled."""
        with self._lock:
            for item in list(self._waiters):
                if item[1] is future:
                    self._waiters.remove(item)
                    break
        self._process_queue()

    def _on_timer(self, loop: asyncio.AbstractEventLoop) -> None:
        """Callback triggered when a scheduled refill or backoff timer fires."""
        with self._lock:
            if loop in self._timers:
                self._timers.pop(loop)
        self._process_queue()

    def _process_queue(self) -> None:
        """Process the waiter queue and satisfy or schedule waiters as appropriate."""
        with self._lock:
            for _loop, timer in list(self._timers.items()):
                timer.cancel()
            self._timers.clear()

            now = self._time_func()

            # Refill tokens up to now, if we are not in a backoff period
            if now > self._last_refill:
                self._tokens = min(
                    self._capacity, self._tokens + (now - self._last_refill) * self._refill_rate
                )
                self._last_refill = now

            while self._waiters and self._waiters[0][1].done():
                self._waiters.popleft()

            if not self._waiters:
                return

            if now < self._backoff_until:
                wait_time = self._backoff_until - now + 1e-3
                loops = {w[1].get_loop() for w in self._waiters if not w[1].done()}
                for loop in loops:
                    try:
                        self._timers[loop] = loop.call_later(wait_time, self._on_timer, loop)
                    except RuntimeError:
                        pass
                return

            if self._greedy:
                new_waiters: deque[tuple[float, asyncio.Future[None]]] = deque()
                for tokens, future in self._waiters:
                    if future.done():
                        continue
                    if self._tokens >= tokens:
                        self._tokens -= tokens
                        if not future.done():
                            future.get_loop().call_soon_threadsafe(future.set_result, None)
                    else:
                        new_waiters.append((tokens, future))
                self._waiters = new_waiters

                # Schedule timers for the remaining waiters
                loop_to_waiter: dict[asyncio.AbstractEventLoop, float] = {}
                for tokens, future in self._waiters:
                    loop = future.get_loop()
                    if loop not in loop_to_waiter:
                        loop_to_waiter[loop] = tokens

                for loop, tokens in loop_to_waiter.items():
                    needed = tokens - self._tokens
                    wait_time = (needed / self._refill_rate) + 1e-3
                    try:
                        self._timers[loop] = loop.call_later(wait_time, self._on_timer, loop)
                    except RuntimeError:
                        pass
            else:
                while self._waiters:
                    tokens, future = self._waiters[0]
                    if future.done():
                        self._waiters.popleft()
                        continue

                    if self._tokens >= tokens:
                        self._tokens -= tokens
                        self._waiters.popleft()
                        if not future.done():
                            future.get_loop().call_soon_threadsafe(future.set_result, None)
                    else:
                        needed = tokens - self._tokens
                        wait_time = (needed / self._refill_rate) + 1e-3
                        loop = future.get_loop()
                        try:
                            self._timers[loop] = loop.call_later(wait_time, self._on_timer, loop)
                        except RuntimeError:
                            pass
                        return


class TokenBucketLimiter(TokenBucket):
    """An alias of TokenBucket matching the TokenBucketLimiter name, for compatibility.

    Accepts `rate` instead of `refill_rate`.
    """

    def __init__(
        self,
        rate: float,
        capacity: float,
        initial_tokens: float | None = None,
        time_func: Callable[[], float] = time.monotonic,
        proxy_rotator: ProxyRotator | None = None,
        api_key_pool: ApiKeyPool | None = None,
        provider: str | None = None,
        greedy: bool = False,
    ) -> None:
        """Initialize TokenBucketLimiter."""
        super().__init__(
            capacity=capacity,
            refill_rate=rate,
            initial_tokens=initial_tokens,
            time_func=time_func,
            proxy_rotator=proxy_rotator,
            api_key_pool=api_key_pool,
            provider=provider,
            greedy=greedy,
        )
