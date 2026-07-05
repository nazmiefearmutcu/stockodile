import json
import time
from typing import Any

import pytest

from stockodile.ratelimit import ApiKeyPool, ProxyRotator, TokenBucket


def test_proxy_rotator_empty() -> None:
    rotator = ProxyRotator()
    assert rotator.get_proxy() is None
    assert rotator.rotate() is None
    # Report failure on None/empty should not raise error
    rotator.report_failure("http://proxy1")


def test_proxy_rotator_config_file_json(tmp_path: Any) -> None:
    p = tmp_path / "proxies.json"
    p.write_text(json.dumps(["http://proxy1", "http://proxy2"]), encoding="utf-8")

    rotator = ProxyRotator(config_path=p)
    assert rotator.proxies == ["http://proxy1", "http://proxy2"]
    assert rotator.get_proxy() == "http://proxy1"
    assert rotator.rotate() == "http://proxy2"
    assert rotator.get_proxy() == "http://proxy2"
    assert rotator.rotate() == "http://proxy1"


def test_proxy_rotator_config_file_dict(tmp_path: Any) -> None:
    p = tmp_path / "proxies.json"
    p.write_text(json.dumps({"proxies": ["http://p1", "http://p2"]}), encoding="utf-8")

    rotator = ProxyRotator(config_path=p)
    assert rotator.proxies == ["http://p1", "http://p2"]


def test_proxy_rotator_config_file_text(tmp_path: Any) -> None:
    p = tmp_path / "proxies.txt"
    p.write_text("http://p1\n# comment\nhttp://p2\n  \n", encoding="utf-8")

    rotator = ProxyRotator(config_path=p)
    assert rotator.proxies == ["http://p1", "http://p2"]


def test_proxy_rotator_env_var(monkeypatch: Any) -> None:
    monkeypatch.setenv("STOCKODILE_PROXIES", "http://p1,http://p2")
    rotator = ProxyRotator()
    assert rotator.proxies == ["http://p1", "http://p2"]

    monkeypatch.setenv("STOCKODILE_PROXIES", '["http://p3", "http://p4"]')
    rotator = ProxyRotator()
    assert rotator.proxies == ["http://p3", "http://p4"]


def test_proxy_rotator_env_file(tmp_path: Any, monkeypatch: Any) -> None:
    p = tmp_path / "proxies.txt"
    p.write_text("http://p1\nhttp://p2")
    monkeypatch.setenv("STOCKODILE_PROXY_FILE", str(p))
    rotator = ProxyRotator()
    assert rotator.proxies == ["http://p1", "http://p2"]


def test_proxy_rotator_report_failure() -> None:
    rotator = ProxyRotator()
    rotator.proxies = ["http://p1", "http://p2"]
    assert rotator.get_proxy() == "http://p1"

    # Reporting failure for a different proxy shouldn't rotate
    rotator.report_failure("http://p2")
    assert rotator.get_proxy() == "http://p1"

    # Reporting failure for the current proxy should rotate
    rotator.report_failure("http://p1")
    assert rotator.get_proxy() == "http://p2"


def test_api_key_pool_init_dict() -> None:
    keys = {"tiingo": ["key1", "key2"], "openfigi": ["figi_key"]}
    pool = ApiKeyPool(keys=keys)
    assert pool.get_key("tiingo") == "key1"
    assert pool.get_key("tiingo") == "key2"
    assert pool.get_key("tiingo") == "key1"
    assert pool.get_key("openfigi") == "figi_key"


def test_api_key_pool_env_vars(monkeypatch: Any) -> None:
    monkeypatch.setenv("TIINGO_API_KEYS", "key_t1,key_t2")
    monkeypatch.setenv("STOCKODILE_API_KEYS", '{"openfigi": ["key_f1"]}')

    pool = ApiKeyPool()
    assert pool.get_key("tiingo") == "key_t1"
    assert pool.get_key("tiingo") == "key_t2"
    assert pool.get_key("openfigi") == "key_f1"


def test_api_key_pool_config_file(tmp_path: Any) -> None:
    p = tmp_path / "keys.json"
    p.write_text(json.dumps({"tiingo": ["file_t1"]}), encoding="utf-8")

    pool = ApiKeyPool(config_path=p)
    assert pool.get_key("tiingo") == "file_t1"


def test_api_key_pool_quota_and_throttling() -> None:
    # Use a custom time_func to precisely control time
    current_time = 100.0

    def time_func() -> float:
        return current_time

    pool = ApiKeyPool(keys={"tiingo": ["key1", "key2", "key3"]}, time_func=time_func)

    # All keys are initially active. Round-robin:
    assert pool.get_key("tiingo") == "key1"
    assert pool.get_key("tiingo") == "key2"
    assert pool.get_key("tiingo") == "key3"
    assert pool.get_key("tiingo") == "key1"

    # Throttle key2 for 50 seconds (reset_at = 150.0)
    pool.report_throttled("tiingo", "key2", 50.0)
    # Exclude key1 by exhausting it for 200 seconds (reset_at = 300.0)
    pool.report_exhausted("tiingo", "key1", 200.0)

    # Now key3 is the only active key. It should be returned consistently:
    assert pool.get_key("tiingo") == "key3"
    assert pool.get_key("tiingo") == "key3"

    # Throttle key3 as well (reset_at = 120.0)
    pool.report_throttled("tiingo", "key3", 20.0)

    # Now all keys are throttled/exhausted:
    # key1: reset_at = 300 (exhausted)
    # key2: reset_at = 150 (throttled)
    # key3: reset_at = 120 (throttled)
    # The pool should select key3 because it's only throttled and resets the earliest (120)
    assert pool.get_key("tiingo") == "key3"

    # Advance time to 130.0. key3 resets and becomes fully active!
    current_time = 130.0
    assert pool.get_key("tiingo") == "key3"

    # Advance time to 160.0. key2 also resets and is active!
    current_time = 160.0
    # Now active keys: key2, key3. Key rotation should happen between them:
    assert pool.get_key("tiingo") in ("key2", "key3")


def test_api_key_pool_update_quota() -> None:
    current_time = 1000.0

    def time_func() -> float:
        return current_time

    pool = ApiKeyPool(keys={"tiingo": ["key1"]}, time_func=time_func)

    # Update quota: remaining = 0, reset at epoch time = now + 10s
    epoch_now = time.time()
    pool.update_quota("tiingo", "key1", remaining=0, reset_at_epoch=epoch_now + 10.0)

    # It should be throttled (reset_at = 1010.0)
    assert pool.get_key("tiingo") == "key1"
    status = pool._pools["tiingo"][0]
    assert status.remaining == 0
    assert pytest.approx(status.reset_at, abs=1e-1) == 1010.0


def test_api_key_pool_failures_exponential_backoff() -> None:
    current_time = 10.0

    def time_func() -> float:
        return current_time

    pool = ApiKeyPool(keys={"tiingo": ["key1"]}, time_func=time_func)

    pool.report_failure("tiingo", "key1")  # backoff = 2^1 = 2s
    status = pool._pools["tiingo"][0]
    assert status.reset_at == 12.0

    pool.report_failure("tiingo", "key1")  # backoff = 2^2 = 4s
    assert status.reset_at == 14.0


@pytest.mark.asyncio
async def test_token_bucket_integration() -> None:
    # Setup proxy rotator and api key pool
    rotator = ProxyRotator()
    rotator.proxies = ["http://p1", "http://p2"]

    pool = ApiKeyPool(keys={"tiingo": ["k1", "k2"]})

    bucket = TokenBucket(
        capacity=10.0, refill_rate=2.0, proxy_rotator=rotator, api_key_pool=pool, provider="tiingo"
    )

    assert bucket.proxy_rotator is rotator
    assert bucket.api_key_pool is pool
    assert bucket.provider == "tiingo"

    # Check proxy retrieval
    assert bucket.get_proxy() == "http://p1"
    assert bucket.rotate_proxy() == "http://p2"
    assert bucket.get_proxy() == "http://p2"

    # Check API key retrieval
    assert bucket.get_api_key() == "k1"
    assert bucket.get_api_key() == "k2"

    # Test update_backoff with key and proxy
    bucket.update_backoff(delay=100.0, key="k1", proxy="http://p2")

    # Proxy rotator should rotate because p2 failed
    assert bucket.get_proxy() == "http://p1"

    # k1 should be throttled, so only k2 is returned
    assert bucket.get_api_key() == "k2"


def test_proxy_rotator_format_validation() -> None:
    import os

    os.environ["STOCKODILE_TEST_INVALID_PROXIES"] = "127.0.0.1:8080"
    with pytest.raises(ValueError, match="Scheme must be"):
        ProxyRotator(env_var="STOCKODILE_TEST_INVALID_PROXIES")


def test_proxy_rotator_failure_cooldown_and_success() -> None:
    current_time = 100.0

    def time_func() -> float:
        return current_time

    rotator = ProxyRotator(time_func=time_func)
    rotator.proxies = ["http://p1", "http://p2", "http://p3"]

    assert rotator.get_proxy() == "http://p1"

    # Report failure on p1. It should cool down and rotator rotates to p2
    rotator.report_failure("http://p1")
    assert rotator.get_proxy() == "http://p2"

    # Report failure on p2. It should cool down and rotator rotates to p3
    rotator.report_failure("http://p2")
    assert rotator.get_proxy() == "http://p3"

    # Now p1 and p2 are failed. If we ask for proxy, we get p3
    assert rotator.get_proxy() == "http://p3"

    # Report failure on p3. All are failed.
    rotator.report_failure("http://p3")

    # Fallback to the one that resets the earliest (p1: backoff=2s, reset_at=102.0)
    assert rotator.get_proxy() == "http://p1"

    # Report success on p2. It becomes immediately active
    rotator.report_success("http://p2")
    assert rotator.get_proxy() == "http://p2"


def test_api_key_pool_quota_reset_recovery() -> None:
    current_time = 1000.0

    def time_func() -> float:
        return current_time

    pool = ApiKeyPool(keys={"tiingo": ["key1"]}, time_func=time_func)

    # Exhaust key1 (reset_at = 1000 + 86400 = 87400)
    pool.report_exhausted("tiingo", "key1", reset_in=86400.0)

    # Quota resets: update_quota says remaining > 0, reset_at_epoch is None
    pool.update_quota("tiingo", "key1", remaining=100)

    # It should become immediately available (reset_at = 0.0)
    assert pool.get_key("tiingo") == "key1"
    status = pool._pools["tiingo"][0]
    assert status.reset_at == 0.0


def test_api_key_pool_success_clears_throttling() -> None:
    current_time = 100.0

    def time_func() -> float:
        return current_time

    pool = ApiKeyPool(keys={"tiingo": ["key1"]}, time_func=time_func)

    # Throttle key1 (reset_at = 160)
    pool.report_throttled("tiingo", "key1", 60.0)
    status = pool._pools["tiingo"][0]
    assert status.reset_at == 160.0

    # Succeed key1
    pool.report_success("tiingo", "key1")

    # It should clear reset_at to 0.0 immediately
    assert status.reset_at == 0.0
