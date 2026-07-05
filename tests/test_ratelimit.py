import asyncio
import time

import pytest

from stockodile.ratelimit import TokenBucket


@pytest.mark.asyncio
async def test_token_bucket_initial_state() -> None:
    bucket = TokenBucket(capacity=10.0, refill_rate=2.0)
    assert bucket.capacity == 10.0
    assert bucket.refill_rate == 2.0
    assert bucket.tokens == pytest.approx(10.0, abs=1e-3)
    assert not bucket.is_backed_off
    assert bucket.backoff_remaining == 0.0


@pytest.mark.asyncio
async def test_token_bucket_acquire_immediate() -> None:
    bucket = TokenBucket(capacity=10.0, refill_rate=2.0)
    start = time.monotonic()
    await bucket.acquire(4.0)
    # Since tokens are available, it should return immediately
    duration = time.monotonic() - start
    assert duration < 0.05
    assert bucket.tokens == pytest.approx(6.0, abs=1e-2)


@pytest.mark.asyncio
async def test_token_bucket_acquire_delay() -> None:
    # Capacity = 2, refill_rate = 10 (1 token per 0.1s), initial_tokens = 0
    bucket = TokenBucket(capacity=2.0, refill_rate=10.0, initial_tokens=0.0)
    assert bucket.tokens == pytest.approx(0.0, abs=1e-3)

    start = time.monotonic()
    # Acquire 1 token. Should take approx 0.1s to refill.
    await bucket.acquire(1.0)
    duration = time.monotonic() - start
    assert 0.08 <= duration <= 0.25
    assert bucket.tokens < 0.5


@pytest.mark.asyncio
async def test_token_bucket_invalid_args() -> None:
    with pytest.raises(ValueError):
        TokenBucket(capacity=-1.0, refill_rate=2.0)
    with pytest.raises(ValueError):
        TokenBucket(capacity=10.0, refill_rate=-1.0)
    with pytest.raises(ValueError):
        TokenBucket(capacity=10.0, refill_rate=2.0, initial_tokens=15.0)
    with pytest.raises(ValueError):
        TokenBucket(capacity=10.0, refill_rate=2.0, initial_tokens=-1.0)

    bucket = TokenBucket(capacity=10.0, refill_rate=2.0)
    with pytest.raises(ValueError):
        await bucket.acquire(-1.0)
    with pytest.raises(ValueError):
        await bucket.acquire(15.0)


@pytest.mark.asyncio
async def test_token_bucket_cancellation() -> None:
    bucket = TokenBucket(capacity=1.0, refill_rate=1.0, initial_tokens=0.0)

    # Start acquire in background task
    task = asyncio.create_task(bucket.acquire(1.0))
    await asyncio.sleep(0.01)  # Allow it to register as waiter

    assert len(bucket._waiters) == 1
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    # Give a tiny slice to run loop done callbacks
    await asyncio.sleep(0.01)
    assert len(bucket._waiters) == 0


@pytest.mark.asyncio
async def test_token_bucket_backoff() -> None:
    bucket = TokenBucket(capacity=5.0, refill_rate=10.0, initial_tokens=5.0)

    # Trigger a 0.2 second backoff
    bucket.update_backoff(0.2)
    assert bucket.is_backed_off
    assert 0.1 <= bucket.backoff_remaining <= 0.25
    assert bucket.tokens == pytest.approx(0.0, abs=1e-3)

    start = time.monotonic()
    # Attempt to acquire 1 token. It must wait until backoff expires, and then
    # since tokens were cleared to 0.0, wait another 0.1 seconds (1 token / 10 tokens/sec = 0.1s).
    # So total wait should be approx 0.3s.
    await bucket.acquire(1.0)
    duration = time.monotonic() - start
    assert 0.25 <= duration <= 0.45
    assert not bucket.is_backed_off


@pytest.mark.asyncio
async def test_token_bucket_fifo_order() -> None:
    bucket = TokenBucket(capacity=5.0, refill_rate=10.0, initial_tokens=0.0)

    results = []

    async def worker(worker_id: int, tokens: float) -> None:
        await bucket.acquire(tokens)
        results.append(worker_id)

    # Schedule worker 1 (needs 1 token, should wait 0.1s)
    # Schedule worker 2 (needs 2 tokens, should wait 0.3s total)
    # Schedule worker 3 (needs 1 token, should wait 0.4s total)
    task1 = asyncio.create_task(worker(1, 1.0))
    await asyncio.sleep(0.01)
    task2 = asyncio.create_task(worker(2, 2.0))
    await asyncio.sleep(0.01)
    task3 = asyncio.create_task(worker(3, 1.0))

    await asyncio.gather(task1, task2, task3)
    assert results == [1, 2, 3]


@pytest.mark.asyncio
async def test_token_bucket_non_head_waiter_cancellation() -> None:
    bucket = TokenBucket(capacity=1.0, refill_rate=1.0, initial_tokens=0.0)

    task1 = asyncio.create_task(bucket.acquire(1.0))
    await asyncio.sleep(0.01)
    task2 = asyncio.create_task(bucket.acquire(1.0))
    await asyncio.sleep(0.01)
    task3 = asyncio.create_task(bucket.acquire(1.0))
    await asyncio.sleep(0.01)

    assert len(bucket._waiters) == 3

    task2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task2

    await asyncio.sleep(0.01)

    assert len(bucket._waiters) == 2
    assert not bucket._waiters[0][1].done()
    assert not bucket._waiters[1][1].done()

    task1.cancel()
    task3.cancel()
    try:
        await task1
    except asyncio.CancelledError:
        pass
    try:
        await task3
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_token_bucket_greedy_allocation() -> None:
    bucket = TokenBucket(capacity=5.0, refill_rate=10.0, initial_tokens=2.0, greedy=True)

    results = []

    async def worker(worker_id: int, tokens: float) -> None:
        await bucket.acquire(tokens)
        results.append(worker_id)

    task1 = asyncio.create_task(worker(1, 5.0))
    await asyncio.sleep(0.01)
    task2 = asyncio.create_task(worker(2, 1.0))
    await asyncio.sleep(0.01)

    await task2
    assert results == [2]

    task1.cancel()
    try:
        await task1
    except asyncio.CancelledError:
        pass


def test_token_bucket_multi_loop_concurrency() -> None:
    import threading

    bucket = TokenBucket(capacity=10.0, refill_rate=100.0, initial_tokens=10.0)

    loop1 = asyncio.new_event_loop()
    loop2 = asyncio.new_event_loop()

    results = []

    def run_loop1() -> None:
        asyncio.set_event_loop(loop1)

        async def work() -> None:
            await bucket.acquire(5.0)
            results.append("loop1")

        loop1.run_until_complete(work())

    def run_loop2() -> None:
        asyncio.set_event_loop(loop2)

        async def work() -> None:
            await bucket.acquire(5.0)
            results.append("loop2")

        loop2.run_until_complete(work())

    t1 = threading.Thread(target=run_loop1)
    t2 = threading.Thread(target=run_loop2)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert set(results) == {"loop1", "loop2"}
    assert bucket.tokens == pytest.approx(0.0, abs=0.5)

    loop1.close()
    loop2.close()
