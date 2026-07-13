"""OpenFIGI client implementation supporting mapping, rate limiting, and caching."""

import asyncio
import logging
from typing import Any, Self

import aiohttp
import msgspec

from stockodile.providers.openfigi.cache import InMemoryCache, OpenFigiCache
from stockodile.providers.openfigi.models import (
    FigiRecord,
    OpenFigiJob,
    OpenFigiResponseItem,
    map_raw_to_record,
)
from stockodile.ratelimit.token_bucket import TokenBucketLimiter

logger = logging.getLogger(__name__)


class OpenFigiClient:
    """Client for the OpenFIGI mapping API."""

    def __init__(
        self,
        api_key: str | None = None,
        cache: OpenFigiCache | None = None,
        session: aiohttp.ClientSession | None = None,
        base_url: str = "https://api.openfigi.com/v3/mapping",
    ) -> None:
        """Initialize the OpenFIGI API client.

        Args:
            api_key: Optional OpenFIGI API key.
            cache: Optional cache implementation. Defaults to InMemoryCache.
            session: Optional existing ClientSession.
            base_url: The API endpoint URL (defaults to v3 mapping).
        """
        self.api_key = api_key
        self.cache = cache if cache is not None else InMemoryCache()
        self._session = session
        self.base_url = base_url

        # Initialize rate limiter based on presence of API key
        if api_key:
            # 25 requests per 6 seconds
            self.rate_limiter = TokenBucketLimiter(rate=25.0 / 6.0, capacity=25.0)
            self._batch_size = 100
        else:
            # 25 requests per minute; unauthenticated job batch max is 10
            self.rate_limiter = TokenBucketLimiter(rate=25.0 / 60.0, capacity=25.0)
            self._batch_size = 10

    async def _get_session(self) -> aiohttp.ClientSession:
        """Retrieve or construct the active ClientSession."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Close the internal HTTP session if owned by the client."""
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def map_job(self, job: OpenFigiJob) -> list[FigiRecord]:
        """Map a single OpenFIGI job to FIGI records.

        Args:
            job: The OpenFigiJob mapping query.

        Returns:
            A list of matched FigiRecord objects.
        """
        results = await self.map_jobs([job])
        return results[0]

    async def map_jobs(self, jobs: list[OpenFigiJob]) -> list[list[FigiRecord]]:
        """Map a batch of OpenFIGI jobs to FIGI records.

        Uses local cache to satisfy requests before hitting the API, and batches
        remaining requests.

        Args:
            jobs: List of OpenFigiJob mapping queries.

        Returns:
            A list of lists of matched FigiRecord objects.
        """
        if not jobs:
            return []

        output: list[list[FigiRecord] | None] = [None] * len(jobs)
        pending_indices: list[int] = []

        # 1. Check local cache first
        for idx, job in enumerate(jobs):
            cached = await self.cache.get(job)
            if cached is not None:
                output[idx] = cached
            else:
                pending_indices.append(idx)

        if not pending_indices:
            # All results satisfied from cache
            return [res for res in output if res is not None]

        # 2. Extract pending jobs and partition by authenticated batch limit
        batch_size = self._batch_size
        pending_jobs = [jobs[idx] for idx in pending_indices]
        batches = [
            pending_jobs[i : i + batch_size] for i in range(0, len(pending_jobs), batch_size)
        ]

        # 3. Execute all batches concurrently (under TokenBucketLimiter control)
        tasks = [self._execute_batch(batch) for batch in batches]
        batch_results = await asyncio.gather(*tasks)

        # Flatten batch results
        flat_results = [res for batch_res in batch_results for res in batch_res]
        if len(flat_results) != len(pending_indices):
            raise RuntimeError(
                f"OpenFIGI response length mismatch: got {len(flat_results)} "
                f"results for {len(pending_indices)} jobs"
            )

        # 4. Populate cache and outputs (skip caching API error empties is best-effort)
        for i, idx in enumerate(pending_indices):
            job = jobs[idx]
            records = flat_results[i]
            await self.cache.set(job, records)
            output[idx] = records

        return [res for res in output if res is not None]

    async def _execute_batch(self, batch_jobs: list[OpenFigiJob]) -> list[list[FigiRecord]]:
        """Execute a single batch request to OpenFIGI API with rate limit respect and retries."""
        headers = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.api_key

        payload = []
        for job in batch_jobs:
            item: dict[str, str] = {
                "idType": job.id_type,
                "idValue": job.id_value,
            }
            if job.exch_code is not None:
                item["exchCode"] = job.exch_code
            if job.mic_code is not None:
                item["micCode"] = job.mic_code
            if job.currency is not None:
                item["currency"] = job.currency
            if job.market_sec_des is not None:
                item["marketSecDes"] = job.market_sec_des
            payload.append(item)

        session = await self._get_session()
        max_retries = 5
        retry_delay = 5.0

        for _attempt in range(max_retries):
            # Block until rate limiter permits request
            await self.rate_limiter.acquire(1.0)

            try:
                async with session.post(
                    self.base_url,
                    json=payload,
                    headers=headers,
                ) as response:
                    if response.status == 200:
                        body = await response.read()
                        response_data = msgspec.json.decode(body, type=list[OpenFigiResponseItem])
                        if len(response_data) != len(batch_jobs):
                            raise RuntimeError(
                                f"OpenFIGI batch length mismatch: sent {len(batch_jobs)} "
                                f"jobs, got {len(response_data)} responses"
                            )

                        results: list[list[FigiRecord]] = []
                        for resp_item in response_data:
                            if resp_item.data is not None:
                                results.append([map_raw_to_record(r) for r in resp_item.data])
                            else:
                                results.append([])
                        return results

                    elif response.status == 413:
                        # Payload too large — shrink batch and retry (unauthenticated limit)
                        if len(batch_jobs) <= 1:
                            body_text = await response.text()
                            raise RuntimeError(
                                f"OpenFIGI 413 for single job: {body_text[:200]}"
                            )
                        mid = max(1, len(batch_jobs) // 2)
                        logger.warning(
                            "OpenFIGI 413; splitting batch of %d into %d + %d",
                            len(batch_jobs),
                            mid,
                            len(batch_jobs) - mid,
                        )
                        left = await self._execute_batch(batch_jobs[:mid])
                        right = await self._execute_batch(batch_jobs[mid:])
                        return left + right

                    elif response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        delay = retry_delay
                        if retry_after is not None:
                            try:
                                delay = float(retry_after)
                            except ValueError:
                                pass
                        logger.warning(
                            "OpenFIGI rate limit exceeded (HTTP 429). "
                            "Backing off for %.2f seconds.",
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    else:
                        response.raise_for_status()

            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    retry_after = e.headers.get("Retry-After") if e.headers else None
                    delay = retry_delay
                    if retry_after is not None:
                        try:
                            delay = float(retry_after)
                        except ValueError:
                            pass
                    logger.warning(
                        "OpenFIGI rate limit exceeded (HTTP 429) inside exception. "
                        "Backing off for %.2f seconds.",
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise
            except Exception as e:
                logger.error("Error executing OpenFIGI batch mapping: %s", str(e))
                raise

        raise RuntimeError(
            f"Failed to map batch after {max_retries} attempts due to rate limiting."
        )
