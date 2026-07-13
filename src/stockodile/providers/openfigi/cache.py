"""Caching implementation for OpenFIGI provider."""

import asyncio
import sqlite3
from pathlib import Path
from typing import Protocol

import msgspec

from stockodile.providers.openfigi.models import FigiRecord, OpenFigiJob


def make_cache_key(job: OpenFigiJob) -> str:
    """Generate a stable unique cache key for an OpenFigiJob."""
    return msgspec.json.encode(job).decode("utf-8")


class OpenFigiCache(Protocol):
    """Protocol defining the caching interface for OpenFIGI provider."""

    async def get(self, job: OpenFigiJob) -> list[FigiRecord] | None:
        """Retrieve cached FIGI records for a job. Returns None on cache miss."""
        ...

    async def set(self, job: OpenFigiJob, records: list[FigiRecord]) -> None:
        """Cache FIGI records for a job."""
        ...


class InMemoryCache:
    """In-memory dictionary cache implementation for OpenFIGI."""

    def __init__(self) -> None:
        self._cache: dict[str, list[FigiRecord]] = {}

    async def get(self, job: OpenFigiJob) -> list[FigiRecord] | None:
        key = make_cache_key(job)
        return self._cache.get(key)

    async def set(self, job: OpenFigiJob, records: list[FigiRecord]) -> None:
        key = make_cache_key(job)
        self._cache[key] = records


class SQLiteCache:
    """Persistent SQLite database cache implementation for OpenFIGI."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS openfigi_cache (
                    cache_key TEXT PRIMARY KEY,
                    response_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def _get_sync(self, key: str) -> str | None:
        with self._connect() as conn:
            cursor = conn.execute(
                "SELECT response_json FROM openfigi_cache WHERE cache_key = ?", (key,)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def _set_sync(self, key: str, value_json: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO openfigi_cache (cache_key, response_json) VALUES (?, ?)",
                (key, value_json),
            )

    async def get(self, job: OpenFigiJob) -> list[FigiRecord] | None:
        key = make_cache_key(job)
        res_json = await asyncio.to_thread(self._get_sync, key)
        if res_json is None:
            return None
        return list(msgspec.json.decode(res_json.encode("utf-8"), type=list[FigiRecord]))

    async def set(self, job: OpenFigiJob, records: list[FigiRecord]) -> None:
        key = make_cache_key(job)
        res_json = msgspec.json.encode(records).decode("utf-8")
        await asyncio.to_thread(self._set_sync, key, res_json)
