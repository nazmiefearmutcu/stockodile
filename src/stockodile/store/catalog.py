"""DuckDB-backed catalog for querying hive-partitioned Parquet data.

Design:
    - ``Catalog(data_dir)`` builds per-channel DuckDB views over
      ``read_parquet(glob, hive_partitioning=true, union_by_name=true)``.
    - ``query(sql)`` executes arbitrary SQL against registered views,
      returns a Polars DataFrame.
    - ``scan(channel, symbol, start_ns, end_ns)`` narrows the glob path by
      provider/channel/date **before** the WHERE clause for partition pruning,
      then filters by ``symbol`` and ``local_ts`` range, returns a Polars DataFrame
      ordered by ``local_ts``.

Partition layout:
    data/provider={P}/channel={C}/date=YYYY-MM-DD/bucket={0..127}/part-*.parquet
    or (for low frequency channels):
    data/provider={P}/channel={C}/date=YYYY-MM-DD/part-*.parquet

Views registered:
    One DuckDB VIEW per channel found on disk, named by the channel string.
    Views are created lazily on first access and re-created whenever
    ``refresh_views()`` is called explicitly.
"""

from __future__ import annotations

import datetime
import glob as _glob
import logging
import re
import threading
from pathlib import Path
from typing import Self

import duckdb
import polars as pl

from stockodile.store.rows import _symbol_bucket

log = logging.getLogger(__name__)

# Safe channel / view identifiers (no SQL / glob metacharacters)
_SAFE_CHANNEL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Patterns blocked for network-facing / restricted SQL (MCP)
_UNSAFE_SQL_RE = re.compile(
    r"\b(COPY|ATTACH|INSTALL|LOAD|PRAGMA|CALL|EXPORT|IMPORT|CREATE\s+OR\s+REPLACE\s+TABLE|"
    r"DROP|ALTER|INSERT|UPDATE|DELETE|TRUNCATE|GRANT|REVOKE)\b"
    r"|read_csv|read_csv_auto|read_json|read_json_auto|read_parquet\s*\(\s*['\"]/"
    r"|read_blob|read_text",
    re.IGNORECASE,
)


def assert_readonly_sql(sql: str) -> None:
    """Reject multi-statement and mutating / external-access SQL.

    Used by MCP and other network-facing query entry points. Local CLI may
    still call ``Catalog.query`` without this guard for power users.
    """
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        raise ValueError("Empty SQL")
    # Single statement only
    if ";" in stripped:
        raise ValueError("Multi-statement SQL is not allowed")
    upper = stripped.lstrip().upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH") or upper.startswith("DESCRIBE")
            or upper.startswith("SHOW") or upper.startswith("EXPLAIN")):
        raise ValueError("Only SELECT/WITH/DESCRIBE/SHOW/EXPLAIN queries are allowed")
    if _UNSAFE_SQL_RE.search(stripped):
        raise ValueError("SQL contains disallowed keywords or external readers")


class Catalog:
    """Query interface over a hive-partitioned Parquet data lake.

    Args:
        data_dir: Root of the data lake.
    """

    def __init__(self, data_dir: Path | str) -> None:
        self._data_dir = Path(data_dir)
        self._lock = threading.Lock()
        self._thread_local = threading.local()
        # In-memory DuckDB connection.
        # Note: enable_external_access stays on so hive parquet views/scan work.
        # Network-facing callers must use query(..., readonly=True) allowlist.
        self._conn = duckdb.connect()
        self._registered_channels: set[str] = set()
        self._closed = False
        # Register views for all channels present on disk.
        self._refresh_views()

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        if self._closed:
            raise RuntimeError("Catalog is closed")
        if not hasattr(self._thread_local, "conn"):
            self._thread_local.conn = self._conn.cursor()
        return self._thread_local.conn  # type: ignore[no-any-return]

    def close(self) -> None:
        """Close thread-local cursors and the main DuckDB connection."""
        if self._closed:
            return
        with self._lock:
            tl_conn = getattr(self._thread_local, "conn", None)
            if tl_conn is not None:
                try:
                    tl_conn.close()
                except Exception:
                    pass
                try:
                    del self._thread_local.conn
                except Exception:
                    pass
            try:
                self._conn.close()
            except Exception:
                pass
            self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query(self, sql: str, *, readonly: bool = False) -> pl.DataFrame:
        """Execute SQL against registered channel views.

        Views available mirror the channel names (e.g. ``trade``, ``quote``, …).

        Args:
            sql: Any DuckDB-compatible SQL query.
            readonly: When True, reject multi-statement and mutating SQL
                (use for MCP / network surfaces).

        Returns:
            A Polars DataFrame with the query result.
        """
        if readonly:
            assert_readonly_sql(sql)
        # Refresh views so newly written files are picked up.
        self._refresh_views()
        conn = self._get_conn()
        result = conn.execute(sql)
        df = result.pl()
        return df

    def scan(
        self,
        channel: str,
        symbol: str,
        start_ns: int,
        end_ns: int,
    ) -> pl.DataFrame:
        """Return rows for a single symbol within a nanosecond time range.

        Partition pruning is applied by narrowing the glob **before** the
        ``WHERE`` clause — only date partitions that overlap ``[start_ns,
        end_ns]`` are discovered, avoiding a full directory scan.

        Args:
            channel: Channel name, e.g. ``"trade"``.
            symbol: Canonical symbol string, e.g. ``"AAPL"``.
            start_ns: Inclusive lower bound on ``local_ts`` (nanoseconds UTC).
            end_ns: Inclusive upper bound on ``local_ts`` (nanoseconds UTC).

        Returns:
            A Polars DataFrame ordered by ``local_ts``, potentially empty if
            no rows match.
        """
        if not _SAFE_CHANNEL_RE.fullmatch(channel):
            raise ValueError(f"Unsafe channel name: {channel!r}")
        # Build narrow glob paths by date.
        glob_paths = self._build_date_globs(channel, start_ns, end_ns, symbol)

        if not glob_paths:
            return pl.DataFrame()

        # Deduplicate.
        unique_globs = list(dict.fromkeys(glob_paths))

        # Build a multi-path read_parquet expression.
        paths_literal = ", ".join(f"'{p.replace(chr(39), chr(39) * 2)}'" for p in unique_globs)

        sql = f"""
            SELECT *
            FROM read_parquet(
                [{paths_literal}],
                hive_partitioning => true,
                union_by_name => true
            )
            WHERE symbol = ?
              AND local_ts >= ?
              AND local_ts <= ?
            ORDER BY local_ts
        """
        conn = self._get_conn()
        result = conn.execute(sql, [symbol, start_ns, end_ns])
        df = result.pl()
        if len(df) == 0:
            return pl.DataFrame()
        return df

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """The underlying DuckDB connection (read-only accessor).

        Returns:
            The :class:`duckdb.DuckDBPyConnection` instance backing this catalog.
        """
        return self._conn

    def refresh_views(self) -> None:
        """Re-scan the data directory and re-register channel views."""
        self._refresh_views(force=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _refresh_views(self, force: bool = False) -> None:
        """Scan data_dir for channel directories and create/replace views."""
        with self._lock:
            channel_dir = self._data_dir
            if not channel_dir.exists():
                return

            # Discover channels from directory names ``channel=<name>``.
            for provider_dir in channel_dir.iterdir():
                if not provider_dir.is_dir() or not provider_dir.name.startswith("provider="):
                    continue
                for chan_dir in provider_dir.iterdir():
                    if not chan_dir.is_dir() or not chan_dir.name.startswith("channel="):
                        continue
                    channel = chan_dir.name[len("channel=") :]
                    if not _SAFE_CHANNEL_RE.fullmatch(channel):
                        log.warning("Skipping unsafe channel directory name: %r", channel)
                        continue
                    if force or channel not in self._registered_channels:
                        self._create_view(channel)

    def _create_view(self, channel: str) -> None:
        """Register a DuckDB VIEW named after the channel.

        The glob covers all providers and all dates for that channel so that
        ``query("SELECT … FROM trade")`` works without extra parameters.
        """
        if not _SAFE_CHANNEL_RE.fullmatch(channel):
            log.warning("Refusing to create view for unsafe channel name: %r", channel)
            return

        patterns: list[str] = []
        # Determine if bucketed or non-bucketed layout is used on disk.
        pattern_bucket = str(
            self._data_dir
            / "provider=*"
            / f"channel={channel}"
            / "date=*"
            / "bucket=*"
            / "part-*.parquet"
        )
        pattern_nobucket = str(
            self._data_dir / "provider=*" / f"channel={channel}" / "date=*" / "part-*.parquet"
        )

        if _glob.glob(pattern_bucket):
            patterns.append(pattern_bucket)
        if _glob.glob(pattern_nobucket):
            patterns.append(pattern_nobucket)

        if not patterns:
            return

        escaped_patterns = [p.replace("'", "''") for p in patterns]
        if len(escaped_patterns) == 1:
            paths_literal = f"'{escaped_patterns[0]}'"
        else:
            paths_literal = ", ".join(f"'{p}'" for p in escaped_patterns)
            paths_literal = f"[{paths_literal}]"

        # Double-quote identifier escape is "" not single-quote doubling
        escaped_channel = channel.replace('"', '""')
        sql = f"""
            CREATE OR REPLACE VIEW "{escaped_channel}" AS
            SELECT * FROM read_parquet(
                {paths_literal},
                hive_partitioning => true,
                union_by_name => true
            )
        """
        try:
            self._conn.execute(sql)
            self._registered_channels.add(channel)
        except Exception as e:
            log.warning("Failed to create view for channel %r: %s", channel, e)

    def _build_date_globs(
        self, channel: str, start_ns: int, end_ns: int, symbol: str | None = None
    ) -> list[str]:
        """Return concrete glob patterns narrowed to dates in [start_ns, end_ns]."""
        channel_dirs = list(self._data_dir.glob(f"provider=*/channel={channel}"))
        if not channel_dirs:
            return []

        # Compute the set of dates covered by [start_ns, end_ns].
        dates = set(_ns_range_to_dates(start_ns, end_ns))

        globs: list[str] = []
        for chan_dir in channel_dirs:
            for date_dir in chan_dir.iterdir():
                if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                    continue
                date_str = date_dir.name[len("date=") :]
                if date_str in dates:
                    # 1. Bucket pattern
                    if symbol is not None:
                        bucket = _symbol_bucket(symbol)
                        pattern_bucket = str(date_dir / f"bucket={bucket}" / "part-*.parquet")
                    else:
                        pattern_bucket = str(date_dir / "bucket=*" / "part-*.parquet")

                    if _glob.glob(pattern_bucket):
                        globs.append(pattern_bucket)
                        continue

                    # 2. No-bucket pattern
                    pattern_nobucket = str(date_dir / "part-*.parquet")
                    if _glob.glob(pattern_nobucket):
                        globs.append(pattern_nobucket)

        return globs


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _ns_to_date(ns: int) -> str:
    """Convert nanosecond UTC epoch to a ``YYYY-MM-DD`` string."""
    dt = datetime.datetime.fromtimestamp(ns // 1_000_000_000, tz=datetime.UTC)
    return dt.strftime("%Y-%m-%d")


def _ns_range_to_dates(start_ns: int, end_ns: int) -> list[str]:
    """Return all UTC date strings that overlap the nanosecond range."""
    start_dt = datetime.datetime.fromtimestamp(start_ns // 1_000_000_000, tz=datetime.UTC).date()
    end_dt = datetime.datetime.fromtimestamp(end_ns // 1_000_000_000, tz=datetime.UTC).date()

    dates: list[str] = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime("%Y-%m-%d"))
        current += datetime.timedelta(days=1)
    return dates
