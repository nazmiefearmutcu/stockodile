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
from pathlib import Path

import duckdb
import polars as pl


class Catalog:
    """Query interface over a hive-partitioned Parquet data lake.

    Args:
        data_dir: Root of the data lake.
    """

    def __init__(self, data_dir: Path | str) -> None:
        self._data_dir = Path(data_dir)
        # In-memory DuckDB connection.
        self._conn = duckdb.connect()
        self._registered_channels: set[str] = set()
        # Register views for all channels present on disk.
        self._refresh_views()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pl.DataFrame:
        """Execute arbitrary SQL against registered channel views.

        Views available mirror the channel names (e.g. ``trade``, ``quote``, …).

        Args:
            sql: Any DuckDB-compatible SQL query.

        Returns:
            A Polars DataFrame with the query result.
        """
        # Refresh views so newly written files are picked up.
        self._refresh_views()
        result = self._conn.execute(sql)
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
        # Build narrow glob paths by date.
        glob_paths = self._build_date_globs(channel, start_ns, end_ns)

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
        result = self._conn.execute(sql, [symbol, start_ns, end_ns])
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
        self._refresh_views()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _refresh_views(self) -> None:
        """Scan data_dir for channel directories and create/replace views."""
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
                channel = chan_dir.name[len("channel="):]
                if channel not in self._registered_channels:
                    self._create_view(channel)

    def _create_view(self, channel: str) -> None:
        """Register a DuckDB VIEW named after the channel.

        The glob covers all providers and all dates for that channel so that
        ``query("SELECT … FROM trade")`` works without extra parameters.
        """
        # Determine if bucketed or non-bucketed layout is used on disk.
        channel_dirs = list(self._data_dir.glob(f"provider=*/channel={channel}"))
        has_buckets = False
        for chan_dir in channel_dirs:
            if list(chan_dir.glob("date=*/bucket=*")):
                has_buckets = True
                break

        if has_buckets:
            glob_pattern = str(
                self._data_dir
                / "provider=*"
                / f"channel={channel}"
                / "date=*"
                / "bucket=*"
                / "part-*.parquet"
            )
        else:
            glob_pattern = str(
                self._data_dir
                / "provider=*"
                / f"channel={channel}"
                / "date=*"
                / "part-*.parquet"
            )

        escaped_glob = glob_pattern.replace("'", "''")
        escaped_channel = channel.replace("'", "''")
        sql = f"""
            CREATE OR REPLACE VIEW "{escaped_channel}" AS
            SELECT * FROM read_parquet(
                '{escaped_glob}',
                hive_partitioning => true,
                union_by_name => true
            )
        """
        self._conn.execute(sql)
        self._registered_channels.add(channel)

    def _build_date_globs(
        self, channel: str, start_ns: int, end_ns: int
    ) -> list[str]:
        """Return concrete glob patterns narrowed to dates in [start_ns, end_ns]."""
        channel_dirs = list(self._data_dir.glob(f"provider=*/channel={channel}"))
        if not channel_dirs:
            return []

        # Compute the set of dates covered by [start_ns, end_ns].
        dates = _ns_range_to_dates(start_ns, end_ns)

        globs: list[str] = []
        for chan_dir in channel_dirs:
            for date_str in dates:
                date_dir = chan_dir / f"date={date_str}"
                if date_dir.exists():
                    # 1. Bucket pattern
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
