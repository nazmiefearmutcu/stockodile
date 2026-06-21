"""StockodileClient - high-level API wrapping the DuckDB Catalog.

StockodileClient(data_dir) is the primary entry-point for users who want to
query and scan the Parquet data lake without interacting with the lower-level
Catalog directly.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import polars as pl

from stockodile.client.export import ExportFmt
from stockodile.client.export import export as _export
from stockodile.replay.merge import replay as _kway_merge
from stockodile.schema.records import Record
from stockodile.store.catalog import Catalog
from stockodile.store.rows import from_row


def _df_to_record_iter(df: pl.DataFrame) -> Iterator[Record]:
    """Yield Records from a Polars DataFrame, one row at a time.

    The DataFrame must contain a ``channel`` column so that ``from_row``
    can reconstruct the correct Record type.
    """
    for row_dict in df.to_dicts():
        yield from_row(row_dict)


class StockodileClient:
    """High-level data client wrapping the hive-partitioned Parquet catalog.

    Args:
        data_dir: Root directory of the data lake.
    """

    def __init__(self, data_dir: Path | str) -> None:
        self._catalog = Catalog(data_dir)

    def query(self, sql: str) -> pl.DataFrame:
        """Execute arbitrary DuckDB SQL against registered channel views."""
        return self._catalog.query(sql)

    def scan(
        self,
        channel: str,
        symbols: list[str],
        start_ns: int,
        end_ns: int,
    ) -> pl.DataFrame:
        """Return rows for one or more symbols within a nanosecond time range."""
        if not symbols:
            return pl.DataFrame()

        frames: list[pl.DataFrame] = []
        for symbol in symbols:
            df = self._catalog.scan(channel, symbol, start_ns, end_ns)
            if len(df) > 0:
                frames.append(df)

        if not frames:
            return pl.DataFrame()

        if len(frames) == 1:
            return frames[0]

        # Concatenate across symbols and re-sort globally by local_ts.
        combined = pl.concat(frames, how="diagonal")
        return combined.sort("local_ts")

    def replay(
        self,
        channels: list[str],
        symbols: list[str],
        frm: int,
        to: int,
    ) -> Iterator[Record]:
        """Iterate over canonical Records sorted by ``local_ts`` (k-way merge)."""
        if not symbols or not channels:
            return iter([])

        streams: list[Iterator[Record]] = []
        for channel in channels:
            for symbol in symbols:
                df = self._catalog.scan(channel, symbol, frm, to)
                if len(df) > 0:
                    streams.append(_df_to_record_iter(df))

        if not streams:
            return iter([])

        return _kway_merge(streams)

    def export(
        self,
        channel: str,
        symbols: list[str],
        frm: int,
        to: int,
        fmt: ExportFmt,
        dest: Path | str,
    ) -> None:
        """Write rows for a channel x symbols x time range to a file."""
        _export(self._catalog, channel, symbols, frm, to, fmt, Path(dest))

    def resample(
        self,
        symbol: str,
        start_ns: int,
        end_ns: int,
        interval: str,
        *,
        fill_empty: bool = False,
    ) -> pl.DataFrame:
        """Resample trade data in the DuckDB Catalog into OHLCV bars."""
        from stockodile.resample.ohlcv import resample_ohlcv

        return resample_ohlcv(
            self._catalog,
            symbol,
            start_ns,
            end_ns,
            interval,
            fill_empty=fill_empty,
        )
