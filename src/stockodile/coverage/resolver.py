"""Coverage resolver for stockodile.

Deduplicates and merges overlapping market data records from multiple sources.
Supports prioritizing sources and merging both msgspec Record structures and Polars DataFrames.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Literal

import msgspec
import polars as pl

from stockodile.schema.records import Record


class CoverageResolver:
    """Resolver for merging and deduplicating market data coverage across sources."""

    def __init__(
        self,
        priority_list: Sequence[str],
        timestamp_col: str = "local_ts",
        provider_col: str = "provider",
        symbol_col: str = "symbol",
    ) -> None:
        """Initialize the CoverageResolver with source priorities.

        Args:
            priority_list: List of provider/source names ordered by descending priority.
            timestamp_col: Name of the timestamp column or field (defaults to 'local_ts').
            provider_col: Name of the provider/source column or field (defaults to 'provider').
            symbol_col: Name of the symbol column or field (defaults to 'symbol').
        """
        self.priority_list = list(priority_list)
        self.timestamp_col = timestamp_col
        self.provider_col = provider_col
        self.symbol_col = symbol_col

    def get_priority_rank(self, provider: str) -> int:
        """Return the priority rank index of the provider (lower is higher priority)."""
        try:
            return self.priority_list.index(provider)
        except ValueError:
            return len(self.priority_list)

    def resolve_records(
        self,
        records: Sequence[Record],
        strategy: Literal["priority", "fill_nulls"] = "priority",
    ) -> list[Record]:
        """Merge a list of msgspec Record structures.

        Args:
            records: List of msgspec Record structures (e.g. Bar, Trade, Quote).
            strategy: Resolving strategy. 'priority' picks the record from the
                      highest priority provider. 'fill_nulls' starts with the
                      highest priority record and fills None fields using values from
                      lower priority records.

        Returns:
            Deduplicated and merged list of Record structures.
        """
        if strategy not in ("priority", "fill_nulls"):
            raise ValueError(f"Unknown strategy: {strategy!r}")
        if not records:
            return []

        # Group by (symbol, timestamp)
        groups = defaultdict(list)
        for r in records:
            sym = getattr(r, self.symbol_col, None)
            ts = getattr(r, self.timestamp_col, None)
            groups[(sym, ts)].append(r)

        resolved: list[Record] = []
        for group_records in groups.values():
            if not group_records:
                continue

            # Sort the group records by provider priority rank (highest priority first)
            sorted_group = sorted(
                group_records,
                key=lambda r: self.get_priority_rank(getattr(r, self.provider_col, "")),
            )

            if strategy == "priority":
                resolved.append(sorted_group[0])
            elif strategy == "fill_nulls":
                merged = sorted_group[0]
                for other in sorted_group[1:]:
                    fields_to_check = [f.name for f in msgspec.structs.fields(merged)]
                    updates = {}
                    for field in fields_to_check:
                        current_val = getattr(merged, field)
                        if current_val is None:
                            if hasattr(other, field):
                                other_val = getattr(other, field)
                                if other_val is not None:
                                    updates[field] = other_val
                    if updates:
                        merged = msgspec.structs.replace(merged, **updates)
                resolved.append(merged)
            else:
                raise ValueError(f"Unknown strategy: {strategy!r}")

        # Sort the final output by symbol and timestamp safely
        def sort_key(r: Record) -> tuple[str, int, str]:
            sym_val = getattr(r, self.symbol_col, None)
            ts_val = getattr(r, self.timestamp_col, None)
            sym_str = str(sym_val) if sym_val is not None else ""
            if ts_val is None:
                return (sym_str, 1, "")
            if isinstance(ts_val, int):
                # Format to a zero-padded string to allow correct lexicographical sorting
                return (sym_str, 0, f"{ts_val:020d}")
            return (sym_str, 0, str(ts_val))

        resolved.sort(key=sort_key)
        return resolved

    def resolve_df(
        self,
        df: pl.DataFrame,
        strategy: Literal["priority", "fill_nulls"] = "priority",
    ) -> pl.DataFrame:
        """Merge a Polars DataFrame of records.

        Args:
            df: Polars DataFrame of records.
            strategy: Resolving strategy. 'priority' picks the row from the
                      highest priority provider. 'fill_nulls' starts with the
                      highest priority row and fills null values using values from
                      lower priority rows.

        Returns:
            Deduplicated and merged Polars DataFrame.
        """
        if strategy not in ("priority", "fill_nulls"):
            raise ValueError(f"Unknown strategy: {strategy!r}")
        if df.height == 0:
            return df

        # Validate that required columns exist
        for col, name in [
            (self.symbol_col, "symbol"),
            (self.timestamp_col, "timestamp"),
            (self.provider_col, "provider"),
        ]:
            if col not in df.columns:
                raise ValueError(
                    f"Required {name} column '{col}' not found in DataFrame. "
                    f"Available columns: {df.columns}"
                )

        priority_map = {provider: idx for idx, provider in enumerate(self.priority_list)}
        default_rank = len(self.priority_list)

        # Add temporary rank column
        df_with_rank = df.with_columns(
            pl.col(self.provider_col)
            .replace_strict(priority_map, default=default_rank)
            .alias("_priority_rank")
        )

        if strategy == "priority":
            resolved = (
                df_with_rank.sort([self.symbol_col, self.timestamp_col, "_priority_rank"])
                .unique(subset=[self.symbol_col, self.timestamp_col], keep="first")
                .drop("_priority_rank")
            )
        elif strategy == "fill_nulls":
            sorted_df = df_with_rank.sort([self.symbol_col, self.timestamp_col, "_priority_rank"])

            group_keys = [self.symbol_col, self.timestamp_col]
            agg_exprs = []
            for col in df.columns:
                if col in group_keys:
                    continue
                if col == self.provider_col:
                    agg_exprs.append(pl.col(col).first().alias(col))
                else:
                    agg_exprs.append(pl.col(col).drop_nulls().first().alias(col))

            resolved = sorted_df.group_by(group_keys).agg(agg_exprs).select(df.columns)
        else:
            raise ValueError(f"Unknown strategy: {strategy!r}")

        # Ensure output is sorted by symbol and timestamp
        return resolved.sort([self.symbol_col, self.timestamp_col])

    def resolve(
        self,
        data: Sequence[Record] | pl.DataFrame,
        strategy: Literal["priority", "fill_nulls"] = "priority",
    ) -> list[Record] | pl.DataFrame:
        """Resolve coverage across both list of msgspec Records and Polars DataFrames.

        Args:
            data: Either a Sequence of msgspec Record structures, or a Polars DataFrame.
            strategy: Resolving strategy ('priority' or 'fill_nulls').

        Returns:
            Resolved output of the same type as input (list of Records or Polars DataFrame).
        """
        if isinstance(data, pl.DataFrame):
            return self.resolve_df(data, strategy=strategy)
        else:
            return self.resolve_records(data, strategy=strategy)
