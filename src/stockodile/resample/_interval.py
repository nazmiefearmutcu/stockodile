"""Shared interval-parsing utilities for the resample package."""

from __future__ import annotations

import re

# Map from shorthand suffix to DuckDB INTERVAL unit word.
_UNIT_MAP: dict[str, str] = {
    "s": "second",
    "m": "minute",
    "h": "hour",
    "d": "day",
    "w": "week",
}

_NS_MAP: dict[str, int] = {
    "s": 1_000_000_000,
    "m": 60_000_000_000,
    "h": 3_600_000_000_000,
    "d": 86_400_000_000_000,
    "w": 604_800_000_000_000,
}

_INTERVAL_RE = re.compile(r"^(\d+)([smhdw])$")


def parse_interval(interval: str) -> tuple[int, str, str]:
    """Translate a shorthand interval string to safe SQL components and nanoseconds.

    Args:
        interval: Short-hand interval string (e.g. ``"1s"``, ``"5m"``).

    Returns:
        A 3-tuple ``(interval_ns, interval_sql, polars_str)`` where
        ``interval_ns`` is the interval duration in nanoseconds,
        ``interval_sql`` is a safe DuckDB ``INTERVAL '...'`` literal, and
        ``polars_str`` is a Polars-compatible interval string.

    Raises:
        ValueError: If the interval string cannot be parsed.
    """
    m = _INTERVAL_RE.match(interval.strip().lower())
    if m is None:
        raise ValueError(
            f"Cannot parse interval {interval!r}. "
            f"Expected a number followed by s/m/h/d/w (e.g. '1s', '5m', '1h')."
        )
    qty_str: str = m.group(1)
    qty: int = int(qty_str)
    unit_char: str = m.group(2)

    ns = qty * _NS_MAP[unit_char]
    duckdb_unit = _UNIT_MAP[unit_char]
    interval_sql = f"INTERVAL '{qty_str} {duckdb_unit}'"
    polars_str = f"{qty_str}{unit_char}"

    return ns, interval_sql, polars_str
