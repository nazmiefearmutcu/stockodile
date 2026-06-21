"""Convert canonical Records to flat dicts suitable for Polars/Parquet writing.

Each row gets four extra partition columns:
    channel : str           — discriminator tag (e.g. "trade", "book_snapshot")
    date    : str           — UTC date "YYYY-MM-DD" derived from local_ts
    bucket  : int           — hash(symbol) % 128, avoids per-symbol directory explosion
    exchange: str           — provider mapped to exchange for hive partition compatibility

``from_row`` is the inverse: reconstruct a Record from a Parquet-read flat dict.
"""

from __future__ import annotations

import datetime
import enum
from typing import Any

import mmh3
import msgspec.structs

from stockodile.schema.enums import CorpActionType, Tape
from stockodile.schema.records import (
    OHLCV,
    Bar,
    BookDelta,
    BookSnapshot,
    CorporateAction,
    Filing,
    Fundamental,
    IndexValue,
    Quote,
    Record,
    Trade,
)


def _symbol_bucket(symbol: str) -> int:
    """Stable MurmurHash3 bucket for a canonical symbol string.

    Uses MurmurHash3 (unsigned) over the UTF-8 bytes of symbol mod 128.
    This gives uniform distribution across [0, 127].
    """
    return mmh3.hash(symbol.encode("utf-8"), signed=False) % 128


def _date_from_ns(local_ts: int) -> str:
    """Return UTC date string "YYYY-MM-DD" from a nanosecond epoch integer."""
    seconds = local_ts // 1_000_000_000
    dt = datetime.datetime.fromtimestamp(seconds, tz=datetime.UTC)
    return dt.strftime("%Y-%m-%d")


def _convert_value(v: Any) -> Any:
    """Coerce enum values to their primitive form."""
    if isinstance(v, enum.Enum):
        return v.value
    return v


def to_row(record: Record) -> dict[str, Any]:
    """Flatten a Record Struct into a dict ready for Polars / Parquet.

    Added partition columns:
        - ``channel`` : the msgspec tag string (e.g. "trade")
        - ``date``    : UTC date from ``local_ts`` (e.g. "2023-11-14")
        - ``bucket``  : hash(symbol) % 128
        - ``exchange``: provider mapped to exchange for partition path

    Enum fields (``tape``) are converted to their string values.
    List-of-tuple fields (``bids``, ``asks``) are preserved as Python
    ``list[tuple[float, float]]`` — Polars can infer these as list[struct].
    """
    # Extract channel tag from the struct class metadata
    channel: str = type(record).__struct_config__.tag  # type: ignore[assignment]

    # Build the base dict from struct fields
    raw = msgspec.structs.asdict(record)

    # Coerce enum values to primitives
    row: dict[str, Any] = {k: _convert_value(v) for k, v in raw.items()}

    # Add partition columns
    row["channel"] = channel
    row["date"] = _date_from_ns(record.local_ts)
    row["bucket"] = _symbol_bucket(record.symbol)
    row["exchange"] = record.provider

    return row


# Partition-only columns added by to_row / hive layout — not Record fields.
_PARTITION_COLS = frozenset({"channel", "date", "bucket", "exchange"})


def _coerce_levels_from_row(raw: Any) -> list[tuple[float, float]]:
    """Convert list-of-dicts or list-of-tuples book levels to list[tuple[float, float]]."""
    if not raw:
        return []
    result: list[tuple[float, float]] = []
    for item in raw:
        if isinstance(item, dict):
            # Supports both size/amount in dict representation
            size_val = item.get("size") if "size" in item else item.get("amount")
            result.append((float(item["price"]), float(size_val if size_val is not None else 0.0)))
        else:
            result.append((float(item[0]), float(item[1])))
    return result


def from_row(row: dict[str, Any]) -> Record:
    """Reconstruct a canonical Record from a flat dict (e.g., read from Parquet)."""
    channel = row["channel"]
    d: dict[str, Any] = {k: v for k, v in row.items() if k not in _PARTITION_COLS}

    if channel == "trade":
        return Trade(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            id=str(d["id"]),
            price=float(d["price"]),
            size=float(d["size"]),
            conditions=d.get("conditions"),
            tape=Tape(d["tape"]) if d.get("tape") else None,
            venue=d.get("venue"),
        )
    if channel == "quote":
        return Quote(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            bid_px=float(d["bid_px"]),
            bid_sz=float(d["bid_sz"]),
            ask_px=float(d["ask_px"]),
            ask_sz=float(d["ask_sz"]),
            is_nbbo=bool(d.get("is_nbbo", False)),
            is_consolidated=bool(d.get("is_consolidated", False)),
            conditions=d.get("conditions"),
            tape=Tape(d["tape"]) if d.get("tape") else None,
        )
    if channel == "book_snapshot":
        return BookSnapshot(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            bids=_coerce_levels_from_row(d.get("bids", [])),
            asks=_coerce_levels_from_row(d.get("asks", [])),
            depth=int(d["depth"]),
            sequence_id=d.get("sequence_id"),
            is_snapshot=bool(d.get("is_snapshot", True)),
        )
    if channel == "book_delta":
        return BookDelta(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            bids=_coerce_levels_from_row(d.get("bids", [])),
            asks=_coerce_levels_from_row(d.get("asks", [])),
            seq_id=d.get("seq_id"),
            prev_seq_id=d.get("prev_seq_id"),
            is_snapshot=bool(d.get("is_snapshot", False)),
        )
    if channel == "corp_action":
        return CorporateAction(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            ex_date=str(d["ex_date"]),
            type=CorpActionType(d["type"]),
            value=float(d["value"]),
        )
    if channel == "bar":
        return Bar(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            interval=str(d["interval"]),
            open=float(d["open"]),
            high=float(d["high"]),
            low=float(d["low"]),
            close=float(d["close"]),
            volume=float(d["volume"]),
            vwap=d.get("vwap"),
            trade_count=d.get("trade_count"),
        )
    if channel == "fundamental":
        return Fundamental(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            taxonomy=str(d["taxonomy"]),
            tag=str(d["tag"]),
            unit=str(d["unit"]),
            val=float(d["val"]),
            end=str(d["end"]),
            start=d.get("start"),
            fy=int(d["fy"]) if d.get("fy") is not None else None,
            fp=d.get("fp"),
            form=d.get("form"),
            filed=d.get("filed"),
            accn=d.get("accn"),
            frame=d.get("frame"),
        )
    if channel == "filing":
        return Filing(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            accession_number=d["accession_number"],
            form=d["form"],
            filing_date=d["filing_date"],
            primary_document=d["primary_document"],
            document_url=d["document_url"],
            report_date=d.get("report_date"),
            is_xbrl=bool(d["is_xbrl"]) if d.get("is_xbrl") is not None else None,
        )
    if channel == "ohlcv":
        return OHLCV(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            interval=str(d["interval"]),
            open=float(d["open"]),
            high=float(d["high"]),
            low=float(d["low"]),
            close=float(d["close"]),
            volume=float(d["volume"]),
            vwap=d.get("vwap"),
            trade_count=d.get("trade_count"),
        )
    if channel == "index_value":
        return IndexValue(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            value=float(d["value"]),
        )
    raise ValueError(f"Unknown channel tag: {channel!r}")
