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

from stockodile.schema.enums import (
    CorpActionType,
    FundPeriod,
    OptType,
    SecurityType,
    Side,
    Tape,
)
from stockodile.schema.records import (
    OHLCV,
    Auction,
    BalanceCorrection,
    Bar,
    BookDelta,
    BookSnapshot,
    CorporateAction,
    Filing,
    Fundamental,
    Holding13F,
    IndexValue,
    InsiderTransaction,
    Instrument,
    LimitOrderFill,
    LiquidationCall,
    MacroSeries,
    OptionQuote,
    PoRUpdate,
    Quote,
    Record,
    ReserveDataUpdated,
    ShortInterest,
    ShortVolume,
    Trade,
    TradingStatus,
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

    if channel == "instrument" and "exchange" in row:
        row["exchange_name"] = row.pop("exchange")

    if channel in ("short_volume", "macro_series") and "date" in row:
        row["date_val"] = row.pop("date")

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
            is_snapshot=bool(d["is_snapshot"]) if d.get("is_snapshot") is not None else True,
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
            is_snapshot=bool(d["is_snapshot"]) if d.get("is_snapshot") is not None else False,
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
            taxonomy=str(d["taxonomy"]) if d.get("taxonomy") is not None else None,  # type: ignore[arg-type]
            tag=str(d["tag"]) if d.get("tag") is not None else None,  # type: ignore[arg-type]
            unit=str(d["unit"]) if d.get("unit") is not None else None,  # type: ignore[arg-type]
            val=float(d["val"]) if d.get("val") is not None else None,  # type: ignore[arg-type]
            end=str(d["end"]) if d.get("end") is not None else None,  # type: ignore[arg-type]
            start=d.get("start"),
            fy=int(d["fy"]) if d.get("fy") is not None else None,
            fp=FundPeriod(d["fp"]) if d.get("fp") else None,
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
    if channel == "auction":
        return Auction(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            paired_shares=(
                float(d["paired_shares"]) if d.get("paired_shares") is not None else None
            ),
            imbalance_shares=(
                float(d["imbalance_shares"]) if d.get("imbalance_shares") is not None else None
            ),
            imbalance_side=Side(d["imbalance_side"]) if d.get("imbalance_side") else None,
            reference_price=(
                float(d["reference_price"]) if d.get("reference_price") is not None else None
            ),
            indicative_price=(
                float(d["indicative_price"]) if d.get("indicative_price") is not None else None
            ),
            auction_type=d.get("auction_type"),
        )
    if channel == "trading_status":
        return TradingStatus(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            status=str(d["status"]),
            reason=d.get("reason"),
            limit_up_price=(
                float(d["limit_up_price"]) if d.get("limit_up_price") is not None else None
            ),
            limit_down_price=(
                float(d["limit_down_price"]) if d.get("limit_down_price") is not None else None
            ),
            indicator=d.get("indicator"),
        )
    if channel == "instrument":
        return Instrument(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            name=d.get("name"),
            cik=d.get("cik"),
            figi=d.get("figi"),
            composite_figi=d.get("composite_figi"),
            share_class_figi=d.get("share_class_figi"),
            cusip=d.get("cusip"),
            exchange=d.get("exchange_name"),
            security_type=SecurityType(d["security_type"]) if d.get("security_type") else None,
            sic=d.get("sic"),
            shares_outstanding=(
                int(d["shares_outstanding"]) if d.get("shares_outstanding") is not None else None
            ),
            listing_date=d.get("listing_date"),
            status=d.get("status"),
        )
    if channel == "insider":
        return InsiderTransaction(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            insider_name=str(d["insider_name"]),
            position=str(d["position"]),
            transaction_type=str(d["transaction_type"]),
            transaction_date=str(d["transaction_date"]),
            shares=float(d["shares"]) if d.get("shares") is not None else None,
            price=float(d["price"]) if d.get("price") is not None else None,
            value=float(d["value"]) if d.get("value") is not None else None,
            ownership=d.get("ownership"),
        )
    if channel == "holding_13f":
        return Holding13F(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            manager_name=str(d["manager_name"]),
            issuer_name=str(d["issuer_name"]),
            cusip=str(d["cusip"]),
            value=float(d["value"]),
            shares=float(d["shares"]),
            shares_type=str(d["shares_type"]),
            discretion=d.get("discretion"),
            voting_sole=float(d["voting_sole"]) if d.get("voting_sole") is not None else None,
            voting_shared=float(d["voting_shared"]) if d.get("voting_shared") is not None else None,
            voting_none=float(d["voting_none"]) if d.get("voting_none") is not None else None,
            report_date=d.get("report_date"),
            accession_number=d.get("accession_number"),
        )
    if channel == "short_interest":
        return ShortInterest(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            settlement_date=str(d["settlement_date"]),
            short_interest=float(d["short_interest"]),
            prev_short_interest=(
                float(d["prev_short_interest"])
                if d.get("prev_short_interest") is not None
                else None
            ),
            days_to_cover=float(d["days_to_cover"]) if d.get("days_to_cover") is not None else None,
            change_pct=float(d["change_pct"]) if d.get("change_pct") is not None else None,
        )
    if channel == "short_volume":
        return ShortVolume(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            date=str(d["date_val"]),
            short_volume=float(d["short_volume"]),
            short_exempt_volume=(
                float(d["short_exempt_volume"])
                if d.get("short_exempt_volume") is not None
                else None
            ),
            total_volume=float(d["total_volume"]),
        )
    if channel == "option_quote":
        return OptionQuote(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            underlying=str(d["underlying"]),
            expiry=str(d["expiry"]),
            strike=float(d["strike"]),
            type=OptType(d["type"]),
            bid=float(d["bid"]) if d.get("bid") is not None else None,
            ask=float(d["ask"]) if d.get("ask") is not None else None,
            last=float(d["last"]) if d.get("last") is not None else None,
            volume=float(d["volume"]) if d.get("volume") is not None else None,
            open_interest=float(d["open_interest"]) if d.get("open_interest") is not None else None,
            implied_volatility=(
                float(d["implied_volatility"]) if d.get("implied_volatility") is not None else None
            ),
            delta=float(d["delta"]) if d.get("delta") is not None else None,
            gamma=float(d["gamma"]) if d.get("gamma") is not None else None,
            vega=float(d["vega"]) if d.get("vega") is not None else None,
            theta=float(d["theta"]) if d.get("theta") is not None else None,
            rho=float(d["rho"]) if d.get("rho") is not None else None,
        )
    if channel == "macro_series":
        return MacroSeries(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            source_ts=d.get("source_ts"),
            local_ts=int(d["local_ts"]),
            date=str(d["date_val"]),
            value=float(d["value"]) if d.get("value") is not None else None,
            realtime_start=d.get("realtime_start"),
            realtime_end=d.get("realtime_end"),
        )
    if channel == "reserve_data_updated":
        return ReserveDataUpdated(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            local_ts=int(d["local_ts"]),
            source_ts=d.get("source_ts"),
            exchange_ts=d.get("exchange_ts"),
            reserve=d.get("reserve"),
            liquidity_rate=(
                float(d["liquidity_rate"]) if d.get("liquidity_rate") is not None else None
            ),
            stable_borrow_rate=(
                float(d["stable_borrow_rate"]) if d.get("stable_borrow_rate") is not None else None
            ),
            variable_borrow_rate=(
                float(d["variable_borrow_rate"])
                if d.get("variable_borrow_rate") is not None
                else None
            ),
            liquidity_index=(
                int(d["liquidity_index"]) if d.get("liquidity_index") is not None else None
            ),
            variable_borrow_index=(
                int(d["variable_borrow_index"])
                if d.get("variable_borrow_index") is not None
                else None
            ),
        )
    if channel == "liquidation_call":
        return LiquidationCall(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            local_ts=int(d["local_ts"]),
            source_ts=d.get("source_ts"),
            exchange_ts=d.get("exchange_ts"),
            collateral_asset=d.get("collateral_asset"),
            debt_asset=d.get("debt_asset"),
            user=d.get("user"),
            debt_to_cover=(
                float(d["debt_to_cover"]) if d.get("debt_to_cover") is not None else None
            ),
            liquidated_collateral_amount=(
                float(d["liquidated_collateral_amount"])
                if d.get("liquidated_collateral_amount") is not None
                else None
            ),
            liquidator=d.get("liquidator"),
            receive_a_token=(
                bool(d["receive_a_token"]) if d.get("receive_a_token") is not None else None
            ),
        )
    if channel == "limit_order_fill":
        return LimitOrderFill(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            local_ts=int(d["local_ts"]),
            source_ts=d.get("source_ts"),
            exchange_ts=d.get("exchange_ts"),
            tx_hash=d.get("tx_hash"),
            log_index=int(d["log_index"]) if d.get("log_index") is not None else None,
            protocol=d.get("protocol"),
            maker=d.get("maker"),
            taker=d.get("taker"),
            maker_token=d.get("maker_token"),
            taker_token=d.get("taker_token"),
            maker_amount=(
                float(d["maker_amount"]) if d.get("maker_amount") is not None else None
            ),
            taker_amount=(
                float(d["taker_amount"]) if d.get("taker_amount") is not None else None
            ),
            order_hash=d.get("order_hash"),
        )
    if channel == "balance_correction":
        return BalanceCorrection(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            exchange_ts=d.get("exchange_ts"),
            local_ts=int(d["local_ts"]),
            holder_address=str(d["holder_address"]),
            token_address=str(d["token_address"]),
            local_balance=float(d["local_balance"]),
            onchain_balance=float(d["onchain_balance"]),
            correction_amount=float(d["correction_amount"]),
            source_ts=d.get("source_ts"),
        )
    if channel == "por_update":
        return PoRUpdate(
            provider=d["provider"],
            symbol=d["symbol"],
            symbol_raw=d["symbol_raw"],
            exchange_ts=int(d["exchange_ts"]),
            local_ts=int(d["local_ts"]),
            feed_address=str(d["feed_address"]),
            token_address=str(d["token_address"]),
            reserves=float(d["reserves"]),
            total_supply=float(d["total_supply"]),
            backing_ratio=float(d["backing_ratio"]),
            is_backed=bool(d["is_backed"]),
            source_ts=d.get("source_ts"),
        )
    raise ValueError(f"Unknown channel tag: {channel!r}")
