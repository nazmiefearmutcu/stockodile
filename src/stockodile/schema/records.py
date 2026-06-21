"""Canonical record schemas for Stockodile."""

from __future__ import annotations

import msgspec

from stockodile.schema.enums import (
    CorpActionType,
    FundPeriod,
    OptType,
    SecurityType,
    Side,
    Tape,
)

Level = tuple[float, float]  # (price, size); size == 0.0 means REMOVE this level


class Trade(msgspec.Struct, frozen=True, tag="trade", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    id: str
    price: float
    size: float
    conditions: list[str] | None = None
    tape: Tape | None = None
    venue: str | None = None


class Quote(msgspec.Struct, frozen=True, tag="quote", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    bid_px: float
    bid_sz: float
    ask_px: float
    ask_sz: float
    is_nbbo: bool = False
    is_consolidated: bool = False
    conditions: list[str] | None = None
    tape: Tape | None = None


class BookSnapshot(msgspec.Struct, frozen=True, tag="book_snapshot", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    bids: list[Level]
    asks: list[Level]
    depth: int
    sequence_id: int | None = None
    is_snapshot: bool = True


class BookDelta(msgspec.Struct, frozen=True, tag="book_delta", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    bids: list[Level]
    asks: list[Level]
    seq_id: int | None = None
    prev_seq_id: int | None = None
    is_snapshot: bool = False


class Auction(msgspec.Struct, frozen=True, tag="auction", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    paired_shares: float | None
    imbalance_shares: float | None
    imbalance_side: Side | None
    reference_price: float | None
    indicative_price: float | None
    auction_type: str | None


class TradingStatus(msgspec.Struct, frozen=True, tag="trading_status", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    status: str
    reason: str | None
    limit_up_price: float | None
    limit_down_price: float | None
    indicator: str | None


class Instrument(msgspec.Struct, frozen=True, tag="instrument", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    name: str | None
    cik: str | None
    figi: str | None
    composite_figi: str | None
    share_class_figi: str | None
    cusip: str | None
    exchange: str | None
    security_type: SecurityType | None
    sic: str | None
    shares_outstanding: int | None
    listing_date: str | None
    status: str | None


class CorporateAction(msgspec.Struct, frozen=True, tag="corp_action", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    ex_date: str  # YYYY-MM-DD
    type: CorpActionType
    value: float


class Bar(msgspec.Struct, frozen=True, tag="bar", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    interval: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    trade_count: int | None = None


class Fundamental(msgspec.Struct, frozen=True, tag="fundamental", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    taxonomy: str
    tag: str
    unit: str
    val: float
    end: str                 # period_end
    start: str | None = None  # for duration facts, None for instant facts
    fy: int | None = None
    fp: FundPeriod | None = None    # e.g., Q1, Q2, Q3, Q4, FY, TTM
    form: str | None = None
    filed: str | None = None
    accn: str | None = None
    frame: str | None = None


class InsiderTransaction(msgspec.Struct, frozen=True, tag="insider", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    insider_name: str
    position: str
    transaction_type: str
    transaction_date: str  # YYYY-MM-DD
    shares: float | None = None
    price: float | None = None
    value: float | None = None
    ownership: str | None = None  # "D" or "I"


class Holding13F(msgspec.Struct, frozen=True, tag="holding_13f", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    manager_name: str
    issuer_name: str
    cusip: str
    value: float
    shares: float
    shares_type: str
    discretion: str | None
    voting_sole: float | None
    voting_shared: float | None
    voting_none: float | None
    report_date: str | None
    accession_number: str | None


class ShortInterest(msgspec.Struct, frozen=True, tag="short_interest", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    settlement_date: str
    short_interest: float
    prev_short_interest: float | None
    days_to_cover: float | None
    change_pct: float | None


class ShortVolume(msgspec.Struct, frozen=True, tag="short_volume", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    date: str
    short_volume: float
    short_exempt_volume: float | None
    total_volume: float


class Filing(msgspec.Struct, frozen=True, tag="filing", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    accession_number: str
    form: str
    filing_date: str
    primary_document: str
    document_url: str
    report_date: str | None
    is_xbrl: bool | None


class OptionQuote(msgspec.Struct, frozen=True, tag="option_quote", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    underlying: str
    expiry: str  # YYYY-MM-DD
    strike: float
    type: OptType
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    volume: float | None = None
    open_interest: float | None = None
    implied_volatility: float | None = None
    delta: float | None = None
    gamma: float | None = None
    vega: float | None = None
    theta: float | None = None
    rho: float | None = None


class OHLCV(msgspec.Struct, frozen=True, tag="ohlcv", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    interval: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    trade_count: int | None = None


class IndexValue(msgspec.Struct, frozen=True, tag="index_value", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    value: float


class MacroSeries(msgspec.Struct, frozen=True, tag="macro_series", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    source_ts: int | None
    local_ts: int
    date: str
    value: float | None
    realtime_start: str | None
    realtime_end: str | None


Record = (
    Trade
    | Quote
    | BookSnapshot
    | BookDelta
    | Bar
    | Auction
    | TradingStatus
    | Instrument
    | CorporateAction
    | Fundamental
    | InsiderTransaction
    | Holding13F
    | ShortInterest
    | ShortVolume
    | Filing
    | OptionQuote
    | IndexValue
    | MacroSeries
    | OHLCV
)
