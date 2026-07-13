"""Canonical record schemas for Stockodile.

Timestamp convention
--------------------
All ``local_ts``, ``source_ts``, and ``exchange_ts`` fields are **UTC epoch
nanoseconds** (``int``). Use ``int(time.time_ns())`` / provider ``*_to_ns``
helpers. Do not store seconds or milliseconds without converting.
"""

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

# Epoch nanoseconds UTC (canonical Stockodile time unit)
TsNs = int

Level = tuple[float, float]  # (price, size); size == 0.0 means REMOVE this level


class Trade(msgspec.Struct, frozen=True, tag="trade", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: TsNs
    id: str
    price: float
    size: float
    source_ts: TsNs | None = None
    conditions: list[str] | None = None
    tape: Tape | None = None
    venue: str | None = None


class Quote(msgspec.Struct, frozen=True, tag="quote", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    bid_px: float
    bid_sz: float
    ask_px: float
    ask_sz: float
    source_ts: int | None = None
    is_nbbo: bool = False
    is_consolidated: bool = False
    conditions: list[str] | None = None
    tape: Tape | None = None


class BookSnapshot(msgspec.Struct, frozen=True, tag="book_snapshot", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    bids: list[Level]
    asks: list[Level]
    depth: int
    source_ts: int | None = None
    sequence_id: int | None = None
    is_snapshot: bool = True


class BookDelta(msgspec.Struct, frozen=True, tag="book_delta", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    bids: list[Level]
    asks: list[Level]
    source_ts: int | None = None
    seq_id: int | None = None
    prev_seq_id: int | None = None
    is_snapshot: bool = False


class Auction(msgspec.Struct, frozen=True, tag="auction", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    source_ts: int | None = None
    paired_shares: float | None = None
    imbalance_shares: float | None = None
    imbalance_side: Side | None = None
    reference_price: float | None = None
    indicative_price: float | None = None
    auction_type: str | None = None


class TradingStatus(msgspec.Struct, frozen=True, tag="trading_status", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    status: str
    source_ts: int | None = None
    reason: str | None = None
    limit_up_price: float | None = None
    limit_down_price: float | None = None
    indicator: str | None = None


class Instrument(msgspec.Struct, frozen=True, tag="instrument", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    source_ts: int | None = None
    name: str | None = None
    cik: str | None = None
    figi: str | None = None
    composite_figi: str | None = None
    share_class_figi: str | None = None
    cusip: str | None = None
    exchange: str | None = None
    security_type: SecurityType | None = None
    sic: str | None = None
    shares_outstanding: int | None = None
    listing_date: str | None = None
    status: str | None = None


class CorporateAction(msgspec.Struct, frozen=True, tag="corp_action", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    ex_date: str  # YYYY-MM-DD
    type: CorpActionType
    value: float
    source_ts: int | None = None


class Bar(msgspec.Struct, frozen=True, tag="bar", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    interval: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_ts: int | None = None
    vwap: float | None = None
    trade_count: int | None = None


class Fundamental(msgspec.Struct, frozen=True, tag="fundamental", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    taxonomy: str
    tag: str
    unit: str
    val: float
    end: str  # period_end
    source_ts: int | None = None
    start: str | None = None  # for duration facts, None for instant facts
    fy: int | None = None
    fp: FundPeriod | None = None  # e.g., Q1, Q2, Q3, Q4, FY, TTM
    form: str | None = None
    filed: str | None = None
    accn: str | None = None
    frame: str | None = None


class InsiderTransaction(msgspec.Struct, frozen=True, tag="insider", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    insider_name: str
    position: str
    transaction_type: str
    transaction_date: str  # YYYY-MM-DD
    source_ts: int | None = None
    shares: float | None = None
    price: float | None = None
    value: float | None = None
    ownership: str | None = None  # "D" or "I"


class Holding13F(msgspec.Struct, frozen=True, tag="holding_13f", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    manager_name: str
    issuer_name: str
    cusip: str
    value: float
    shares: float
    shares_type: str
    source_ts: int | None = None
    discretion: str | None = None
    voting_sole: float | None = None
    voting_shared: float | None = None
    voting_none: float | None = None
    report_date: str | None = None
    accession_number: str | None = None


class ShortInterest(msgspec.Struct, frozen=True, tag="short_interest", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    settlement_date: str
    short_interest: float
    source_ts: int | None = None
    prev_short_interest: float | None = None
    days_to_cover: float | None = None
    change_pct: float | None = None


class ShortVolume(msgspec.Struct, frozen=True, tag="short_volume", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    date: str
    short_volume: float
    total_volume: float
    source_ts: int | None = None
    short_exempt_volume: float | None = None


class Filing(msgspec.Struct, frozen=True, tag="filing", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    accession_number: str
    form: str
    filing_date: str
    primary_document: str
    document_url: str
    source_ts: int | None = None
    report_date: str | None = None
    is_xbrl: bool | None = None


class OptionQuote(msgspec.Struct, frozen=True, tag="option_quote", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    underlying: str
    expiry: str  # YYYY-MM-DD
    strike: float
    type: OptType
    source_ts: int | None = None
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
    local_ts: int
    interval: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_ts: int | None = None
    vwap: float | None = None
    trade_count: int | None = None


class IndexValue(msgspec.Struct, frozen=True, tag="index_value", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    value: float
    source_ts: int | None = None


class MacroSeries(msgspec.Struct, frozen=True, tag="macro_series", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    date: str
    source_ts: int | None = None
    value: float | None = None
    realtime_start: str | None = None
    realtime_end: str | None = None


class ReserveDataUpdated(
    msgspec.Struct, frozen=True, tag="reserve_data_updated", tag_field="channel"
):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    source_ts: int | None = None
    exchange_ts: int | None = None
    reserve: str | None = None
    liquidity_rate: float | None = None
    stable_borrow_rate: float | None = None
    variable_borrow_rate: float | None = None
    liquidity_index: int | None = None
    variable_borrow_index: int | None = None


class LiquidationCall(
    msgspec.Struct, frozen=True, tag="liquidation_call", tag_field="channel"
):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    source_ts: int | None = None
    exchange_ts: int | None = None
    collateral_asset: str | None = None
    debt_asset: str | None = None
    user: str | None = None
    debt_to_cover: float | None = None
    liquidated_collateral_amount: float | None = None
    liquidator: str | None = None
    receive_a_token: bool | None = None


class LimitOrderFill(
    msgspec.Struct, frozen=True, tag="limit_order_fill", tag_field="channel"
):
    provider: str
    symbol: str
    symbol_raw: str
    local_ts: int
    source_ts: int | None = None
    exchange_ts: int | None = None
    tx_hash: str | None = None
    log_index: int | None = None
    protocol: str | None = None
    maker: str | None = None
    taker: str | None = None
    maker_token: str | None = None
    taker_token: str | None = None
    maker_amount: float | None = None
    taker_amount: float | None = None
    order_hash: str | None = None


class BalanceCorrection(
    msgspec.Struct, frozen=True, tag="balance_correction", tag_field="channel"
):
    provider: str
    symbol: str
    symbol_raw: str
    exchange_ts: int | None
    local_ts: int
    holder_address: str
    token_address: str
    local_balance: float
    onchain_balance: float
    correction_amount: float
    source_ts: int | None = None


class PoRUpdate(msgspec.Struct, frozen=True, tag="por_update", tag_field="channel"):
    provider: str
    symbol: str
    symbol_raw: str
    exchange_ts: int
    local_ts: int
    feed_address: str
    token_address: str
    reserves: float
    total_supply: float
    backing_ratio: float
    is_backed: bool
    source_ts: int | None = None


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
    | LimitOrderFill
    | ReserveDataUpdated
    | LiquidationCall
    | BalanceCorrection
    | PoRUpdate
)

# Compatibility alias for onchain normalized records
BookTicker = Quote
