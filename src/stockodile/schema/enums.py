"""Enumerations for stockodile schema."""

from enum import StrEnum


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class OptType(StrEnum):
    C = "C"
    P = "P"


class Tape(StrEnum):
    A = "A"
    B = "B"
    C = "C"
    UNKNOWN = "unknown"


class SecurityType(StrEnum):
    CS = "CS"
    ETF = "ETF"
    ADR = "ADR"
    REIT = "REIT"
    PFD = "PFD"
    WARRANT = "WARRANT"
    UNIT = "UNIT"
    RIGHT = "RIGHT"
    UNKNOWN = "unknown"


class CorpActionType(StrEnum):
    SPLIT = "split"
    DIVIDEND_CASH = "dividend_cash"
    DIVIDEND_STOCK = "dividend_stock"
    SPINOFF = "spinoff"
    MERGER = "merger"
    TICKER_CHANGE = "ticker_change"


class FundPeriod(StrEnum):
    Q1 = "Q1"
    Q2 = "Q2"
    Q3 = "Q3"
    Q4 = "Q4"
    FY = "FY"
    TTM = "TTM"


class Channel(StrEnum):
    TRADE = "trade"
    QUOTE = "quote"
    BOOK_SNAPSHOT = "book_snapshot"
    BOOK_DELTA = "book_delta"
    BAR = "bar"
    AUCTION = "auction"
    TRADING_STATUS = "trading_status"
    INSTRUMENT = "instrument"
    CORP_ACTION = "corp_action"
    FUNDAMENTAL = "fundamental"
    INSIDER = "insider"
    HOLDING_13F = "holding_13f"
    SHORT_INTEREST = "short_interest"
    SHORT_VOLUME = "short_volume"
    FILING = "filing"
    OPTION_QUOTE = "option_quote"
    INDEX_VALUE = "index_value"
    MACRO_SERIES = "macro_series"
    OHLCV = "ohlcv"

