"""Models for the reference/security master data."""

from datetime import datetime

import msgspec

from stockodile.schema.enums import SecurityType


class Security(msgspec.Struct, frozen=True):
    """Represents a security in the Security Master database."""

    symbol: str
    ticker: str
    exchange: str
    name: str | None = None
    security_type: SecurityType = SecurityType.UNKNOWN
    cik: str | None = None
    figi: str | None = None
    cusip: str | None = None
    isin: str | None = None
    is_active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None


class TickerMapping(msgspec.Struct, frozen=True):
    """Represents a mapping from a ticker to a standardized symbol."""

    ticker: str
    symbol: str
    exchange: str | None = None
