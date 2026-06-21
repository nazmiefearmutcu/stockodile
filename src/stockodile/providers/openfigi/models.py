"""Models and schemas for OpenFIGI provider."""

import msgspec


class OpenFigiJob(msgspec.Struct, frozen=True):
    """Represents a single OpenFIGI mapping request job."""

    id_type: str  # e.g., "TICKER", "ID_ISIN", "ID_CUSIP"
    id_value: str
    exch_code: str | None = None
    mic_code: str | None = None
    currency: str | None = None
    market_sec_des: str | None = None


class FigiRecord(msgspec.Struct, frozen=True):
    """Represents a standardized FIGI reference record."""

    figi: str
    security_type: str | None = None
    market_sector: str | None = None
    ticker: str | None = None
    name: str | None = None
    exch_code: str | None = None
    share_class_figi: str | None = None
    composite_figi: str | None = None
    security_type_2: str | None = None
    security_description: str | None = None


class OpenFigiResult(msgspec.Struct):
    """Raw result structure returned by OpenFIGI mapping API."""

    figi: str
    securityType: str | None = None
    marketSector: str | None = None
    ticker: str | None = None
    name: str | None = None
    exchCode: str | None = None
    shareClassFIGI: str | None = None
    compositeFIGI: str | None = None
    securityType2: str | None = None
    securityDescription: str | None = None


class OpenFigiResponseItem(msgspec.Struct):
    """Raw response item container returned by OpenFIGI mapping API."""

    data: list[OpenFigiResult] | None = None
    error: str | None = None


def map_raw_to_record(raw: OpenFigiResult) -> FigiRecord:
    """Map raw OpenFIGI response result to standardized FigiRecord."""
    return FigiRecord(
        figi=raw.figi,
        security_type=raw.securityType,
        market_sector=raw.marketSector,
        ticker=raw.ticker,
        name=raw.name,
        exch_code=raw.exchCode,
        share_class_figi=raw.shareClassFIGI,
        composite_figi=raw.compositeFIGI,
        security_type_2=raw.securityType2,
        security_description=raw.securityDescription,
    )
