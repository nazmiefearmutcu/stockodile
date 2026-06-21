import msgspec

from stockodile.schema.enums import SecurityType


class Instrument(msgspec.Struct, frozen=True):
    symbol: str  # canonical symbol, e.g., "AAPL"
    provider: str
    symbol_raw: str
    security_type: SecurityType
    name: str | None = None
    exchange: str | None = None
    cik: str | None = None
    figi: str | None = None
    cusip: str | None = None


class InstrumentRegistry:
    def __init__(self) -> None:
        self._by_raw: dict[tuple[str, str], Instrument] = {}
        self._by_symbol: dict[str, Instrument] = {}

    def add(self, inst: Instrument) -> None:
        self._by_raw[(inst.provider, inst.symbol_raw)] = inst
        self._by_symbol[inst.symbol] = inst

    def by_raw(self, provider: str, symbol_raw: str) -> Instrument:
        return self._by_raw[(provider, symbol_raw)]

    def by_symbol(self, symbol: str) -> Instrument:
        return self._by_symbol[symbol]

    def get_raw(self, provider: str, symbol_raw: str) -> Instrument | None:
        return self._by_raw.get((provider, symbol_raw))
