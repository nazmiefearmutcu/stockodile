import msgspec

from stockodile.reference.master import SecurityMaster
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
    def __init__(self, security_master: SecurityMaster | None = None) -> None:
        self._by_raw: dict[tuple[str, str], Instrument] = {}
        self._by_symbol: dict[str, Instrument] = {}
        self.security_master = security_master

    def add(self, inst: Instrument) -> None:
        self._by_raw[(inst.provider, inst.symbol_raw)] = inst
        self._by_symbol[inst.symbol] = inst

    def by_raw(self, provider: str, symbol_raw: str) -> Instrument:
        inst = self.get_raw(provider, symbol_raw)
        if inst is None:
            raise KeyError((provider, symbol_raw))
        return inst

    def by_symbol(self, symbol: str) -> Instrument:
        inst = self._by_symbol.get(symbol)
        if inst is not None:
            return inst

        if self.security_master is not None:
            sec = self.security_master.get_by_symbol(symbol)
            if sec is not None:
                inst = Instrument(
                    symbol=sec.symbol,
                    provider="default",
                    symbol_raw=sec.ticker,
                    security_type=sec.security_type,
                    name=sec.name,
                    exchange=sec.exchange,
                    cik=sec.cik,
                    figi=sec.figi,
                    cusip=sec.cusip,
                )
                self._by_symbol[symbol] = inst
                return inst
        raise KeyError(symbol)

    def get_raw(self, provider: str, symbol_raw: str) -> Instrument | None:
        inst = self._by_raw.get((provider, symbol_raw))
        if inst is not None:
            return inst

        if self.security_master is not None:
            symbol = self.security_master.resolve_ticker(symbol_raw)
            if symbol is not None:
                sec = self.security_master.get_by_symbol(symbol)
                if sec is not None:
                    inst = Instrument(
                        symbol=sec.symbol,
                        provider=provider,
                        symbol_raw=symbol_raw,
                        security_type=sec.security_type,
                        name=sec.name,
                        exchange=sec.exchange,
                        cik=sec.cik,
                        figi=sec.figi,
                        cusip=sec.cusip,
                    )
                    self._by_raw[(provider, symbol_raw)] = inst
                    self._by_symbol[sec.symbol] = inst
                    return inst
        return None
