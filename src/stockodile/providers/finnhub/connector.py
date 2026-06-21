from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable

from stockodile.ingest.transport import AiohttpWsTransport, Transport
from stockodile.providers.base import Provider
from stockodile.reference.registry import Instrument, InstrumentRegistry
from stockodile.schema.enums import SecurityType
from stockodile.schema.records import Record, Trade
from stockodile.sink.base import Sink
from stockodile.util.time import ms_to_ns

log = logging.getLogger(__name__)


class FinnhubProvider(Provider):
    name = "finnhub"
    rest_url = "https://finnhub.io/api/v1"

    def __init__(
        self,
        symbols: list[str],
        channels: list[str],
        out: Sink,
        registry: InstrumentRegistry,
        token: str | None = None,
    ) -> None:
        if len(symbols) > 50:
            raise ValueError(
                "Finnhub free tier WebSocket limits subscriptions to 50 symbols."
            )

        super().__init__(symbols, channels, out, registry)
        self.token = token or os.environ.get("FINNHUB_API_KEY")
        if not self.token:
            raise ValueError("Finnhub API key missing. Set FINNHUB_API_KEY.")
        self.ws_url = f"wss://ws.finnhub.io?token={self.token}"
        self.transport = AiohttpWsTransport(self.ws_url)
        self._running = False

    async def list_instruments(self) -> list[Instrument]:
        insts = []
        for sym in self.symbols:
            insts.append(
                Instrument(
                    symbol=sym,
                    provider=self.name,
                    symbol_raw=sym,
                    security_type=SecurityType.CS,
                )
            )
        return insts

    async def _subscribe(self, transport: Transport) -> None:
        unsupported_channels = [ch for ch in self.channels if ch != "trade"]
        if unsupported_channels:
            log.warning(
                "Finnhub free WebSocket only supports 'trade' channel. Ignoring channels: %s",
                unsupported_channels,
            )

        if "trade" in self.channels:
            for sym in self.symbols:
                sub_msg = {"type": "subscribe", "symbol": sym}
                await transport.send(json.dumps(sub_msg).encode())

    def normalize(self, msg: object, local_ts: int) -> Iterable[Record]:
        if not isinstance(msg, dict):
            return
        msg_type = msg.get("type")
        if msg_type == "trade":
            data = msg.get("data")
            if not isinstance(data, list):
                return
            for item in data:
                if not isinstance(item, dict):
                    continue
                source_ts = ms_to_ns(item["t"]) if "t" in item else None
                yield Trade(
                    provider=self.name,
                    symbol=item["s"],
                    symbol_raw=item["s"],
                    source_ts=source_ts,
                    local_ts=local_ts,
                    id="",
                    price=float(item["p"]),
                    size=float(item["v"]),
                    venue=None,
                    conditions=item.get("c"),
                    tape=None,
                )

    async def run(self, max_reconnects: int = -1) -> None:
        if self._running:
            raise RuntimeError("FinnhubProvider is already running (connection limit: 1).")
        self._running = True
        try:
            if isinstance(self.transport, AiohttpWsTransport):
                self.transport = AiohttpWsTransport(self.ws_url)
            await super().run(max_reconnects)
        finally:
            self._running = False
