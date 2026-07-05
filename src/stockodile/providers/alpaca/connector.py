from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Iterable
from typing import Any

from stockodile.ingest.transport import AiohttpWsTransport, Transport
from stockodile.providers.base import Provider
from stockodile.reference.registry import Instrument, InstrumentRegistry
from stockodile.schema.enums import SecurityType, Tape
from stockodile.schema.records import Bar, Quote, Record, Trade
from stockodile.sink.base import Sink
from stockodile.util.time import rfc3339_to_ns

log = logging.getLogger(__name__)


class FatalProviderError(BaseException):
    """Exception raised for fatal errors that should terminate the provider supervisor."""

    pass


class AlpacaProvider(Provider):
    name = "alpaca"
    ws_url = "wss://stream.data.alpaca.markets/v2/iex"
    rest_url = "https://data.alpaca.markets"

    def __init__(
        self,
        symbols: list[str],
        channels: list[str],
        out: Sink,
        registry: InstrumentRegistry,
        key: str | None = None,
        secret: str | None = None,
        feed: str | None = None,
    ) -> None:
        self.feed = feed or os.environ.get("ALPACA_FEED") or "iex"
        if self.feed not in ("iex", "sip"):
            raise ValueError("Alpaca feed must be 'iex' or 'sip'")

        # Respect symbol limit: Alpaca free tier permits max 30 symbols for trades + quotes
        has_capped_channels = any(ch in ["trade", "quote"] for ch in channels)
        if self.feed == "iex" and has_capped_channels and len(symbols) > 30:
            raise ValueError(
                "Alpaca basic plan WebSocket limits trade/quote subscriptions to 30 symbols."
            )

        super().__init__(symbols, channels, out, registry)
        self.key = key or os.environ.get("ALPACA_API_KEY")
        self.secret = secret or os.environ.get("ALPACA_API_SECRET")
        if not self.key or not self.secret:
            raise ValueError(
                "Alpaca API credentials missing. Set ALPACA_API_KEY and ALPACA_API_SECRET."
            )
        self.ws_url = f"wss://stream.data.alpaca.markets/v2/{self.feed}"
        self.transport = AiohttpWsTransport(self.ws_url)
        self._running = False

    async def list_instruments(self) -> list[Instrument]:
        insts = []
        for sym in self.symbols:
            sec_type = SecurityType.CS
            if ":" in sym or "/" in sym:
                sec_type = SecurityType.UNKNOWN
            elif len(sym) >= 15 and sym[-8:].isdigit() and any(c in sym for c in ("C", "P")):
                sec_type = SecurityType.UNKNOWN
            elif self.registry is not None:
                try:
                    inst = self.registry.get_raw(self.name, sym)
                    if inst is not None:
                        sec_type = inst.security_type
                    elif self.registry.security_master is not None:
                        sec = self.registry.security_master.get_by_symbol(sym)
                        if sec is None:
                            resolved = self.registry.security_master.resolve_ticker(sym)
                            if resolved is not None:
                                sec = self.registry.security_master.get_by_symbol(resolved)
                        if sec is not None and sec.security_type is not None:
                            sec_type = sec.security_type
                except Exception:
                    pass

            insts.append(
                Instrument(
                    symbol=sym,
                    provider=self.name,
                    symbol_raw=sym,
                    security_type=sec_type,
                )
            )
        return insts

    async def _subscribe(self, transport: Transport) -> None:
        # Read the initial greeting message: [{"T":"success","msg":"connected"}]
        iterator = transport.__aiter__()
        try:
            raw = await asyncio.wait_for(iterator.__anext__(), timeout=10.0)
            msg = json.loads(raw)
            if isinstance(msg, list) and len(msg) > 0:
                first = msg[0]
                if not (first.get("T") == "success" and first.get("msg") == "connected"):
                    raise RuntimeError(f"Unexpected greeting from Alpaca: {raw.decode()}")
            else:
                raise RuntimeError(f"Unexpected greeting structure from Alpaca: {raw.decode()}")
        except TimeoutError as err:
            raise RuntimeError("Timeout waiting for Alpaca greeting handshake") from err
        except StopAsyncIteration as err:
            raise RuntimeError("Transport closed during connection handshake") from err

        # Authenticate
        auth_msg = {
            "action": "auth",
            "key": self.key,
            "secret": self.secret,
        }
        await transport.send(json.dumps(auth_msg).encode())

        # Await auth confirmation: [{"T":"success","msg":"authenticated"}]
        try:
            raw = await asyncio.wait_for(iterator.__anext__(), timeout=10.0)
            msg = json.loads(raw)
            if isinstance(msg, list) and len(msg) > 0:
                first = msg[0]
                if first.get("T") == "success" and first.get("msg") == "authenticated":
                    pass
                elif first.get("T") == "error":
                    err_msg = first.get("msg")
                    err_code = first.get("code")
                    if err_code == 406 or any(
                        w in str(err_msg).lower() for w in ("auth", "credential", "key", "secret")
                    ):
                        raise FatalProviderError(
                            f"Alpaca authentication failed: {err_msg} (code {err_code})"
                        )
                    raise RuntimeError(f"Alpaca authentication failed: {err_msg} (code {err_code})")
                else:
                    raise RuntimeError(f"Unexpected auth response: {raw.decode()}")
            else:
                raise RuntimeError(f"Unexpected auth response structure: {raw.decode()}")
        except TimeoutError as err:
            raise RuntimeError("Timeout waiting for Alpaca auth handshake") from err
        except StopAsyncIteration as err:
            raise RuntimeError("Transport closed during auth handshake") from err

        # Subscribe
        sub_msg: dict[str, Any] = {"action": "subscribe"}
        for ch in self.channels:
            if ch == "trade":
                sub_msg["trades"] = self.symbols
            elif ch == "quote":
                sub_msg["quotes"] = self.symbols
            elif ch == "bar":
                sub_msg["bars"] = self.symbols

        await transport.send(json.dumps(sub_msg).encode())

    def normalize(self, msg: object, local_ts: int) -> Iterable[Record]:
        if not isinstance(msg, list):
            return
        for item in msg:
            if not isinstance(item, dict):
                continue
            try:
                t_type = item.get("T")
                if t_type == "error":
                    code = item.get("code")
                    err_msg = item.get("msg")
                    if code == 406 or any(
                        w in str(err_msg).lower() for w in ("auth", "credential", "key", "secret")
                    ):
                        raise FatalProviderError(f"Alpaca WebSocket error: {err_msg} (code {code})")
                    raise ValueError(f"Alpaca WebSocket error: {err_msg} (code {code})")

                elif t_type == "t":
                    tape_val = item.get("z")
                    try:
                        tape = Tape(tape_val) if tape_val else Tape.UNKNOWN
                    except ValueError:
                        tape = Tape.UNKNOWN

                    yield Trade(
                        provider=self.name,
                        symbol=item["S"],
                        symbol_raw=item["S"],
                        source_ts=rfc3339_to_ns(item["t"]),
                        local_ts=local_ts,
                        id=str(item["i"]) if "i" in item else "",
                        price=float(item["p"]),
                        size=float(item["s"]),
                        venue=item.get("x"),
                        conditions=item.get("c"),
                        tape=tape,
                    )
                elif t_type == "q":
                    tape_val = item.get("z")
                    try:
                        tape = Tape(tape_val) if tape_val else Tape.UNKNOWN
                    except ValueError:
                        tape = Tape.UNKNOWN

                    yield Quote(
                        provider=self.name,
                        symbol=item["S"],
                        symbol_raw=item["S"],
                        source_ts=rfc3339_to_ns(item["t"]),
                        local_ts=local_ts,
                        bid_px=float(item["bp"]),
                        bid_sz=float(item["bs"]),
                        ask_px=float(item["ap"]),
                        ask_sz=float(item["as"]),
                        is_nbbo=False,
                        is_consolidated=False,
                        conditions=item.get("c"),
                        tape=tape,
                    )
                elif t_type == "b":
                    yield Bar(
                        provider=self.name,
                        symbol=item["S"],
                        symbol_raw=item["S"],
                        source_ts=rfc3339_to_ns(item["t"]),
                        local_ts=local_ts,
                        interval="1m",
                        open=float(item["o"]),
                        high=float(item["h"]),
                        low=float(item["l"]),
                        close=float(item["c"]),
                        volume=float(item["v"]),
                        vwap=float(item["vw"]) if item.get("vw") is not None else None,
                        trade_count=int(item["n"]) if item.get("n") is not None else None,
                    )
            except (FatalProviderError, ValueError):
                raise
            except Exception as exc:
                log.error("Alpaca normalize item error: %s (item: %s)", exc, item)

    async def run(self, max_reconnects: int = -1) -> None:
        if self._running:
            raise RuntimeError("AlpacaProvider is already running (connection limit: 1).")
        self._running = True
        try:
            if isinstance(self.transport, AiohttpWsTransport):
                self.transport = AiohttpWsTransport(self.ws_url)
            await super().run(max_reconnects)
        finally:
            self._running = False
