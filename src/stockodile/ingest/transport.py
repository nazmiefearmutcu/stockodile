"""WS transport Protocol + FakeTransport for deterministic testing.

Also provides ``AiohttpWsTransport`` — a live aiohttp-backed WebSocket
transport.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Transport(Protocol):
    """Minimal interface that every WS transport must satisfy."""

    async def connect(self) -> None: ...

    def __aiter__(self) -> AsyncIterator[bytes]: ...

    async def send(self, data: bytes) -> None: ...

    async def close(self) -> None: ...


class FakeTransport:
    """Yields canned frames then stops — drives providers without network."""

    def __init__(self, frames: list[bytes]) -> None:
        self._frames = frames
        self._connected = False

    async def connect(self) -> None:
        self._connected = True

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[bytes]:
        for frame in self._frames:
            yield frame

    async def send(self, data: bytes) -> None:
        pass  # no-op for tests

    async def close(self) -> None:
        self._connected = False


class AiohttpWsTransport:
    """Live WebSocket transport backed by ``aiohttp``.

    Used to connect to provider WebSocket endpoints.
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._session: Any = None  # aiohttp.ClientSession
        self._ws: Any = None  # aiohttp.ClientWebSocketResponse

    async def connect(self) -> None:
        import aiohttp

        await self.close()
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self._url, heartbeat=20.0)

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[bytes]:
        import aiohttp

        if self._ws is None:
            return
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                yield msg.data.encode()
            elif msg.type == aiohttp.WSMsgType.BINARY:
                yield msg.data
            elif msg.type == aiohttp.WSMsgType.ERROR:
                exc = self._ws.exception()
                if exc is not None:
                    raise exc
                raise ConnectionError("WebSocket connection closed with error")
            elif msg.type == aiohttp.WSMsgType.CLOSE:
                break

    async def send(self, data: bytes) -> None:
        if self._ws is not None:
            try:
                text = data.decode("utf-8")
                await self._ws.send_str(text)
            except UnicodeDecodeError:
                await self._ws.send_bytes(data)

    async def close(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
        self._ws = None
        self._session = None
