"""Live collect orchestrator.

Runs N providers concurrently using asyncio.TaskGroup, each fully supervised.
SIGINT / CancelledError -> graceful sink.close().
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from stockodile.providers.base import Provider
from stockodile.sink.base import Sink

log = logging.getLogger(__name__)


async def _run_isolated(provider: Provider, max_reconnects: int) -> None:
    """Run a provider, catching and logging any non-cancellation exception."""
    try:
        await provider.run(max_reconnects=max_reconnects)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.error(
            "Provider %r raised an unhandled exception (isolated): %s",
            getattr(provider, "name", repr(provider)),
            exc,
            exc_info=True,
        )


async def collect(
    providers: Sequence[Provider],
    sink: Sink,
    *,
    max_reconnects: int = -1,
) -> None:
    """Run providers concurrently, writing all emitted Records into sink."""
    if not providers:
        await sink.close()
        return

    _cancelled = False
    try:
        async with asyncio.TaskGroup() as tg:
            for provider in providers:
                tg.create_task(_run_isolated(provider, max_reconnects))
    except* asyncio.CancelledError:
        _cancelled = True
    except* Exception as eg:
        for exc in eg.exceptions:
            log.error("Unexpected group-level exception from collect(): %s", exc, exc_info=True)
    finally:
        await sink.close()

    if _cancelled:
        raise asyncio.CancelledError()
