from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from stockodile.providers.msn_money.connector import MsnMoneyProvider, get_spoofed_headers
from stockodile.reference.registry import InstrumentRegistry
from stockodile.schema.enums import CorpActionType, SecurityType
from stockodile.schema.records import Bar, CorporateAction
from stockodile.sink.base import MemorySink


def test_msn_money_helpers() -> None:
    headers = get_spoofed_headers()
    assert "User-Agent" in headers
    assert "Accept" in headers
    assert "Cookie" in headers
    assert "Referer" in headers


@pytest.mark.asyncio
async def test_msn_money_list_instruments() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL", "^SPX"],
        channels=["bar"],
        out=sink,
        registry=registry,
    )

    insts = await provider.list_instruments()
    assert len(insts) == 2
    assert insts[0].symbol == "AAPL"
    assert insts[0].security_type == SecurityType.CS
    assert insts[1].symbol == "^SPX"
    assert insts[1].security_type == SecurityType.UNKNOWN


@pytest.mark.asyncio
async def test_msn_money_no_ops() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
    )

    await provider._subscribe(None)
    assert list(provider.normalize({}, 0)) == []


@pytest.mark.asyncio
async def test_msn_money_resolve_sec_id() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
    )

    mock_suggest_resp = MagicMock()
    mock_suggest_resp.status = 200
    mock_suggest_resp.json = AsyncMock(
        return_value={
            "data": {
                "stocks": [
                    '{"RT00S": "AAPL", "SecId": "12345"}',
                    '{"RT00S": "MSFT", "SecId": "67890"}',
                ]
            }
        }
    )

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_suggest_resp)
    mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)

    provider.session = mock_session

    sec_id = await provider._resolve_sec_id("AAPL")
    assert sec_id == "12345"


@pytest.mark.asyncio
async def test_msn_money_backfill_bar() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
    )

    # Mock suggests response
    mock_suggest_resp = MagicMock()
    mock_suggest_resp.status = 200
    mock_suggest_resp.json = AsyncMock(
        return_value={
            "data": {
                "stocks": [
                    '{"RT00S": "AAPL", "SecId": "12345"}',
                ]
            }
        }
    )

    # Mock chart response
    mock_chart_resp = MagicMock()
    mock_chart_resp.status = 200
    mock_chart_resp.json = AsyncMock(
        return_value=[
            {
                "series": {
                    "openPrices": [150.0],
                    "prices": [151.5],
                    "pricesHigh": [152.0],
                    "pricesLow": [149.5],
                    "volumes": [1000000.0],
                    "timeStamps": ["2026-06-20T12:00:00Z"],
                }
            }
        ]
    )

    mock_session = MagicMock()

    # Handle multiple get calls
    def mock_get(url: str, *args: Any, **kwargs: Any) -> MagicMock:
        resp = MagicMock()
        if "Query" in url:
            resp.status = 200
            resp.json = AsyncMock(
                return_value={
                    "data": {
                        "stocks": [
                            '{"RT00S": "AAPL", "SecId": "12345"}',
                        ]
                    }
                }
            )
        elif "Charts" in url:
            resp.status = 200
            resp.json = AsyncMock(
                return_value=[
                    {
                        "series": {
                            "openPrices": [150.0],
                            "prices": [151.5],
                            "pricesHigh": [152.0],
                            "pricesLow": [149.5],
                            "volumes": [1000000.0],
                            "timeStamps": ["2026-06-20T12:00:00Z"],
                        }
                    }
                ]
            )
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=None)
        return resp

    mock_session.get.side_effect = mock_get
    provider.session = mock_session

    start_ns = 1781913600000000000  # 2026-06-20T00:00:00Z
    end_ns = 1782000000000000000  # 2026-06-21T00:00:00Z

    bars = []
    async for rec in provider.backfill("bar", "AAPL", start_ns, end_ns):
        bars.append(rec)

    assert len(bars) == 1
    bar = bars[0]
    assert isinstance(bar, Bar)
    assert bar.symbol == "AAPL"
    assert bar.open == 150.0
    assert bar.high == 152.0
    assert bar.low == 149.5
    assert bar.close == 151.5
    assert bar.volume == 1000000.0


@pytest.mark.asyncio
async def test_msn_money_backfill_corporate_action() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["corp_actions"],
        out=sink,
        registry=registry,
    )

    mock_session = MagicMock()

    def mock_get(url: str, *args: Any, **kwargs: Any) -> MagicMock:
        resp = MagicMock()
        if "Query" in url:
            resp.status = 200
            resp.json = AsyncMock(
                return_value={
                    "data": {
                        "stocks": [
                            '{"RT00S": "AAPL", "SecId": "12345"}',
                        ]
                    }
                }
            )
        elif "QuoteSummary" in url:
            resp.status = 200
            resp.json = AsyncMock(
                return_value=[
                    {
                        "equity": {
                            "shareStatistics": {
                                "exDividendAmount": 0.25,
                                "exDividendDate": "2026-06-20T00:00:00Z",
                                "lastSplitFactor": "2:1",
                                "lastSplitDate": "2026-06-21T00:00:00Z",
                            }
                        }
                    }
                ]
            )
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=None)
        return resp

    mock_session.get.side_effect = mock_get
    provider.session = mock_session

    start_ns = 1781913600000000000  # 2026-06-20T00:00:00Z
    end_ns = 1782086400000000000  # 2026-06-22T00:00:00Z

    actions = []
    async for rec in provider.backfill("corp_action", "AAPL", start_ns, end_ns):
        actions.append(rec)

    corp_actions = [a for a in actions if isinstance(a, CorporateAction)]
    assert len(corp_actions) == 2

    div = next(a for a in corp_actions if a.type == CorpActionType.DIVIDEND_CASH)
    assert div.value == 0.25
    assert div.ex_date == "2026-06-20"

    split = next(a for a in corp_actions if a.type == CorpActionType.SPLIT)
    assert split.value == 2.0
    assert split.ex_date == "2026-06-21"


@pytest.mark.asyncio
async def test_msn_money_close() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
    )

    mock_session = AsyncMock()
    provider.session = mock_session

    await provider.close()
    mock_session.close.assert_called_once()
    assert provider.session is None
