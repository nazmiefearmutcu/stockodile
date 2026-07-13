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
        apikey="test-key",
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
        apikey="test-key",
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
        apikey="test-key",
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
        apikey="test-key",
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
        apikey="test-key",
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
        apikey="test-key",
    )

    mock_session = AsyncMock()
    provider.session = mock_session

    await provider.close()
    mock_session.close.assert_called_once()
    assert provider.session is None


@pytest.mark.asyncio
async def test_msn_money_split_parsing_ratios() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["corp_actions"],
        out=sink,
        registry=registry,
        apikey="test-key",
    )

    mock_session = MagicMock()

    # Test 3:2 and 1:5 splits
    def mock_get(url: str, *args: Any, **kwargs: Any) -> MagicMock:
        resp = MagicMock()
        resp.status = 200
        if "Query" in url:
            stock_aapl = '{"RT00S": "AAPL", "SecId": "12345"}'
            resp.json = AsyncMock(return_value={"data": {"stocks": [stock_aapl]}})
        elif "QuoteSummary" in url:
            resp.json = AsyncMock(
                return_value=[
                    {
                        "equity": {
                            "shareStatistics": {
                                "lastSplitFactor": "3:2",
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

    start_ns = 1781913600000000000
    end_ns = 1782086400000000000

    # 3:2 split should yield 1.5
    actions_3_2 = []
    async for rec in provider.backfill("corp_action", "AAPL", start_ns, end_ns):
        actions_3_2.append(rec)
    assert len(actions_3_2) == 1
    act_3_2 = actions_3_2[0]
    assert isinstance(act_3_2, CorporateAction)
    assert act_3_2.value == 1.5

    # 1:5 split should yield 0.2
    def mock_get_1_5(url: str, *args: Any, **kwargs: Any) -> MagicMock:
        resp = MagicMock()
        resp.status = 200
        if "Query" in url:
            stock_aapl = '{"RT00S": "AAPL", "SecId": "12345"}'
            resp.json = AsyncMock(return_value={"data": {"stocks": [stock_aapl]}})
        elif "QuoteSummary" in url:
            resp.json = AsyncMock(
                return_value=[
                    {
                        "equity": {
                            "shareStatistics": {
                                "lastSplitFactor": "1:5",
                                "lastSplitDate": "2026-06-21T00:00:00Z",
                            }
                        }
                    }
                ]
            )
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=None)
        return resp

    mock_session.get.side_effect = mock_get_1_5
    actions_1_5 = []
    async for rec in provider.backfill("corp_action", "AAPL", start_ns, end_ns):
        actions_1_5.append(rec)
    assert len(actions_1_5) == 1
    act_1_5 = actions_1_5[0]
    assert isinstance(act_1_5, CorporateAction)
    assert act_1_5.value == 0.2


@pytest.mark.asyncio
async def test_msn_money_autosuggest_class_suffixes() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["RDS.A"],
        channels=["bar"],
        out=sink,
        registry=registry,
        apikey="test-key",
    )

    mock_session = MagicMock()
    mock_suggest_resp = MagicMock()
    mock_suggest_resp.status = 200
    # Bing returns suggestion RDS.A and RDS.B
    mock_suggest_resp.json = AsyncMock(
        return_value={
            "data": {
                "stocks": [
                    '{"RT00S": "RDS.B", "SecId": "54321"}',
                    '{"RT00S": "RDS.A", "SecId": "12345"}',
                ]
            }
        }
    )
    mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_suggest_resp)
    mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)

    provider.session = mock_session
    sec_id = await provider._resolve_sec_id("RDS.A")
    # Should match RDS.A and return 12345 (instead of 54321 fallback because of mismatch)
    assert sec_id == "12345"


@pytest.mark.asyncio
async def test_msn_money_float_conversion_vulnerability() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
        apikey="test-key",
    )

    mock_session = MagicMock()

    # Mock data returning None, "N/A", empty string for price arrays
    def mock_get(url: str, *args: Any, **kwargs: Any) -> MagicMock:
        resp = MagicMock()
        resp.status = 200
        if "Query" in url:
            stock_aapl = '{"RT00S": "AAPL", "SecId": "12345"}'
            resp.json = AsyncMock(return_value={"data": {"stocks": [stock_aapl]}})
        elif "Charts" in url:
            resp.json = AsyncMock(
                return_value=[
                    {
                        "series": {
                            "openPrices": [None],
                            "prices": ["N/A"],
                            "pricesHigh": [""],
                            "pricesLow": ["150.0"],
                            "volumes": ["10,000"],
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

    start_ns = 1781913600000000000
    end_ns = 1782000000000000000

    bars = []
    async for rec in provider.backfill("bar", "AAPL", start_ns, end_ns):
        bars.append(rec)

    bar = bars[0]
    assert isinstance(bar, Bar)
    assert bar.open == 0.0
    assert bar.close == 0.0
    assert bar.high == 0.0
    assert bar.low == 150.0
    assert bar.volume == 10000.0


@pytest.mark.asyncio
async def test_msn_money_response_error_handling() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
        apikey="test-key",
    )

    # 1. Non-200 suggest response → hard fail (no silent wrong SecId)
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status = 404
    mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)
    provider.session = mock_session
    with pytest.raises(ValueError, match="Could not resolve"):
        await provider._resolve_sec_id("AAPL")

    # 2. Chart JSON returns list with None [None] — resolve succeeds, chart skips cleanly
    def mock_get(url: str, *args: Any, **kwargs: Any) -> MagicMock:
        resp = MagicMock()
        resp.status = 200
        if "Query" in url:
            resp.json = AsyncMock(
                return_value={"data": {"stocks": ['{"RT00S": "AAPL", "SecId": "12345"}']}}
            )
        elif "Charts" in url:
            resp.json = AsyncMock(return_value=[None])
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=None)
        return resp

    mock_session.get.side_effect = mock_get
    bars = []
    async for rec in provider.backfill("bar", "AAPL", 0, 999999999999999999):
        bars.append(rec)
    assert len(bars) == 0  # didn't throw TypeError, just cleanly skipped


@pytest.mark.asyncio
async def test_msn_money_run_not_implemented() -> None:
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
        apikey="test-key",
    )
    with pytest.raises(NotImplementedError):
        await provider.run()


def test_msn_money_apikey_defaults_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default apikey must not be a hardcoded production-looking secret."""
    monkeypatch.delenv("MSN_MONEY_APIKEY", raising=False)
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
    )
    assert provider.apikey == ""
    # Guard against reintroducing a shipped default key in source.
    import inspect

    src = inspect.getsource(MsnMoneyProvider.__init__)
    assert "0QfOX3Vn" not in src
    assert 'apikey: str = "' not in src


def test_msn_money_apikey_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MSN_MONEY_APIKEY", "env-test-key")
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
    )
    assert provider.apikey == "env-test-key"


def test_msn_money_apikey_explicit_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MSN_MONEY_APIKEY", "env-test-key")
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
        apikey="explicit-key",
    )
    assert provider.apikey == "explicit-key"


@pytest.mark.asyncio
async def test_msn_money_backfill_requires_apikey(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MSN_MONEY_APIKEY", raising=False)
    registry = InstrumentRegistry()
    sink = MemorySink()
    provider = MsnMoneyProvider(
        symbols=["AAPL"],
        channels=["bar"],
        out=sink,
        registry=registry,
    )
    with pytest.raises(ValueError, match="API key required"):
        async for _ in provider.backfill("bar", "AAPL", 0, 1):
            pass
