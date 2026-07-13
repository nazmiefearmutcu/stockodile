from __future__ import annotations

from typing import Any

from stockodile.schema.enums import Tape
from stockodile.schema.records import (
    Bar,
    BookDelta,
    BookSnapshot,
    Quote,
    Trade,
)
from stockodile.store.rows import from_row, to_row

_BASE_TS = 1_700_000_000_000_000_000  # 2023-11-14


def test_to_row_adds_partition_cols() -> None:
    t = Trade(
        provider="alpaca",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000000000000000,
        id="1",
        price=150.0,
        size=10.0,
        conditions=["@"],
        tape=Tape.A,
        venue="NASDAQ",
    )
    row = to_row(t)
    assert row["channel"] == "trade"
    assert row["date"] == "2023-11-14"
    assert 0 <= row["bucket"] < 128
    assert row["tape"] == "A"
    assert row["exchange"] == "alpaca"


def test_to_row_source_ts_none() -> None:
    t = Trade(
        provider="finnhub",
        symbol="TSLA",
        symbol_raw="TSLA",
        source_ts=None,
        local_ts=1700000000000000000,
        id="2",
        price=200.0,
        size=5.0,
        conditions=None,
        tape=None,
        venue=None,
    )
    row = to_row(t)
    assert row["source_ts"] is None
    assert row["channel"] == "trade"
    assert row["tape"] is None


def test_to_row_book_snapshot_levels() -> None:
    snap = BookSnapshot(
        provider="alpaca",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000000000000000,
        bids=[(150.0, 100.0), (149.0, 0.0)],
        asks=[(151.0, 50.0)],
        depth=2,
        sequence_id=42,
        is_snapshot=True,
    )
    row = to_row(snap)
    assert row["channel"] == "book_snapshot"
    assert row["date"] == "2023-11-14"
    assert 0 <= row["bucket"] < 128
    assert row["bids"] == [(150.0, 100.0), (149.0, 0.0)]
    assert row["asks"] == [(151.0, 50.0)]


def test_bucket_is_deterministic() -> None:
    t = Trade(
        provider="alpaca",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=1700000000000000000,
        id="3",
        price=150.0,
        size=1.0,
        conditions=None,
        tape=None,
        venue=None,
    )
    row1 = to_row(t)
    row2 = to_row(t)
    assert row1["bucket"] == row2["bucket"]


def _base(channel: str) -> dict[str, Any]:
    """Shared fields present in every channel row."""
    return {
        "channel": channel,
        "provider": "alpaca",
        "symbol": "AAPL",
        "symbol_raw": "AAPL",
        "source_ts": _BASE_TS,
        "local_ts": _BASE_TS,
        "date": "2023-11-14",
        "bucket": 42,
        "exchange": "alpaca",
    }


def test_from_row_trade() -> None:
    row = {
        **_base("trade"),
        "id": "1",
        "price": 150.0,
        "size": 10.0,
        "conditions": ["@"],
        "tape": "A",
        "venue": "NASDAQ",
    }
    rec = from_row(row)
    assert isinstance(rec, Trade)
    assert rec.price == 150.0
    assert rec.size == 10.0
    assert rec.tape == Tape.A
    assert rec.venue == "NASDAQ"


def test_from_row_quote() -> None:
    row = {
        **_base("quote"),
        "bid_px": 150.0,
        "bid_sz": 100.0,
        "ask_px": 151.0,
        "ask_sz": 50.0,
        "is_nbbo": True,
        "is_consolidated": True,
        "conditions": ["R"],
        "tape": "B",
    }
    rec = from_row(row)
    assert isinstance(rec, Quote)
    assert rec.bid_px == 150.0
    assert rec.bid_sz == 100.0
    assert rec.ask_px == 151.0
    assert rec.ask_sz == 50.0
    assert rec.is_nbbo is True
    assert rec.is_consolidated is True
    assert rec.tape == Tape.B


def test_from_row_book_snapshot() -> None:
    row = {
        **_base("book_snapshot"),
        "bids": [{"price": 150.0, "size": 100.0}, {"price": 149.0, "size": 0.0}],
        "asks": [{"price": 151.0, "size": 50.0}],
        "depth": 2,
        "sequence_id": 42,
        "is_snapshot": True,
    }
    rec = from_row(row)
    assert isinstance(rec, BookSnapshot)
    assert rec.bids == [(150.0, 100.0), (149.0, 0.0)]
    assert rec.asks == [(151.0, 50.0)]
    assert rec.depth == 2
    assert rec.sequence_id == 42
    assert rec.is_snapshot is True


def test_from_row_book_delta() -> None:
    row = {
        **_base("book_delta"),
        "bids": [{"price": 150.0, "size": 100.0}],
        "asks": [{"price": 151.0, "size": 50.0}],
        "seq_id": 43,
        "prev_seq_id": 42,
        "is_snapshot": False,
    }
    rec = from_row(row)
    assert isinstance(rec, BookDelta)
    assert rec.bids == [(150.0, 100.0)]
    assert rec.asks == [(151.0, 50.0)]
    assert rec.seq_id == 43
    assert rec.prev_seq_id == 42
    assert rec.is_snapshot is False


def test_from_row_bar() -> None:
    row = {
        **_base("bar"),
        "interval": "1m",
        "open": 150.0,
        "high": 151.0,
        "low": 149.0,
        "close": 150.5,
        "volume": 10000.0,
        "vwap": 150.2,
        "trade_count": 120,
    }
    rec = from_row(row)
    assert isinstance(rec, Bar)
    assert rec.interval == "1m"
    assert rec.open == 150.0
    assert rec.vwap == 150.2
    assert rec.trade_count == 120


def test_from_row_unknown_channel_raises() -> None:
    import pytest

    with pytest.raises(ValueError, match="Unknown channel tag"):
        from_row({**_base("not_a_channel")})


def test_onchain_limit_order_fill_roundtrip() -> None:
    from stockodile.schema.records import LimitOrderFill

    rec = LimitOrderFill(
        provider="base_onchain",
        symbol="ETH-USDC",
        symbol_raw="ETH-USDC",
        local_ts=_BASE_TS,
        source_ts=_BASE_TS,
        exchange_ts=_BASE_TS,
        tx_hash="0xabc",
        log_index=1,
        protocol="1inch",
        maker="0x1",
        taker="0x2",
        maker_token="0xa",
        taker_token="0xb",
        maker_amount=1.5,
        taker_amount=2.5,
        order_hash="0xord",
    )
    row = to_row(rec)
    assert row["channel"] == "limit_order_fill"
    assert row["maker_amount"] == 1.5
    back = from_row(row)
    assert isinstance(back, LimitOrderFill)
    assert back.tx_hash == "0xabc"
    assert back.maker_amount == 1.5


def test_onchain_por_update_roundtrip() -> None:
    from stockodile.schema.records import PoRUpdate

    rec = PoRUpdate(
        provider="base_onchain",
        symbol="cbBTC",
        symbol_raw="cbBTC",
        exchange_ts=_BASE_TS,
        local_ts=_BASE_TS,
        feed_address="0xfeed",
        token_address="0xtok",
        reserves=100.0,
        total_supply=100.0,
        backing_ratio=1.0,
        is_backed=True,
        source_ts=_BASE_TS,
    )
    back = from_row(to_row(rec))
    assert isinstance(back, PoRUpdate)
    assert back.is_backed is True
    assert back.reserves == 100.0
