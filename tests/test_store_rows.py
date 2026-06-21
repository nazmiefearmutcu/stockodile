from stockodile.schema.enums import CorpActionType, FundPeriod, Tape
from stockodile.schema.records import (
    OHLCV,
    Bar,
    BookDelta,
    BookSnapshot,
    CorporateAction,
    Filing,
    Fundamental,
    IndexValue,
    Quote,
    Trade,
)
from stockodile.store.rows import from_row, to_row


def test_trade_to_from_row() -> None:
    t = Trade(
        provider="alpaca",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        id="t-12345",
        price=180.5,
        size=100.0,
        conditions=["@", "F"],
        tape=Tape.A,
        venue="IEX",
    )
    row = to_row(t)
    assert row["channel"] == "trade"
    assert row["symbol"] == "AAPL"
    assert row["tape"] == "A"
    assert "date" in row
    assert "bucket" in row
    assert isinstance(row["bucket"], int)

    t2 = from_row(row)
    assert t2 == t


def test_quote_to_from_row() -> None:
    q = Quote(
        provider="alpaca",
        symbol="MSFT",
        symbol_raw="MSFT",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        bid_px=350.0,
        bid_sz=10.0,
        ask_px=350.1,
        ask_sz=20.0,
        is_nbbo=True,
        is_consolidated=True,
        conditions=["R"],
        tape=Tape.B,
    )
    row = to_row(q)
    assert row["channel"] == "quote"
    assert row["bid_px"] == 350.0
    assert row["tape"] == "B"

    q2 = from_row(row)
    assert q2 == q


def test_book_snapshot_to_from_row() -> None:
    b = BookSnapshot(
        provider="iex",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        bids=[(180.0, 10.0), (179.9, 20.0)],
        asks=[(180.1, 5.0), (180.2, 15.0)],
        depth=2,
        sequence_id=98765,
        is_snapshot=True,
    )
    row = to_row(b)
    assert row["channel"] == "book_snapshot"
    # bids/asks preserved as lists of tuples/lists
    assert row["bids"] == [(180.0, 10.0), (179.9, 20.0)]

    b2 = from_row(row)
    assert b2 == b


def test_book_delta_to_from_row() -> None:
    d = BookDelta(
        provider="iex",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        bids=[(180.0, 0.0)],  # remove level
        asks=[],
        seq_id=98766,
        prev_seq_id=98765,
        is_snapshot=False,
    )
    row = to_row(d)
    assert row["channel"] == "book_delta"

    d2 = from_row(row)
    assert d2 == d


def test_corporate_action_to_from_row() -> None:
    c = CorporateAction(
        provider="sec_edgar",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        ex_date="2026-06-15",
        type=CorpActionType.DIVIDEND_CASH,
        value=0.5,
    )
    row = to_row(c)
    assert row["channel"] == "corp_action"
    assert row["type"] == "dividend_cash"

    c2 = from_row(row)
    assert c2 == c


def test_bar_to_from_row() -> None:
    b = Bar(
        provider="stooq",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=1700000005000000000,
        interval="1d",
        open=180.0,
        high=182.0,
        low=179.5,
        close=181.2,
        volume=5000000.0,
        vwap=180.8,
        trade_count=1234,
    )
    row = to_row(b)
    assert row["channel"] == "bar"

    b2 = from_row(row)
    assert b2 == b


def test_fundamental_to_from_row() -> None:
    f = Fundamental(
        provider="sec_edgar",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        taxonomy="us-gaap",
        tag="Revenues",
        unit="USD",
        val=90000000000.0,
        end="2025-12-31",
        start="2025-10-01",
        fy=2025,
        fp=FundPeriod.Q4,
        form="10-K",
        filed="2026-01-30",
        accn="0000320193-26-000010",
        frame="CY2025Q4",
    )
    row = to_row(f)
    assert row["channel"] == "fundamental"

    f2 = from_row(row)
    assert f2 == f


def test_filing_to_from_row() -> None:
    f = Filing(
        provider="sec_edgar",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        accession_number="0000320193-26-000010",
        form="10-K",
        filing_date="2026-01-30",
        primary_document="aapl-20251231.htm",
        document_url="https://www.sec.gov/Archives/edgar/data/320193/000032019326000010/aapl-20251231.htm",
        report_date="2025-12-31",
        is_xbrl=True,
    )
    row = to_row(f)
    assert row["channel"] == "filing"

    f2 = from_row(row)
    assert f2 == f


def test_ohlcv_to_from_row() -> None:
    o = OHLCV(
        provider="stooq",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=None,
        local_ts=1700000005000000000,
        interval="1d",
        open=180.0,
        high=182.0,
        low=179.5,
        close=181.2,
        volume=5000000.0,
        vwap=180.8,
        trade_count=1234,
    )
    row = to_row(o)
    assert row["channel"] == "ohlcv"

    o2 = from_row(row)
    assert o2 == o


def test_index_value_to_from_row() -> None:
    i = IndexValue(
        provider="fred",
        symbol="SP500",
        symbol_raw="SP500",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        value=5000.5,
    )
    row = to_row(i)
    assert row["channel"] == "index_value"

    i2 = from_row(row)
    assert i2 == i
