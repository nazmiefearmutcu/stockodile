from stockodile.schema.enums import (
    CorpActionType,
    FundPeriod,
    OptType,
    SecurityType,
    Side,
    Tape,
)
from stockodile.schema.records import (
    OHLCV,
    Auction,
    Bar,
    BookDelta,
    BookSnapshot,
    CorporateAction,
    Filing,
    Fundamental,
    Holding13F,
    IndexValue,
    InsiderTransaction,
    Instrument,
    MacroSeries,
    OptionQuote,
    Quote,
    ShortInterest,
    ShortVolume,
    Trade,
    TradingStatus,
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


def test_new_records_to_from_row() -> None:
    # 1. Auction
    a = Auction(
        provider="nasdaq",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        paired_shares=100000.0,
        imbalance_shares=5000.0,
        imbalance_side=Side.BUY,
        reference_price=180.5,
        indicative_price=180.6,
        auction_type="open",
    )
    assert from_row(to_row(a)) == a

    # 2. TradingStatus
    ts = TradingStatus(
        provider="nasdaq",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        status="H",
        reason="LULD",
        limit_up_price=190.0,
        limit_down_price=170.0,
        indicator="Y",
    )
    assert from_row(to_row(ts)) == ts

    # 3. Instrument
    inst = Instrument(
        provider="alpaca",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        name="Apple Inc.",
        cik="0000320193",
        figi="BBG000B9Y5X2",
        composite_figi="BBG000B9Y5X2",
        share_class_figi="BBG001S5N8V8",
        cusip="037833100",
        exchange="NASDAQ",
        security_type=SecurityType.CS,
        sic="3571",
        shares_outstanding=15000000000,
        listing_date="1980-12-12",
        status="active",
    )
    assert from_row(to_row(inst)) == inst

    # 4. InsiderTransaction
    ins = InsiderTransaction(
        provider="yahoo",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        insider_name="Cook Timothy D",
        position="CEO",
        transaction_type="Sale",
        transaction_date="2026-06-01",
        shares=10000.0,
        price=180.5,
        value=1805000.0,
        ownership="D",
    )
    assert from_row(to_row(ins)) == ins

    # 5. Holding13F
    h13 = Holding13F(
        provider="sec_edgar",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        manager_name="BERKSHIRE HATHAWAY INC",
        issuer_name="APPLE INC",
        cusip="037833100",
        value=150000000.0,
        shares=1000000.0,
        shares_type="SH",
        discretion="SOLE",
        voting_sole=1000000.0,
        voting_shared=0.0,
        voting_none=0.0,
        report_date="2025-12-31",
        accession_number="0000320193-26-000010",
    )
    assert from_row(to_row(h13)) == h13

    # 6. ShortInterest
    si = ShortInterest(
        provider="finra",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        settlement_date="2026-05-15",
        short_interest=50000000.0,
        prev_short_interest=48000000.0,
        days_to_cover=1.5,
        change_pct=4.17,
    )
    assert from_row(to_row(si)) == si

    # 7. ShortVolume
    sv = ShortVolume(
        provider="finra",
        symbol="AAPL",
        symbol_raw="AAPL",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        date="2026-06-18",
        short_volume=1200000.0,
        short_exempt_volume=15000.0,
        total_volume=3000000.0,
    )
    assert from_row(to_row(sv)) == sv

    # 8. OptionQuote
    oq = OptionQuote(
        provider="yahoo",
        symbol="AAPL260618C00180000",
        symbol_raw="AAPL260618C00180000",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        underlying="AAPL",
        expiry="2026-06-18",
        strike=180.0,
        type=OptType.C,
        bid=5.5,
        ask=5.7,
        last=5.6,
        volume=1200.0,
        open_interest=5000.0,
        implied_volatility=0.25,
        delta=0.55,
        gamma=0.03,
        vega=0.15,
        theta=-0.05,
        rho=0.08,
    )
    assert from_row(to_row(oq)) == oq

    # 9. MacroSeries
    ms = MacroSeries(
        provider="fred",
        symbol="UNRATE",
        symbol_raw="UNRATE",
        source_ts=1700000000000000000,
        local_ts=1700000005000000000,
        date="2026-05-01",
        value=3.9,
        realtime_start="2026-06-05",
        realtime_end="9999-12-31",
    )
    assert from_row(to_row(ms)) == ms
