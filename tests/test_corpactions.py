import datetime

import polars as pl
import pytest

from stockodile.corpactions.calculator import (
    adjust_bars,
    adjust_dataframe,
    calculate_cumulative_factors,
    calculate_total_returns,
)
from stockodile.schema.enums import CorpActionType
from stockodile.schema.records import Bar, CorporateAction


def test_calculate_cumulative_factors_basic() -> None:
    # Scenario matching the Appendix worked example, but with correct ex-date logic:
    # Let's say Day 4 is the ex-date for a 2:1 split (FACPR = 1.0, f = 2.0).
    # Since Day 4 is the ex-date, Day 4 is post-split. Day 3 is pre-split.
    # Therefore, C(5) = 1.0, C(4) = 1.0, C(3) = 2.0, C(2) = 2.0, C(1) = 2.0.
    dates = [
        datetime.date(2026, 6, 1),  # Day 1
        datetime.date(2026, 6, 2),  # Day 2
        datetime.date(2026, 6, 3),  # Day 3
        datetime.date(2026, 6, 4),  # Day 4 (split ex-date)
        datetime.date(2026, 6, 5),  # Day 5
    ]

    actions = [
        CorporateAction(
            provider="test",
            symbol="TEST",
            symbol_raw="TEST",
            source_ts=None,
            local_ts=0,
            ex_date="2026-06-04",
            type=CorpActionType.SPLIT,
            value=1.0,  # FACPR = 1.0 -> f = 2.0
        ),
        CorporateAction(
            provider="test",
            symbol="TEST",
            symbol_raw="TEST",
            source_ts=None,
            local_ts=0,
            ex_date="2026-06-03",
            type=CorpActionType.DIVIDEND_CASH,
            value=0.50,  # Cash dividend (should not affect C(t))
        ),
    ]

    factors = calculate_cumulative_factors(actions, dates, base_date=datetime.date(2026, 6, 5))

    assert factors[datetime.date(2026, 6, 5)] == (1.0, 1.0)
    assert factors[datetime.date(2026, 6, 4)] == (1.0, 1.0)
    assert factors[datetime.date(2026, 6, 3)] == (2.0, 2.0)
    assert factors[datetime.date(2026, 6, 2)] == (2.0, 2.0)
    assert factors[datetime.date(2026, 6, 1)] == (2.0, 2.0)


def test_calculate_cumulative_factors_alternate_ex_date() -> None:
    # If we treat the split as occurring between Day 4 and Day 5 (so ex-date is Day 5):
    # Then C(5) = 1.0, C(4) = 2.0, C(3) = 2.0, C(2) = 2.0, C(1) = 2.0.
    dates = [
        "2026-06-01",
        "2026-06-02",
        "2026-06-03",
        "2026-06-04",
        "2026-06-05",
    ]

    actions = [
        CorporateAction(
            provider="test",
            symbol="TEST",
            symbol_raw="TEST",
            source_ts=None,
            local_ts=0,
            ex_date="2026-06-05",  # Split ex-date is Day 5
            type=CorpActionType.SPLIT,
            value=1.0,  # f = 2.0
        )
    ]

    factors = calculate_cumulative_factors(actions, dates, base_date="2026-06-05")

    assert factors[datetime.date(2026, 6, 5)] == (1.0, 1.0)
    assert factors[datetime.date(2026, 6, 4)] == (2.0, 2.0)
    assert factors[datetime.date(2026, 6, 3)] == (2.0, 2.0)
    assert factors[datetime.date(2026, 6, 2)] == (2.0, 2.0)
    assert factors[datetime.date(2026, 6, 1)] == (2.0, 2.0)


def test_adjust_bars_and_returns() -> None:
    # Create sample bars
    # Day 1: 200.0
    # Day 2: 210.0
    # Day 3: 220.0
    # Day 4: 104.0
    # Day 5: 106.0
    # Assume source_ts is Unix timestamp in ms
    base_ts = 1780000000000  # Some timestamp in ms
    one_day_ms = 24 * 60 * 60 * 1000

    bars = [
        Bar("test", "T", "T", base_ts, 0, "1d", 200.0, 200.0, 200.0, 200.0, 100.0),
        Bar("test", "T", "T", base_ts + one_day_ms, 0, "1d", 210.0, 210.0, 210.0, 210.0, 100.0),
        Bar("test", "T", "T", base_ts + 2 * one_day_ms, 0, "1d", 220.0, 220.0, 220.0, 220.0, 100.0),
        Bar("test", "T", "T", base_ts + 3 * one_day_ms, 0, "1d", 104.0, 104.0, 104.0, 104.0, 200.0),
        Bar("test", "T", "T", base_ts + 4 * one_day_ms, 0, "1d", 106.0, 106.0, 106.0, 106.0, 200.0),
    ]

    dates = []
    for b in bars:
        assert b.source_ts is not None
        dates.append(datetime.date.fromtimestamp(b.source_ts / 1000.0))

    # Event 1: 2:1 split on Day 4
    # Event 2: $0.50 cash dividend on Day 3
    actions = [
        CorporateAction("test", "T", "T", None, 0, dates[3].isoformat(), CorpActionType.SPLIT, 1.0),
        CorporateAction(
            "test", "T", "T", None, 0, dates[2].isoformat(), CorpActionType.DIVIDEND_CASH, 0.50
        ),
    ]

    factors = calculate_cumulative_factors(actions, dates, base_date=dates[4])
    adj_bars = adjust_bars(bars, factors)

    # Day 5 and Day 4 factors are 1.0 (no splits after them)
    assert adj_bars[4].close == pytest.approx(106.0)
    assert adj_bars[4].volume == pytest.approx(200.0)

    assert adj_bars[3].close == pytest.approx(104.0)
    assert adj_bars[3].volume == pytest.approx(200.0)

    # Days 1-3 have factor 2.0 (from 2:1 split)
    assert adj_bars[2].close == pytest.approx(110.0)
    assert adj_bars[2].volume == pytest.approx(200.0)

    assert adj_bars[1].close == pytest.approx(105.0)
    assert adj_bars[1].volume == pytest.approx(200.0)

    assert adj_bars[0].close == pytest.approx(100.0)
    assert adj_bars[0].volume == pytest.approx(200.0)

    # Calculate returns
    returns = calculate_total_returns(bars, actions, factors)
    # Day 1: None
    # Day 2: 105 / 100 - 1 = +5.0%
    # Day 3: (110.0 + (0.50 / 2)) / 105.0 - 1 = 110.25 / 105 - 1 = +5.0%
    # Day 4: 104.0 / 110.0 - 1 = -5.4545%
    # Day 5: 106.0 / 104.0 - 1 = +1.923%
    assert returns[0] is None
    assert returns[1] == pytest.approx(0.05)
    assert returns[2] == pytest.approx(0.05)
    assert returns[3] == pytest.approx(-0.05454545)
    assert returns[4] == pytest.approx(0.01923076)


def test_adjust_dataframe() -> None:
    # Test Polars DataFrame adjustment
    df = pl.DataFrame(
        {
            "date": ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"],
            "open": [200.0, 210.0, 220.0, 104.0, 106.0],
            "high": [200.0, 210.0, 220.0, 104.0, 106.0],
            "low": [200.0, 210.0, 220.0, 104.0, 106.0],
            "close": [200.0, 210.0, 220.0, 104.0, 106.0],
            "volume": [100.0, 100.0, 100.0, 200.0, 200.0],
        }
    )

    # Action 1: 2:1 split on 2026-06-04
    # Action 2: $0.50 cash dividend on 2026-06-03
    actions = pl.DataFrame(
        {
            "ex_date": ["2026-06-04", "2026-06-03"],
            "type": ["split", "dividend_cash"],
            "value": [1.0, 0.50],  # raw FACPR (1.0 for 2:1 split)
        }
    )

    adj_df = adjust_dataframe(df, actions, base_date="2026-06-05")

    # Assert columns added
    assert "cfacpr" in adj_df.columns
    assert "cfacshr" in adj_df.columns
    assert "adj_open" in adj_df.columns
    assert "adj_high" in adj_df.columns
    assert "adj_low" in adj_df.columns
    assert "adj_close" in adj_df.columns
    assert "adj_volume" in adj_df.columns
    assert "total_return" in adj_df.columns

    # Verify values
    close_vals = adj_df["adj_close"].to_list()
    assert close_vals[0] == pytest.approx(100.0)
    assert close_vals[1] == pytest.approx(105.0)
    assert close_vals[2] == pytest.approx(110.0)
    assert close_vals[3] == pytest.approx(104.0)
    assert close_vals[4] == pytest.approx(106.0)

    vol_vals = adj_df["adj_volume"].to_list()
    assert vol_vals[0] == pytest.approx(200.0)
    assert vol_vals[1] == pytest.approx(200.0)
    assert vol_vals[2] == pytest.approx(200.0)
    assert vol_vals[3] == pytest.approx(200.0)
    assert vol_vals[4] == pytest.approx(200.0)

    ret_vals = adj_df["total_return"].to_list()
    assert ret_vals[0] is None
    assert ret_vals[1] == pytest.approx(0.05)
    assert ret_vals[2] == pytest.approx(0.05)
    assert ret_vals[3] == pytest.approx(-0.05454545)
    assert ret_vals[4] == pytest.approx(0.01923076)
