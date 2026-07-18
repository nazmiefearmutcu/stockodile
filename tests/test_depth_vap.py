import pytest

from stockodile.depth.vap import reference_price, split_ladder, volume_at_price
from stockodile.schema.records import Bar


def _bar(o, h, lo, c, v, ts=1):
    return Bar(provider="yahoo", symbol="yahoo:X", symbol_raw="X", local_ts=ts,
               interval="1m", open=o, high=h, low=lo, close=c, volume=v)


def test_reference_price_is_last_close():
    bars = [_bar(10, 11, 9, 10.5, 100, ts=1), _bar(10.5, 12, 10, 11.0, 200, ts=2)]
    assert reference_price(bars) == 11.0


def test_uniform_volume_conserved():
    bars = [_bar(10, 12, 10, 11, 100), _bar(11, 13, 11, 12, 200)]
    levels = volume_at_price(bars, bins=4, method="uniform")
    total = sum(sz for _, sz in levels)
    assert total == pytest.approx(300.0)  # all volume preserved
    assert all(sz >= 0 for _, sz in levels)


def test_typical_price_point_mass():
    # single bar, typical = (H+L+C)/3 = (12+10+11)/3 = 11.0 -> all 90 volume at ~11
    bars = [_bar(10, 12, 10, 11, 90)]
    levels = volume_at_price(bars, bins=10, method="typical")
    assert sum(sz for _, sz in levels) == pytest.approx(90.0)
    peak_price = max(levels, key=lambda pl: pl[1])[0]
    assert peak_price == pytest.approx(11.0, abs=0.5)


def test_split_ladder_orders_and_truncates():
    profile = [(98.0, 1), (99.0, 2), (100.0, 5), (101.0, 3), (102.0, 4)]
    bids, asks = split_ladder(profile, reference_price=100.0, top_n=2)
    # bids: prices < ref, descending; asks: prices > ref, ascending
    assert [p for p, _ in bids] == [99.0, 98.0]
    assert [p for p, _ in asks] == [101.0, 102.0]


def test_degenerate_no_volume_returns_empty():
    bars = [_bar(10, 10, 10, 10, 0)]
    assert volume_at_price(bars, bins=4, method="uniform") == []
