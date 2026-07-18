from stockodile.schema.records import DepthProfile, Record
from stockodile.schema import DepthProfile as DepthProfileExported


def test_depthprofile_tag_and_union():
    rec = DepthProfile(
        provider="synth", symbol="synth:AAPL", symbol_raw="AAPL", local_ts=1,
        bids=[(100.0, 5.0)], asks=[(101.0, 4.0)], reference_price=100.5,
        basis="yahoo_1m_vap", is_synthetic=True, depth=2,
    )
    assert type(rec).__struct_config__.tag == "depth"
    assert isinstance(rec, Record)
    assert DepthProfileExported is DepthProfile
