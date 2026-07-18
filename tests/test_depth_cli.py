from typer.testing import CliRunner

from stockodile.cli import app
from stockodile.schema.records import DepthProfile

runner = CliRunner()


def test_depth_cli_prints_labeled_synth(monkeypatch):
    prof = DepthProfile(
        provider="synth", symbol="synth:AAPL", symbol_raw="AAPL", local_ts=1,
        bids=[(99.0, 10.0)], asks=[(101.0, 8.0)], reference_price=100.0,
        basis="yahoo_1m_vap", is_synthetic=True, depth=2,
    )

    class _FakeSource:
        async def snapshot(self, symbol): return prof

    monkeypatch.setattr("stockodile.depth.select.select_depth_source", lambda **k: _FakeSource())
    # also patch the name imported into cli if imported at call-time
    import stockodile.depth as depthpkg
    monkeypatch.setattr(depthpkg, "select_depth_source", lambda **k: _FakeSource())

    result = runner.invoke(app, ["depth", "AAPL", "--no-persist"])
    assert result.exit_code == 0
    assert "SYNTHETIC" in result.stdout
    assert "99.0" in result.stdout and "101.0" in result.stdout
