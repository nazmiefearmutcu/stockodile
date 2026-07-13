"""Acceptance tests for the Typer CLI."""

from __future__ import annotations

import pathlib

from typer.testing import CliRunner

from stockodile.cli import app
from stockodile.schema.records import BookSnapshot, Trade
from stockodile.store.parquet_sink import ParquetSink

_BASE_TS = 1_700_000_000_000_000_000  # 2023-11-14


async def _write_fixtures(data_dir: pathlib.Path) -> None:
    """Write 3 trades + 1 book_snapshot into the data lake."""
    sink = ParquetSink(data_dir=data_dir, max_buffer_rows=10, flush_interval_seconds=9999)
    for price in [100.0, 200.0, 300.0]:
        await sink.put(
            Trade(
                provider="alpaca",
                symbol="alpaca:AAPL",
                symbol_raw="AAPL",
                source_ts=_BASE_TS,
                local_ts=_BASE_TS,
                id=str(price),
                price=price,
                size=1.0,
            )
        )
    await sink.put(
        BookSnapshot(
            provider="alpaca",
            symbol="alpaca:AAPL",
            symbol_raw="AAPL",
            source_ts=_BASE_TS,
            local_ts=_BASE_TS,
            bids=[(100.0, 5.0)],
            asks=[(101.0, 4.0)],
            depth=1,
            sequence_id=1,
            is_snapshot=True,
        )
    )
    await sink.flush()


async def test_cli_query_exits_zero_with_output(tmp_path: pathlib.Path) -> None:
    """``query`` against a fixture data dir returns exit code 0 and count=3."""
    await _write_fixtures(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["query", "SELECT count(*) AS n FROM trade", "--data-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, f"stdout:\n{result.output}"
    assert "3" in result.output


async def test_cli_catalog_exits_zero_lists_channels(tmp_path: pathlib.Path) -> None:
    """``catalog`` lists available channels and their row counts, exit code 0."""
    await _write_fixtures(tmp_path)
    runner = CliRunner()
    result = runner.invoke(app, ["catalog", "--data-dir", str(tmp_path)])
    assert result.exit_code == 0, f"stdout:\n{result.output}"
    assert "trade" in result.output
    assert "book_snapshot" in result.output


async def test_cli_export_csv_creates_file(tmp_path: pathlib.Path) -> None:
    """``export`` with fmt=csv writes a non-empty file, exit code 0."""
    await _write_fixtures(tmp_path)
    dest = tmp_path / "out" / "trades.csv"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "export",
            "--channel",
            "trade",
            "--symbols",
            "alpaca:AAPL",
            "--from",
            str(_BASE_TS - 1),
            "--to",
            str(_BASE_TS + 1),
            "--fmt",
            "csv",
            "--dest",
            str(dest),
            "--data-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, f"stdout:\n{result.output}"
    assert dest.exists()
    assert dest.stat().st_size > 0


async def test_cli_export_limit_truncates_rows(tmp_path: pathlib.Path) -> None:
    """``export --limit N`` writes at most N data rows."""
    await _write_fixtures(tmp_path)
    dest = tmp_path / "out" / "limited.csv"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "export",
            "--channel",
            "trade",
            "--symbols",
            "alpaca:AAPL",
            "--from",
            str(_BASE_TS - 1),
            "--to",
            str(_BASE_TS + 1),
            "--fmt",
            "csv",
            "--dest",
            str(dest),
            "--limit",
            "1",
            "--data-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, f"stdout:\n{result.output}"
    assert dest.exists()
    lines = dest.read_text().strip().splitlines()
    # header + 1 data row (fixtures write 3 trades)
    assert len(lines) == 2, f"expected header + 1 row, got {len(lines)} lines:\n{dest.read_text()}"


async def test_cli_replay_exits_zero(tmp_path: pathlib.Path) -> None:
    """``replay`` lists records from the fixture data lake, exit code 0."""
    await _write_fixtures(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "replay",
            "--channels",
            "trade",
            "--symbols",
            "alpaca:AAPL",
            "--from",
            str(_BASE_TS - 1),
            "--to",
            str(_BASE_TS + 1),
            "--data-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, f"stdout:\n{result.output}"
    assert "trade" in result.output or "alpaca" in result.output


async def test_cli_resample_exits_zero(tmp_path: pathlib.Path) -> None:
    """``resample`` resamples trade data in the lake to OHLCV bars, exit code 0."""
    await _write_fixtures(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "resample",
            "--symbol",
            "alpaca:AAPL",
            "--interval",
            "1s",
            "--from",
            str(_BASE_TS - 1),
            "--to",
            str(_BASE_TS + 1_000_000_000),
            "--data-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, f"stdout:\n{result.output}"
    assert "open" in result.output or "high" in result.output


async def test_cli_indicators_exits_zero(tmp_path: pathlib.Path) -> None:
    """``indicators`` calculates technical analysis indicators, exit code 0."""
    await _write_fixtures(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "indicators",
            "--symbol",
            "alpaca:AAPL",
            "--indicator",
            "sma",
            "--period",
            "2",
            "--interval",
            "1s",
            "--from",
            str(_BASE_TS - 1),
            "--to",
            str(_BASE_TS + 1_000_000_000),
            "--data-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, f"stdout:\n{result.output}"
    assert "sma" in result.output

