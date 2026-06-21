"""Typer and Rich CLI for Stockodile.

Commands
        collect   -- Run live providers and write data to the Parquet lake.
        query     -- Execute DuckDB SQL against the data lake.
        catalog   -- List all channels present in the data lake with row counts.
        replay    -- Stream canonical Records from the data lake, printed to stdout.
        export    -- Export a channel x symbols x time range to a file.
        resample  -- Resample trade records to OHLCV bars.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from stockodile.client.client import StockodileClient
from stockodile.client.collect import collect as collect_live
from stockodile.ingest.transport import AiohttpWsTransport
from stockodile.providers.factory import make_provider
from stockodile.reference.registry import InstrumentRegistry
from stockodile.store.parquet_sink import ParquetSink

console = Console()

app = typer.Typer(
    name="stockodile",
    help="Stockodile -- open-source US-equity market-data engine.",
    add_completion=False,
)

_DataDirOpt = Annotated[
    Path,
    typer.Option(
        "--data-dir",
        help="Root directory of the Parquet data lake.",
        show_default=False,
    ),
]


@app.command()
def collect(
    provider: Annotated[str, typer.Option("--provider", help="Provider name, e.g. alpaca.")],
    symbols: Annotated[
        list[str],
        typer.Option("--symbols", help="Symbol(s) to collect. Repeat for multiple."),
    ],
    channels: Annotated[
        list[str],
        typer.Option("--channels", help="Channel(s) to subscribe. Repeat for multiple."),
    ],
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """Collect live market data from a provider and write to the Parquet data lake.

    Press Ctrl-C (SIGINT) to stop gracefully.
    """
    sink = ParquetSink(
        data_dir=data_dir,
        max_buffer_rows=10_000,
        flush_interval_seconds=5.0,
    )
    registry = InstrumentRegistry()

    try:
        conn = make_provider(
            provider=provider,
            symbols=list(symbols),
            channels=list(channels),
            out=sink,
            registry=registry,
        )
    except ValueError as exc:
        console.print(f"[bold red]Error:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    # Wire live WebSocket transport if not set
    if conn.transport is None:
        conn.transport = AiohttpWsTransport(conn.ws_url)

    console.print(
        f"[bold green]Starting collection:[/bold green] provider={provider!r} "
        f"symbols={symbols} channels={channels} data_dir={data_dir}"
    )

    try:
        asyncio.run(collect_live([conn], sink))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass

    console.print(f"[bold green]Collection stopped.[/bold green] Data written to: {data_dir}")


@app.command()
def query(
    sql: Annotated[str, typer.Argument(help="DuckDB SQL query to execute.")],
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """Execute a DuckDB SQL query against the data lake and print the result."""
    client = StockodileClient(data_dir=data_dir)
    df = client.query(sql)
    typer.echo(df)


@app.command()
def catalog(
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """List channels present in the data lake with their row counts."""
    client = StockodileClient(data_dir=data_dir)
    cat = client._catalog

    channels = sorted(cat._registered_channels)

    if not channels:
        console.print(f"[bold yellow]No data found in directory:[/bold yellow] {data_dir}")
        raise typer.Exit(code=0)

    table = Table(title="Stockodile Catalog")
    table.add_column("Channel", style="cyan", no_wrap=True)
    table.add_column("Row Count", style="magenta", justify="right")

    for ch in channels:
        try:
            row_df = cat.query(f'SELECT count(*) AS n FROM "{ch}"')
            n = int(row_df["n"][0])
            table.add_row(ch, f"{n:,}")
        except Exception:
            table.add_row(ch, "-1")

    console.print(table)


@app.command()
def replay(
    channels: Annotated[
        list[str],
        typer.Option("--channels", help="Channel name(s). Repeat for multiple."),
    ],
    symbols: Annotated[
        list[str],
        typer.Option("--symbols", help="Canonical symbol(s). Repeat for multiple."),
    ],
    frm: Annotated[
        int,
        typer.Option("--from", help="Start of time range (nanoseconds UTC)."),
    ],
    to: Annotated[
        int,
        typer.Option("--to", help="End of time range (nanoseconds UTC)."),
    ],
    data_dir: _DataDirOpt = Path("data"),
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Maximum number of records to print."),
    ] = None,
) -> None:
    """Stream canonical Records from the data lake, printed to stdout."""
    client = StockodileClient(data_dir=data_dir)
    count = 0
    for record in client.replay(channels, symbols, frm, to):
        typer.echo(repr(record))
        count += 1
        if limit is not None and count >= limit:
            break
    console.print(f"[bold cyan]-- {count} record(s) replayed.[/bold cyan]")


@app.command()
def export(
    channel: Annotated[str, typer.Option("--channel", help="Channel name, e.g. trade.")],
    symbols: Annotated[
        list[str],
        typer.Option("--symbols", help="Canonical symbol(s). Repeat for multiple."),
    ],
    frm: Annotated[
        int,
        typer.Option("--from", help="Start of time range (nanoseconds UTC)."),
    ],
    to: Annotated[
        int,
        typer.Option("--to", help="End of time range (nanoseconds UTC)."),
    ],
    fmt: Annotated[
        str,
        typer.Option("--fmt", help="Output format: parquet|csv|arrow|json|jsonl."),
    ] = "parquet",
    dest: Annotated[
        Path,
        typer.Option("--dest", help="Destination file path."),
    ] = Path("export.parquet"),
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """Export channel x symbols x time range to a file."""
    client = StockodileClient(data_dir=data_dir)
    client.export(channel, symbols, frm, to, fmt=fmt, dest=dest)  # type: ignore[arg-type]
    console.print(f"[bold green]Exported successfully to:[/bold green] {dest}")


@app.command()
def resample(
    symbol: Annotated[
        str,
        typer.Option("--symbol", help="Canonical symbol, e.g. alpaca:AAPL."),
    ],
    interval: Annotated[
        str,
        typer.Option("--interval", help="Resampling interval (e.g. 1m, 1h, 1d)."),
    ],
    frm: Annotated[
        int,
        typer.Option("--from", help="Start of time range (nanoseconds UTC)."),
    ],
    to: Annotated[
        int,
        typer.Option("--to", help="End of time range (nanoseconds UTC)."),
    ],
    fill: Annotated[
        bool,
        typer.Option("--fill", help="Fill empty periods with last known close."),
    ] = False,
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """Resample trade data in the lake into OHLCV bars."""
    client = StockodileClient(data_dir=data_dir)
    df = client.resample(symbol, frm, to, interval, fill_empty=fill)
    typer.echo(df)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
