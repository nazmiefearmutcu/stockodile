"""Typer CLI for Stockodile.

Commands
--------
query      -- Execute DuckDB SQL against the data lake; print result table.
catalog    -- List all channels present in the data lake with row counts.
export     -- Export a channel x symbols x time range to a file.
replay     -- Stream canonical Records from the data lake, printed to stdout.
collect    -- Run live connectors and write data to the Parquet lake.
resample   -- Resample trade records to OHLCV bars.
indicators -- Calculate technical analysis indicators on OHLCV bars.
shell      -- Start an interactive Stockodile shell.
"""

from __future__ import annotations

import os

os.environ["TYPER_USE_RICH"] = "0"

import asyncio
import time
from collections import deque
from pathlib import Path
from typing import Annotated, Any, cast

import typer

from stockodile.sink.base import Sink


def is_interactive_stdin() -> bool:
    import sys
    return sys.stdin.isatty() or getattr(sys.stdin, "_mock_interactive", False)


COMMON_DEFAULT_SYMBOLS = [
    "alpaca:AAPL",
    "alpaca:MSFT",
    "alpaca:GOOGL",
    "alpaca:AMZN",
    "alpaca:TSLA",
    "alpaca:NVDA",
]

VALID_PROVIDERS = ["alpaca", "finnhub", "google_finance", "msn_money", "stooq"]
VALID_CHANNELS = ["trade", "quote", "bar"]

SUGGESTED_SYMBOLS = {
    "alpaca": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"],
    "finnhub": ["AAPL", "MSFT", "GOOGL"],
    "google_finance": ["AAPL", "MSFT"],
    "msn_money": ["AAPL", "MSFT"],
    "stooq": ["AAPL", "MSFT"],
}


_console = None


def get_console():
    global _console
    if _console is None:
        from rich.console import Console
        _console = Console()
    return _console


def prompt_with_autocomplete(
    text: str,
    suggestions: list[str],
    default: str = "",
    meta_dict: dict[str, str] | None = None
) -> str:
    """Prompt the user for input, with autocomplete popup, history, and shadow suggestions."""
    import sys

    from prompt_toolkit import prompt
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
    from prompt_toolkit.completion import WordCompleter
    from prompt_toolkit.filters import has_completions
    from prompt_toolkit.key_binding import KeyBindings

    # If stdin is not interactive (e.g., tests/pipes) or running in pytest,
    # use fallback _prompt_with_esc
    if not is_interactive_stdin() or "pytest" in sys.modules:
        return typer.prompt(text, default=default)

    completer = WordCompleter(suggestions, ignore_case=True, meta_dict=meta_dict)
    
    kb = KeyBindings()
    @kb.add('escape', filter=~has_completions)
    def _(event):
        event.app.exit(exception=KeyboardInterrupt)

    prompt_text = text
    if default:
        prompt_text += f" [{default}]"
    prompt_text += ": "
    
    try:
        val = prompt(
            prompt_text,
            completer=completer,
            complete_while_typing=True,
            auto_suggest=AutoSuggestFromHistory(),
            key_bindings=kb,
        )
        val = val.strip()
        if not val and default:
            return default
        return val
    except (KeyboardInterrupt, EOFError):
        sys.stderr.write("\nCancelled.\n")
        sys.stderr.flush()
        raise typer.Exit(code=0) from None


def prompt_symbol(text: str, data_dir: Path, channel: str | None = None, default: str = "") -> str:
    """Prompt the user for a symbol using autocomplete suggestions from the database catalog."""
    from stockodile.store.catalog import Catalog
    
    suggestions = set()
    try:
        cat = Catalog(data_dir)
        channels = [channel] if channel else list(cat._registered_channels)
        for ch in channels:
            try:
                df = cat.query(f'SELECT DISTINCT symbol FROM "{ch}"')
                for s in df["symbol"].to_list():
                    if s:
                        suggestions.add(str(s))
            except Exception:
                pass
    except Exception:
        pass
        
    suggestions_list = sorted(list(suggestions))
    if not suggestions_list:
        suggestions_list = COMMON_DEFAULT_SYMBOLS
        
    return prompt_with_autocomplete(text, suggestions_list, default=default)


# ---------------------------------------------------------------------------
# Override typer.prompt to support cancellation with the ESC key
# ---------------------------------------------------------------------------
def _prompt_with_esc(
    text: str,
    default: Any = None,
    type: Any = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Prompt the user for input, allowing cancellation via ESC or Ctrl+C."""
    import sys

    # Try imports for Unix TTY
    try:
        import select
        import termios
        import tty
        has_unix_tty = sys.stdin.isatty()
    except ImportError:
        has_unix_tty = False

    # Try imports for Windows console
    try:
        import msvcrt
        has_win_con = True
    except ImportError:
        has_win_con = False

    def read_line() -> str:
        sys.stdout.write(text)
        if default is not None:
            sys.stdout.write(f" [{default}]")
        sys.stdout.write(": ")
        sys.stdout.flush()

        # Fallback if stdin is not a TTY (e.g., tests) or if we don't have tty/msvcrt support
        if not sys.stdin.isatty() or (not has_unix_tty and not has_win_con):
            line = sys.stdin.readline()
            if not line:
                raise KeyboardInterrupt
            line = line.rstrip("\r\n")
            if not line and default is not None:
                return str(default)
            return line

        if has_unix_tty:
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            line = ""
            try:
                tty.setraw(fd)
                while True:
                    ch = sys.stdin.read(1)
                    if not ch:
                        raise EOFError

                    if ch == "\x1b":
                        # Check if ESC or arrow key escape sequence
                        try:
                            r, _, _ = select.select([sys.stdin], [], [], 0.05)
                        except (ValueError, OSError):
                            r = []
                        if not r:
                            raise KeyboardInterrupt
                        else:
                            # Consume arrow keys
                            sys.stdin.read(1)
                            sys.stdin.read(1)
                            continue

                    if ch in ("\r", "\n"):
                        sys.stdout.write("\r\n")
                        sys.stdout.flush()
                        break

                    if ch in ("\x7f", "\x08"):
                        if len(line) > 0:
                            line = line[:-1]
                            sys.stdout.write("\b \b")
                            sys.stdout.flush()
                        continue

                    if ch == "\x03":
                        raise KeyboardInterrupt

                    sys.stdout.write(ch)
                    sys.stdout.flush()
                    line += ch
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

            if not line and default is not None:
                return str(default)
            return line

        elif has_win_con:
            line = ""
            while True:
                ch_bytes = msvcrt.getch()
                if ch_bytes == b'\x03':  # Ctrl+C
                    raise KeyboardInterrupt
                if ch_bytes == b'\x1b':  # ESC
                    raise KeyboardInterrupt
                if ch_bytes in (b'\r', b'\n'):
                    sys.stdout.write("\r\n")
                    sys.stdout.flush()
                    break
                if ch_bytes in (b'\x7f', b'\x08'):  # Backspace
                    if len(line) > 0:
                        line = line[:-1]
                        sys.stdout.write("\b \b")
                        sys.stdout.flush()
                    continue
                if ch_bytes in (b'\x00', b'\xe0'):  # Special key prefix (like arrow keys)
                    msvcrt.getch()  # consume the next byte
                    continue
                try:
                    ch = ch_bytes.decode('utf-8', errors='ignore')
                    if ch:
                        sys.stdout.write(ch)
                        sys.stdout.flush()
                        line += ch
                except Exception:
                    pass

            if not line and default is not None:
                return str(default)
            return line

        # Final fallback
        line = sys.stdin.readline()
        if not line:
            raise KeyboardInterrupt
        line = line.rstrip("\r\n")
        if not line and default is not None:
            return str(default)
        return line

    while True:
        try:
            val_str = read_line()
        except (KeyboardInterrupt, EOFError):
            # Print newline and Cancelled, then exit cleanly
            sys.stderr.write("\nCancelled.\n")
            sys.stderr.flush()
            raise typer.Exit(code=0) from None

        if not val_str and default is None:
            # Re-prompt if value is required and no default is provided
            continue

        if type is not None:
            try:
                return type(val_str)
            except ValueError:
                sys.stdout.write(f"Error: Invalid value of type {type.__name__}.\r\n")
                sys.stdout.flush()
                continue
        return val_str


cast(Any, typer).prompt = _prompt_with_esc


def prompt_time_range_helper(
    data_dir: Path,
    channel: str | None,
    symbols: list[str] | None,
    default_start: int = 0,
    default_end: int = 9999999999999999999
) -> tuple[int, int]:
    """Helper to show available time range from the database and prompt for Start/End times."""
    import datetime

    from stockodile.store.catalog import Catalog
    
    min_ts, max_ts = None, None
    if channel:
        cat = Catalog(data_dir)
        if channel in cat._registered_channels:
            try:
                where_clause = ""
                if symbols:
                    clean_syms = [s for s in symbols if s]
                    if clean_syms:
                        sym_list = ", ".join(f"'{s}'" for s in clean_syms)
                        where_clause = f" WHERE symbol IN ({sym_list})"
                df = cat.query(
                    f'SELECT min(local_ts) as min_t, max(local_ts) as max_t '
                    f'FROM "{channel}"{where_clause}'
                )
                if len(df) > 0:
                    row = df.to_dicts()[0]
                    if row.get("min_t") is not None:
                        min_ts = int(row["min_t"])
                    if row.get("max_t") is not None:
                        max_ts = int(row["max_t"])
            except Exception:
                pass

    if min_ts is not None and max_ts is not None:
        try:
            min_dt = datetime.datetime.fromtimestamp(
                min_ts // 1_000_000_000, tz=datetime.UTC
            )
            min_dt_str = min_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except (ValueError, OSError, OverflowError):
            min_dt_str = str(min_ts) if min_ts is not None else "unknown"
            
        try:
            max_dt = datetime.datetime.fromtimestamp(
                max_ts // 1_000_000_000, tz=datetime.UTC
            )
            max_dt_str = max_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except (ValueError, OSError, OverflowError):
            max_dt_str = str(max_ts) if max_ts is not None else "unknown"

        typer.echo(f"\nAvailable database range: {min_dt_str} to {max_dt_str}")
        start_prompt = "Start time (default: earliest)"
        end_prompt = "End time (default: latest)"
    else:
        typer.echo("\nNo data range found in catalog. Using absolute defaults.")
        start_prompt = "Start time (default: 0)"
        end_prompt = "End time (default: infinity)"

    instructions = (
        "\n--- Time Range Filter Instruction ---\n"
        "Start and End times filter the historical market data "
        "records retrieved from the database.\n"
        "Accepted input formats:\n"
        "  - UTC date-time string: 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DD HH:MM', or 'YYYY-MM-DD'\n"
        "  - Raw 19-digit UTC nanosecond timestamp (e.g., 1718540000000000000)\n"
        "  - Leave blank (press Enter) to use the default values shown below."
    )
    typer.echo(instructions)

    def parse_time(val: str, fallback: int) -> int:
        val = val.strip()
        if not val:
            return fallback
        if val.isdigit() and len(val) <= 19:
            try:
                return int(val)
            except ValueError:
                pass
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(val, fmt).replace(tzinfo=datetime.UTC)
                return int(dt.timestamp() * 1_000_000_000)
            except ValueError:
                continue
                
        fallback_str = str(fallback)
        if 0 < fallback < 9999999999999999999:
            try:
                dt = datetime.datetime.fromtimestamp(
                    fallback // 1_000_000_000, tz=datetime.UTC
                )
                fallback_str = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                pass
        elif fallback == 0:
            fallback_str = "earliest / 1970-01-01"
        elif fallback == 9999999999999999999:
            fallback_str = "latest / infinity"
            
        typer.echo(f"Warning: Invalid date format '{val}'. Using default: {fallback_str}", err=True)
        return fallback

    start_input = typer.prompt(start_prompt, default="").strip()
    resolved_start = parse_time(start_input, min_ts if min_ts is not None else default_start)
    
    end_input = typer.prompt(end_prompt, default="").strip()
    resolved_end = parse_time(end_input, max_ts if max_ts is not None else default_end)
    
    return resolved_start, resolved_end


app = typer.Typer(
    name="stockodile",
    help="Stockodile -- open-source US-equity market-data engine.",
    add_completion=False,
    no_args_is_help=True,
)


_DataDirOpt = Annotated[
    Path,
    typer.Option(
        "--data-dir",
        help="Root directory of the Parquet data lake.",
        show_default=False,
    ),
]


def resolve_data_dir(data_dir: Path) -> Path:
    """Resolve the data directory, falling back to test_data or prompting the user if empty."""
    import sys
    from pathlib import Path

    from stockodile.store.catalog import Catalog

    cwd_test_data = Path("test_data")
    repo_root = Path(__file__).resolve().parents[2]
    repo_test_data = repo_root / "test_data"
    home_test_data = Path.home() / "Stockodile" / "test_data"

    def has_data(d: Path) -> bool:
        if not d.exists() or not d.is_dir():
            return False
        try:
            cat = Catalog(d)
            return len(cat._registered_channels) > 0
           
        except Exception:
            return False

    if has_data(data_dir):
        return data_dir

    fallback_candidate = None
    if has_data(cwd_test_data):
        fallback_candidate = cwd_test_data
    elif has_data(repo_test_data):
        fallback_candidate = repo_test_data
    elif has_data(home_test_data):
        fallback_candidate = home_test_data

    if "pytest" in sys.modules:
        if fallback_candidate and (data_dir == Path("data") or not data_dir.exists()):
            return fallback_candidate
        return data_dir

    if not is_interactive_stdin():
        if fallback_candidate and (data_dir == Path("data") or not data_dir.exists()):
            typer.echo(
                f"Warning: No data found in '{data_dir}', "
                f"falling back to '{fallback_candidate}'.",
                err=True,
            )
            return fallback_candidate
        return data_dir

    if fallback_candidate and (data_dir == Path("data") or not data_dir.exists()):
        use_fallback = typer.confirm(
            f"No data found in '{data_dir}', but test data was "
            f"found at '{fallback_candidate}'. Use it?",
            default=True
        )
        if use_fallback:
            return fallback_candidate

    while True:
        alt_path = typer.prompt("Enter data directory", default=str(data_dir))
        alt_dir = Path(alt_path)
        if has_data(alt_dir):
            return alt_dir
        if not alt_dir.exists():
            typer.echo(f"Directory '{alt_dir}' does not exist.", err=True)
        else:
            typer.echo(f"No registered channels found in '{alt_dir}'.", err=True)
        
        if not typer.confirm("Try another path?", default=True):
            break
            
    return data_dir


def normalize_user_symbol(provider: str, symbol: str) -> str:
    """Normalize user input symbol to standard raw symbol format."""
    s = symbol.strip()
    if not s:
        return ""
    if ":" in s:
        parts = s.split(":", 1)
        if parts[0].lower() == provider.lower() or parts[0].lower() in VALID_PROVIDERS:
            s = parts[1]
    return s.upper()


def resolve_input_symbols(
    data_dir: Path,
    symbols_input: list[str],
    channels: list[str] | str | None = None,
) -> list[str]:
    """Resolve user entered symbols to matching catalog symbols if possible."""
    from stockodile.store.catalog import Catalog
    
    all_registered = None
    
    def get_registered():
        nonlocal all_registered
        if all_registered is not None:
            return all_registered
        all_registered = set()
        try:
            cat = Catalog(data_dir)
            target_channels = cat._registered_channels
            if channels:
                if isinstance(channels, str):
                    ch_list = [channels]
                else:
                    ch_list = list(channels)
                target_channels = [c for c in ch_list if c in target_channels]
                
            for ch in target_channels:
                try:
                    df = cat.query(f'SELECT DISTINCT symbol FROM "{ch}"')
                    for s in df["symbol"].to_list():
                        if s:
                            all_registered.add(str(s))
                except Exception:
                    pass
        except Exception:
            pass
        return all_registered

    def find_match(candidates_set: set[str], sym_clean: str) -> str | None:
        if sym_clean in candidates_set:
            return sym_clean
            
        candidates_list = sorted(list(candidates_set))
        lower_sym = sym_clean.lower()
        for reg in candidates_list:
            if reg.lower() == lower_sym:
                return reg
                
        for reg in candidates_list:
            if ":" in reg:
                parts = reg.split(":", 1)
                if parts[1].lower() == lower_sym:
                    return reg
                    
        matches = []
        for reg in candidates_list:
            if lower_sym in reg.lower():
                matches.append(reg)
        if len(matches) >= 1:
            return matches[0]
            
        return None

    resolved = []
    for sym in symbols_input:
        sym_clean = sym.strip()
        if not sym_clean:
            continue
            
        if ":" in sym_clean:
            parts = sym_clean.split(":", 1)
            exc = parts[0]
            raw = parts[1]
            normalized_raw = normalize_user_symbol(exc, raw)
            resolved.append(f"{exc}:{normalized_raw}")
            continue
            
        reg_symbols_set = get_registered()
        match = find_match(reg_symbols_set, sym_clean)
        if match is not None:
            resolved.append(match)
            continue
            
        match = find_match(set(COMMON_DEFAULT_SYMBOLS), sym_clean)
        if match is not None:
            resolved.append(match)
            continue
            
        guessed_exchange = "alpaca"
        normalized_raw = normalize_user_symbol(guessed_exchange, sym_clean)
        resolved.append(f"{guessed_exchange}:{normalized_raw}")
        
    return resolved


def select_symbols_interactively(
    data_dir: Path, channel: str | None = None
) -> tuple[str, list[str]]:
    """Select channel and symbol(s) interactively using a search/selection wizard."""
    from stockodile.store.catalog import Catalog

    cat = Catalog(data_dir)
    available_channels = sorted(list(cat._registered_channels))

    if not available_channels:
        return "", []

    # 1. Resolve channel if not specified
    if not channel:
        typer.echo("\n--- Select Channel ---")
        for idx, ch in enumerate(available_channels, 1):
            typer.echo(f"  [{idx}] {ch}")
        
        while True:
            choice = typer.prompt("Select channel", default="1").strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(available_channels):
                    channel = available_channels[idx]
                    break
                else:
                    typer.echo("Invalid selection. Try again.", err=True)
            elif choice:
                channel = choice
                break
            else:
                typer.echo("Channel name cannot be empty.", err=True)

    # 2. Query all unique symbols in this channel
    typer.echo(f"\nScanning symbol list for channel '{channel}'...")
    all_symbols = []
    if channel in cat._registered_channels:
        try:
            df = cat.query(f'SELECT DISTINCT symbol FROM "{channel}"')
            all_symbols = sorted([str(s) for s in df["symbol"].to_list() if s])
        except Exception:
            all_symbols = []

    if not all_symbols:
        typer.echo(f"No registered symbols found in channel '{channel}' on disk.", err=True)
        sym_input = prompt_symbol("Symbol (e.g. AAPL)", data_dir, channel=channel)
        symbols = [s.strip() for s in sym_input.split(",") if s.strip()]
        return channel, symbols

    # 3. Wizard loop
    search_query = ""
    while True:
        filtered = [s for s in all_symbols if search_query.lower() in s.lower()]
        
        typer.echo(f"\n--- Symbol Search (Filter: '{search_query}') ---")
        if not filtered:
            typer.echo("No matching symbols found.")
        else:
            display_limit = 15
            for idx, sym in enumerate(filtered[:display_limit], 1):
                typer.echo(f"  [{idx}] {sym}")
            if len(filtered) > display_limit:
                typer.echo(f"  ... and {len(filtered) - display_limit} more symbols ...")
                
        typer.echo("\nOptions:")
        typer.echo("  - Type number(s) (e.g. 1 or 1,2) to select symbol(s).")
        typer.echo("  - Type letters to search/filter.")
        typer.echo("  - Type 'all' to select all currently listed symbols.")
        typer.echo("  - Press Enter with empty query to clear search.")
        typer.echo("  - Press ESC to cancel.")
        
        choice = prompt_with_autocomplete("Search/Select", filtered, default="")
        
        if not choice:
            if search_query:
                search_query = ""
                continue
            else:
                typer.echo("Please make a selection or press ESC to cancel.", err=True)
                continue

        if choice.lower() == "all":
            if filtered:
                typer.echo(f"Selected all {len(filtered)} matching symbols: {filtered}")
                return channel, filtered
            else:
                typer.echo("No symbols to select.", err=True)
                continue

        if "," in choice or (choice.isdigit() and int(choice) > 0):
            parts = [p.strip() for p in choice.split(",")]
            selected = []
            valid = True
            for p in parts:
                if p.isdigit():
                    idx = int(p) - 1
                    if 0 <= idx < len(filtered) and idx < 15:
                        selected.append(filtered[idx])
                    else:
                        valid = False
                        typer.echo(f"Invalid index: {p}", err=True)
                else:
                    valid = False
            if valid and selected:
                typer.echo(f"Selected: {', '.join(selected)}")
                return channel, selected
            if not valid:
                continue

        search_query = choice


def select_collect_params_interactively(
    provider: str | None,
    symbols: list[str] | None,
    channels: list[str] | None
) -> tuple[str, list[str], list[str]]:
    """Select provider, channels, and symbols interactively for live data collection."""
    # 1. Select Provider
    if not provider:
        typer.echo("\n--- Supported Providers ---")
        for idx, pr in enumerate(VALID_PROVIDERS, 1):
            typer.echo(f"  [{idx}] {pr}")
        while True:
            choice = typer.prompt("Select provider", default="1").strip()
            if choice.isdigit():
                i = int(choice) - 1
                if 0 <= i < len(VALID_PROVIDERS):
                    provider = VALID_PROVIDERS[i]
                    break
            elif choice in VALID_PROVIDERS:
                provider = choice
                break
            typer.echo("Invalid selection. Try again.", err=True)

    # 2. Select Channels
    if not channels:
        typer.echo("\n--- Select Channels ---")
        for idx, ch in enumerate(VALID_CHANNELS, 1):
            typer.echo(f"  [{idx}] {ch}")
        typer.echo("  [C] Enter custom channel(s)")
        while True:
            choice = typer.prompt("Select channel(s)", default="1").strip()
            if not choice:
                typer.echo("Invalid selection. Try again.", err=True)
                continue
            if choice.lower() == "c":
                custom_input = typer.prompt("Enter channel(s), comma-separated").strip()
                custom_channels = [c.strip() for c in custom_input.split(",") if c.strip()]
                if custom_channels:
                    channels = custom_channels
                    break
            elif any(c.isdigit() for c in choice):
                parts = [p.strip() for p in choice.split(",")]
                selected = []
                valid = True
                for p in parts:
                    if p.isdigit():
                        idx = int(p) - 1
                        if 0 <= idx < len(VALID_CHANNELS):
                            selected.append(VALID_CHANNELS[idx])
                        else:
                            valid = False
                    else:
                        valid = False
                if valid and selected:
                    channels = selected
                    break
            else:
                input_channels = [c.strip() for c in choice.split(",") if c.strip()]
                if input_channels and all(ch in VALID_CHANNELS for ch in input_channels):
                    channels = input_channels
                    break
            typer.echo("Invalid selection. Try again.", err=True)

    # 3. Select Symbols
    if not symbols:
        suggestions = SUGGESTED_SYMBOLS.get(provider, ["AAPL"])
        typer.echo(f"\n--- Suggested Symbols for {provider} ---")
        for idx, sym in enumerate(suggestions, 1):
            typer.echo(f"  [{idx}] {sym}")
        typer.echo("  [C] Enter custom symbol(s)")
        
        while True:
            choice = typer.prompt("Select symbol(s)", default="1").strip()
            if not choice:
                typer.echo("Invalid selection. Try again.", err=True)
                continue
            if choice.lower() == "c":
                custom_input = prompt_with_autocomplete("Enter symbol (e.g. AAPL)", suggestions)
                custom_symbols = [s.strip() for s in custom_input.split(",") if s.strip()]
                if custom_symbols:
                    symbols = [normalize_user_symbol(provider, s) for s in custom_symbols]
                    break
            else:
                parts = [p.strip() for p in choice.split(",")]
                if parts and all(p.isdigit() and 0 <= int(p) - 1 < len(suggestions) for p in parts):
                    symbols = [suggestions[int(p) - 1] for p in parts]
                    break
                else:
                    custom_symbols = [s.strip() for s in choice.split(",") if s.strip()]
                    if custom_symbols:
                        symbols = [normalize_user_symbol(provider, s) for s in custom_symbols]
                        break
            typer.echo("Invalid selection. Try again.", err=True)

    return provider, symbols, channels


def get_record_value(rec: Any) -> float | None:
    struct_config = getattr(type(rec), "__struct_config__", None)
    tag = getattr(struct_config, "tag", None) if struct_config else None
    if not tag:
        if hasattr(rec, "price"):
            try:
                return float(rec.price)
            except Exception:
                pass
        if hasattr(rec, "close"):
            try:
                return float(rec.close)
            except Exception:
                pass
        return None

    try:
        if tag == "trade":
            return float(rec.price)
        elif tag == "quote":
            return (float(rec.bid_px) + float(rec.ask_px)) / 2.0
        elif tag == "bar":
            return float(rec.close)
    except Exception:
        pass
    return None


def format_record_value(channel: str, val: float) -> str:
    if val < 0.01:
        return f"{val:.6f}"
    return f"{val:,.4f}"


def make_sparkline(prices: list[float]) -> str:
    import math
    prices = [p for p in prices if p is not None and math.isfinite(p)]
    if not prices or len(prices) < 2:
        return ""
    min_p = min(prices)
    max_p = max(prices)
    diff = max_p - min_p
    if diff == 0:
        return "█" * len(prices)
    ticks = [" ", "▂", "▃", "▄", "▅", "▆", "▇", "█"]
    res = []
    for p in prices:
        ratio = (p - min_p) / diff
        idx = int(ratio * (len(ticks) - 1))
        idx = max(0, min(len(ticks) - 1, idx))
        res.append(ticks[idx])
    return "".join(res)

class MonitoringSink(Sink):
    def __init__(self, target: Sink):
        self.target = target
        self.total_records = 0
        self.records_by_key = {}  # (symbol, channel) -> count
        self.values_by_key = {}  # (symbol, channel) -> deque of last values
        self.start_time = time.time()
        self.last_ts_by_key = {}  # (symbol, channel) -> float
        self.last_rec_by_key = {}  # (symbol, channel) -> record
        self.rates_deque = deque(maxlen=10)
        self.last_rate_calc_time = time.time()
        self.records_since_last_calc = 0
        self.current_rate = 0.0

    async def put(self, record: Any) -> None:
        self.total_records += 1
        self.records_since_last_calc += 1
        
        now = time.time()
        if now - self.last_rate_calc_time >= 1.0:
            elapsed = now - self.last_rate_calc_time
            self.current_rate = self.records_since_last_calc / elapsed
            self.rates_deque.append(self.current_rate)
            self.records_since_last_calc = 0
            self.last_rate_calc_time = now

        struct_config = getattr(type(record), "__struct_config__", None)
        channel = (
            getattr(struct_config, "tag", None) if struct_config else None
        ) or getattr(record, "channel", "unknown")
        key = (record.symbol, channel)
        self.records_by_key[key] = self.records_by_key.get(key, 0) + 1
        self.last_ts_by_key[key] = now
        self.last_rec_by_key[key] = record

        val = get_record_value(record)
        if val is not None:
            if key not in self.values_by_key:
                self.values_by_key[key] = deque(maxlen=30)
            self.values_by_key[key].append(val)

        await self.target.put(record)

    async def flush(self) -> None:
        await self.target.flush()

    async def close(self) -> None:
        await self.target.close()


async def run_dashboard(
    monitoring_sink: MonitoringSink,
    provider: str,
    symbols: list[str],
    channels: list[str],
    data_dir: Path,
):
    from rich.align import Align
    from rich.columns import Columns
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    
    console = Console()
    
    def format_elapsed(secs: int) -> str:
        h = secs // 3600
        m = (secs % 3600) // 60
        s = secs % 60
        if h > 0:
            return f"{h:02d}h {m:02d}m {s:02d}s"
        return f"{m:02d}m {s:02d}s"
        
    def generate_layout() -> Panel:
        now = time.time()
        elapsed = int(now - monitoring_sink.start_time)
        
        # Title & Subtitle
        title_text = Text()
        title_text.append("STOCKODILE LIVE DATA INGESTION PIPELINE\n", style="bold cyan")
        title_text.append(
            "Real-time streaming US-equity market data to local Parquet storage\n",
            style="dim white",
        )
        
        # Pipeline Configuration Panel
        config_table = Table.grid(padding=(0, 2))
        config_table.add_column("Property", style="bold cyan")
        config_table.add_column("Value", style="white")
        config_table.add_row("Provider source", f"[bold magenta]{provider.upper()}[/]")
        config_table.add_row("Data destination", f"[bold blue]{data_dir.resolve()}[/]")
        config_table.add_row("Subscribed channels", f"[bold yellow]{', '.join(channels)}[/]")
        config_table.add_row("Session duration", format_elapsed(elapsed))
        
        # Ingestion Speed and Health calculation
        rate = monitoring_sink.current_rate
        if not monitoring_sink.rates_deque and elapsed > 0:
            rate = monitoring_sink.total_records / elapsed
            
        time_since_last_msg = 999.0
        if monitoring_sink.last_ts_by_key:
            time_since_last_msg = now - max(monitoring_sink.last_ts_by_key.values())
            
        if time_since_last_msg > 1.0:
            rate = 0.0
            
        if time_since_last_msg > 10.0:
            status_text = "[bold blink red]STALE (Waiting for data...)[/]"
            status_border = "red"
        else:
            status_text = "[bold cyan]● PIPELINE ACTIVE & STREAMING[/]"
            status_border = "cyan"
            
        buffered_count = 0
        if hasattr(monitoring_sink.target, "_buffers"):
            buffered_count = sum(len(buf) for buf in monitoring_sink.target._buffers.values())
            
        # Stats & Performance Panel
        perf_table = Table.grid(padding=(0, 2))
        perf_table.add_column("Metric", style="bold cyan")
        perf_table.add_column("Value", style="white")
        perf_table.add_row("Pipeline status", status_text)
        perf_table.add_row(
            "Total records saved", f"[bold green]{monitoring_sink.total_records:,}[/]"
        )
        perf_table.add_row("Ingestion speed", f"[bold green]{rate:.1f} records/sec[/]")
        perf_table.add_row("Write-buffer queue", f"[bold red]{buffered_count:,} rows[/]")
        
        # Arrange panels side-by-side
        left_panel = Panel(
            config_table, title="[bold white]Pipeline Config[/]", border_style="cyan"
        )
        right_panel = Panel(
            perf_table,
            title="[bold white]System Performance[/]",
            border_style=status_border,
        )
        
        header_columns = Columns([left_panel, right_panel], expand=True)
        
        # Active Data Streams Table
        stats_table = Table(
            title="[bold white]Active Market Data Streams[/]",
            border_style="cyan",
            expand=True,
        )
        stats_table.add_column("Asset/Symbol", style="bold cyan", ratio=2)
        stats_table.add_column("Data Type (Channel)", style="bold yellow", ratio=2)
        stats_table.add_column("Messages Ingested", justify="right", style="green", ratio=2)
        stats_table.add_column("Latest Value/Price", justify="right", style="magenta", ratio=2)
        stats_table.add_column("Trend (since start)", justify="center", ratio=2)
        stats_table.add_column("Activity Chart (last 30 ticks)", justify="center", ratio=3)
        
        for (sym, ch), count in sorted(monitoring_sink.records_by_key.items()):
            key = (sym, ch)
            last_val_str = "-"
            trend_str = "-"
            sparkline = ""
            
            values_deque = monitoring_sink.values_by_key.get(key)
            if values_deque and len(values_deque) > 0:
                last_val = values_deque[-1]
                last_val_str = format_record_value(ch, last_val)
                
                if len(values_deque) >= 2:
                    values_deque[-2]
                    first_val = values_deque[0]
                    pct_change = ((last_val - first_val) / first_val) * 100.0 if first_val else 0.0
                    
                    if last_val > first_val:
                        trend_str = f"[bold green]▲ (+{pct_change:.2f}%)[/]"
                    elif last_val < first_val:
                        trend_str = f"[bold red]▼ ({pct_change:.2f}%)[/]"
                    else:
                        trend_str = "[bold grey]▶ (0.00%)[/]"
                else:
                    trend_str = "[bold grey]▶ (0.00%)[/]"
                
                sparkline = make_sparkline(list(values_deque))
                
            stats_table.add_row(sym, ch, f"{count:,}", last_val_str, trend_str, sparkline)
            
        footer_text = Text(
            "\nTo view and query your historical local data, "
            "open a new terminal window and run: ",
            style="dim",
        )
        query_sql = f"SELECT * FROM {channels[0] if channels else 'trade'}"
        footer_text.append(f'stockodile query "{query_sql}"', style="bold yellow")
        footer_text.append("\nPress ")
        footer_text.append("Ctrl-C", style="bold red")
        footer_text.append(
            " at any time to safely stop the ingestion pipeline.", style="dim"
        )
        
        group = Group(
            Align.center(title_text),
            header_columns,
            stats_table,
            footer_text
        )
        return Panel(group, border_style="cyan", expand=True)

    with Live(generate_layout(), console=console, refresh_per_second=2) as live:
        while True:
            try:
                await asyncio.sleep(0.5)
                live.update(generate_layout())
            except asyncio.CancelledError:
                break
            except Exception:
                pass


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------
@app.command()
def query(
    sql: Annotated[str, typer.Argument(help="DuckDB SQL query to execute.")] = "",
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """Execute a DuckDB SQL query against the data lake and print the result."""
    from stockodile.client.client import StockodileClient

    data_dir = resolve_data_dir(data_dir)

    if not sql:
        if is_interactive_stdin():
            sql = typer.prompt("SQL query")
        else:
            import sys
            sql = sys.stdin.read().strip()
            if not sql:
                typer.echo("Error: SQL query is required and stdin is empty.", err=True)
                raise typer.Exit(code=1)

    if not sql:
        typer.echo("Error: SQL query cannot be empty.", err=True)
        raise typer.Exit(code=1)

    client = StockodileClient(data_dir=data_dir)
    try:
        df = client.query(sql)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e
    typer.echo(df)


# ---------------------------------------------------------------------------
# catalog
# ---------------------------------------------------------------------------
@app.command()
def catalog(
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """List channels present in the data lake with their row counts."""
    from stockodile.client.client import StockodileClient
    from stockodile.store.catalog import Catalog

    data_dir = resolve_data_dir(data_dir)
    cat: Catalog = StockodileClient(data_dir=data_dir)._catalog
    channels: list[str] = sorted(cat._registered_channels)

    if not channels:
        typer.echo("No data found in: " + str(data_dir))
        raise typer.Exit(code=0)

    typer.echo(f"{'channel':<24}  {'rows':>10}")
    typer.echo("-" * 36)
    for ch in channels:
        try:
            row_df = cat.query(f'SELECT count(*) AS n FROM "{ch}"')
            n = int(row_df["n"][0])
        except Exception:
            n = -1
        typer.echo(f"{ch:<24}  {n:>10,}")


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------
@app.command()
def export(
    channel: Annotated[
        str | None,
        typer.Option("--channel", help="Channel name, e.g. trade."),
    ] = None,
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbols", help="Canonical symbol(s). Repeat for multiple."),
    ] = None,
    frm: Annotated[
        int | None,
        typer.Option("--from", help="Start of time range (nanoseconds UTC)."),
    ] = None,
    to: Annotated[
        int | None,
        typer.Option("--to", help="End of time range (nanoseconds UTC)."),
    ] = None,
    fmt: Annotated[
        str,
        typer.Option("--fmt", help="Output format: parquet|csv|arrow|json|jsonl."),
    ] = "parquet",
    dest: Annotated[
        Path,
        typer.Option("--dest", help="Destination file path."),
    ] = Path("export.parquet"),
    data_dir: _DataDirOpt = Path("data"),
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Maximum number of rows to export."),
    ] = None,
) -> None:
    """Export channel x symbols x time range to a file."""
    from stockodile.client.client import StockodileClient

    data_dir = resolve_data_dir(data_dir)
    if dest == Path("export.parquet") and fmt != "parquet":
        dest = Path(f"export.{fmt}")

    if not is_interactive_stdin():
        if not channel or not symbols:
            typer.echo("Error: channel and symbols are required in non-interactive mode.", err=True)
            raise typer.Exit(code=1)
        if frm is None:
            frm = 0
        if to is None:
            to = 9999999999999999999
    else:
        # Interactive
        if not channel or not symbols:
            channel, selected_symbols = select_symbols_interactively(data_dir, channel)
            if selected_symbols:
                symbols = selected_symbols

        if not channel:
            channel = typer.prompt("Channel (e.g. trade)")
        if not symbols:
            sym_input = prompt_symbol("Symbol (e.g. AAPL)", data_dir, channel=channel)
            symbols = [s.strip() for s in sym_input.split(",") if s.strip()]
        if frm is None or to is None:
            resolved_start, resolved_end = prompt_time_range_helper(
                data_dir,
                channel,
                symbols,
                default_start=0,
                default_end=9999999999999999999,
            )
            if frm is None:
                frm = resolved_start
            if to is None:
                to = resolved_end

    if symbols:
        symbols = resolve_input_symbols(data_dir, symbols, channel)

    if not channel or not symbols:
        typer.echo("Error: Channel and symbols are required.", err=True)
        raise typer.Exit(code=1)

    client = StockodileClient(data_dir=data_dir)
    try:
        client.export(
            channel,
            symbols,
            frm,  # type: ignore[arg-type]
            to,
            fmt=fmt,  # type: ignore[arg-type]
            dest=dest,
            limit=limit,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e
    typer.echo(f"Exported to: {dest}")


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------
@app.command()
def replay(
    channels: Annotated[
        list[str] | None,
        typer.Option("--channels", help="Channel name(s). Repeat for multiple."),
    ] = None,
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbols", help="Canonical symbol(s). Repeat for multiple."),
    ] = None,
    frm: Annotated[
        int | None,
        typer.Option("--from", help="Start of time range (nanoseconds UTC)."),
    ] = None,
    to: Annotated[
        int | None,
        typer.Option("--to", help="End of time range (nanoseconds UTC)."),
    ] = None,
    data_dir: _DataDirOpt = Path("data"),
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Maximum number of records to print."),
    ] = None,
) -> None:
    """Stream canonical Records from the data lake, printed to stdout."""
    from stockodile.client.client import StockodileClient

    data_dir = resolve_data_dir(data_dir)

    if not is_interactive_stdin():
        if not channels or not symbols:
            typer.echo(
                "Error: channels and symbols are required in non-interactive mode.",
                err=True,
            )
            raise typer.Exit(code=1)
        if frm is None:
            frm = 0
        if to is None:
            to = 9999999999999999999
    else:
        # Interactive
        if not channels or not symbols:
            ch, selected_symbols = select_symbols_interactively(
                data_dir,
                channels[0] if channels else None,
            )
            if ch:
                channels = [ch]
            if selected_symbols:
                symbols = selected_symbols
        if not channels:
            ch_input = typer.prompt("Channel(s) (e.g. trade)")
            channels = [c.strip() for c in ch_input.split(",") if c.strip()]
        if not symbols:
            sym_input = prompt_symbol(
                "Symbol (e.g. AAPL)",
                data_dir,
                channel=channels[0] if channels else None,
            )
            symbols = [s.strip() for s in sym_input.split(",") if s.strip()]
        if frm is None or to is None:
            resolved_start, resolved_end = prompt_time_range_helper(
                data_dir,
                channels[0] if channels else None,
                symbols,
                default_start=0,
                default_end=9999999999999999999,
            )
            if frm is None:
                frm = resolved_start
            if to is None:
                to = resolved_end

    if symbols:
        symbols = resolve_input_symbols(data_dir, symbols, channels[0] if channels else None)

    client = StockodileClient(data_dir=data_dir)
    count = 0
    try:
        for record in client.replay(channels, symbols, frm, to):
            typer.echo(repr(record))
            count += 1
            if limit is not None and count >= limit:
                break
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e
    
    get_console().print(f"[bold cyan]-- {count} record(s) replayed.[/bold cyan]")


# ---------------------------------------------------------------------------
# collect
# ---------------------------------------------------------------------------
@app.command()
def collect(
    provider: Annotated[
        str | None,
        typer.Option("--provider", help="Provider name, e.g. alpaca."),
    ] = None,
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbols", help="Symbol(s) to collect. Repeat for multiple."),
    ] = None,
    channels: Annotated[
        list[str] | None,
        typer.Option("--channels", help="Channel(s) to subscribe. Repeat for multiple."),
    ] = None,
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """Collect live market data from a provider and write to the Parquet data lake."""
    from stockodile.client.collect import collect as collect_live
    from stockodile.ingest.transport import AiohttpWsTransport
    from stockodile.providers.factory import make_provider
    from stockodile.reference.registry import InstrumentRegistry
    from stockodile.store.parquet_sink import ParquetSink

    if not is_interactive_stdin():
        if not provider or not symbols or not channels:
            typer.echo(
                "Error: provider, symbols, and channels are required in non-interactive mode.",
                err=True,
            )
            raise typer.Exit(code=1)
    else:
        # Interactive
        if not provider or not symbols or not channels:
            provider, symbols, channels = select_collect_params_interactively(
                provider,
                symbols,
                channels,
            )

        if not provider:
            provider = typer.prompt("Provider (e.g. alpaca)")
        if not symbols:
            sym_input = prompt_symbol("Symbol (e.g. AAPL)", data_dir)
            symbols = [s.strip() for s in sym_input.split(",") if s.strip()]
        if not channels:
            ch_input = typer.prompt("Channel (e.g. trade)")
            channels = [c.strip() for c in ch_input.split(",") if c.strip()]

    if symbols and provider:
        symbols = [normalize_user_symbol(provider, s) for s in symbols]

    if not provider or not symbols or not channels:
        typer.echo("Error: Provider, symbols, and channels are required.", err=True)
        raise typer.Exit(code=1)

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
        get_console().print(f"[bold red]Error:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    if conn.transport is None:
        conn.transport = AiohttpWsTransport(conn.ws_url)

    typer.echo(
        f"Starting collection: provider={provider!r} symbols={symbols} "
        f"channels={channels} data_dir={data_dir}"
    )

    monitoring_sink = MonitoringSink(sink)
    conn.out = monitoring_sink

    if is_interactive_stdin():
        async def collect_with_dashboard():
            dashboard_task = asyncio.create_task(
                run_dashboard(monitoring_sink, provider, symbols, channels, data_dir)
            )
            try:
                await collect_live([conn], monitoring_sink)
            finally:
                dashboard_task.cancel()
                try:
                    await dashboard_task
                except asyncio.CancelledError:
                    pass

        try:
            asyncio.run(collect_with_dashboard())
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
    else:
        try:
            asyncio.run(collect_live([conn], monitoring_sink))
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass

    get_console().print(f"[bold cyan]Collection stopped.[/bold cyan] Data written to: {data_dir}")


# ---------------------------------------------------------------------------
# resample
# ---------------------------------------------------------------------------
@app.command()
def resample(
    symbol: Annotated[
        str | None,
        typer.Option("--symbol", help="Canonical symbol, e.g. alpaca:AAPL."),
    ] = None,
    interval: Annotated[
        str | None,
        typer.Option("--interval", help="Resampling interval (e.g. 1m, 1h, 1d)."),
    ] = None,
    frm: Annotated[
        int | None,
        typer.Option("--from", help="Start of time range (nanoseconds UTC)."),
    ] = None,
    to: Annotated[
        int | None,
        typer.Option("--to", help="End of time range (nanoseconds UTC)."),
    ] = None,
    fill: Annotated[
        bool,
        typer.Option("--fill", help="Fill empty periods with last known close."),
    ] = False,
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """Resample trade data in the lake into OHLCV bars."""
    from stockodile.client.client import StockodileClient

    data_dir = resolve_data_dir(data_dir)

    if not is_interactive_stdin():
        if not symbol or not interval:
            typer.echo("Error: symbol and interval are required in non-interactive mode.", err=True)
            raise typer.Exit(code=1)
        if frm is None:
            frm = 0
        if to is None:
            to = 9999999999999999999
    else:
        # Interactive
        if not symbol:
            symbol = prompt_symbol("Symbol (e.g. AAPL)", data_dir, channel="trade")
        if not interval:
            interval = typer.prompt("Interval (e.g. 1m, 1h, 1d)", default="1m")
        if frm is None or to is None:
            resolved_start, resolved_end = prompt_time_range_helper(
                data_dir,
                "trade",
                [symbol],
                default_start=0,
                default_end=9999999999999999999,
            )
            if frm is None:
                frm = resolved_start
            if to is None:
                to = resolved_end

    client = StockodileClient(data_dir=data_dir)
    try:
        df = client.resample(symbol, frm, to, interval, fill_empty=fill)
        typer.echo(df)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e


# ---------------------------------------------------------------------------
# indicators
# ---------------------------------------------------------------------------
@app.command()
def indicators(
    symbol: Annotated[
        str | None,
        typer.Option("--symbol", help="Canonical symbol, e.g. alpaca:AAPL."),
    ] = None,
    indicator: Annotated[
        str | None,
        typer.Option(
            "--indicator",
            help="Indicator to calculate (sma, ema, rsi, macd, bb, or all).",
        ),
    ] = None,
    period: Annotated[
        int,
        typer.Option(
            "--period",
            help="Smoothing/lookback window size (used for SMA, EMA, RSI, BB).",
        ),
    ] = 14,
    frm: Annotated[
        int | None,
        typer.Option("--from", help="Start of time range (nanoseconds UTC)."),
    ] = None,
    to: Annotated[
        int | None,
        typer.Option("--to", help="End of time range (nanoseconds UTC)."),
    ] = None,
    interval: Annotated[
        str,
        typer.Option("--interval", help="Resampling interval (e.g. 1m, 1h, 1d)."),
    ] = "1d",
    fill_empty: Annotated[
        bool,
        typer.Option(
            "--fill-empty",
            help="Fill empty periods in the resampled grid (can explode wide date ranges).",
        ),
    ] = False,
    data_dir: _DataDirOpt = Path("data"),
) -> None:
    """Calculate technical analysis indicators (SMA, EMA, RSI, MACD, BB) using Polars."""
    from stockodile.analytics import (
        calculate_bollinger_bands,
        calculate_ema,
        calculate_macd,
        calculate_rsi,
        calculate_sma,
    )
    from stockodile.client.client import StockodileClient

    data_dir = resolve_data_dir(data_dir)

    if not is_interactive_stdin():
        if not symbol:
            typer.echo("Error: symbol is required in non-interactive mode.", err=True)
            raise typer.Exit(code=1)
        if frm is None:
            frm = 0
        if to is None:
            to = 9999999999999999999
    else:
        # Interactive
        if not symbol:
            symbol = prompt_symbol("Symbol (e.g. AAPL)", data_dir, channel="trade")
        if frm is None or to is None:
            resolved_start, resolved_end = prompt_time_range_helper(
                data_dir,
                "trade",
                [symbol],
                default_start=0,
                default_end=9999999999999999999,
            )
            if frm is None:
                frm = resolved_start
            if to is None:
                to = resolved_end

    client = StockodileClient(data_dir=data_dir)
    try:
        df = client.resample(symbol, frm, to, interval, fill_empty=fill_empty)
        if len(df) == 0:
            typer.echo("No data found for the given symbol and time range.")
            return

        # Sort by bar timestamp to ensure calculations are correct
        df = df.sort("bar")
        close_series = df["close"]

        if indicator == "sma":
            res = df.with_columns(calculate_sma(close_series, period).alias("sma"))
        elif indicator == "ema":
            res = df.with_columns(calculate_ema(close_series, period).alias("ema"))
        elif indicator == "rsi":
            res = df.with_columns(calculate_rsi(close_series, period).alias("rsi"))
        elif indicator == "macd":
            macd, signal, hist = calculate_macd(close_series)
            res = df.with_columns(
                macd.alias("macd"),
                signal.alias("signal"),
                hist.alias("hist")
            )
        elif indicator == "bb":
            upper, middle, lower = calculate_bollinger_bands(close_series, period=period)
            res = df.with_columns(
                upper.alias("bb_upper"),
                middle.alias("bb_middle"),
                lower.alias("bb_lower")
            )
        elif not indicator or indicator == "all":
            # calculate all indicators and append
            macd, signal, hist = calculate_macd(close_series)
            upper, middle, lower = calculate_bollinger_bands(close_series, period=period)
            res = df.with_columns(
                calculate_sma(close_series, period).alias("sma"),
                calculate_ema(close_series, period).alias("ema"),
                calculate_rsi(close_series, period).alias("rsi"),
                macd.alias("macd"),
                signal.alias("signal"),
                hist.alias("hist"),
                upper.alias("bb_upper"),
                middle.alias("bb_middle"),
                lower.alias("bb_lower")
            )
        else:
            typer.echo(f"Error: Unknown indicator '{indicator}'", err=True)
            raise typer.Exit(code=1)

        typer.echo(res)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e


# ---------------------------------------------------------------------------
# shell
# ---------------------------------------------------------------------------
@app.command()
def shell() -> None:
    """Start an interactive Stockodile shell."""
    from stockodile import __version__
    typer.echo(f"Welcome to Stockodile Interactive Shell! (v{__version__})")
    typer.echo("Type 'help' to list commands. Type 'exit' or 'quit' to exit.")
    
    import sys
    if sys.stderr.isatty():
        sys.stderr.write("Loading autocomplete and history...\n")
        sys.stderr.flush()

    import shlex

    import click
    from prompt_toolkit import PromptSession
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory, Suggestion
    from prompt_toolkit.completion import WordCompleter
    from prompt_toolkit.history import InMemoryHistory
    
    class CommandAndHistoryAutoSuggest(AutoSuggestFromHistory):
        def __init__(self, commands):
            super().__init__()
            self.commands = commands

        def get_suggestion(self, buffer, document):
            text = document.text
            if not text.strip():
                return None
            parts = text.split()
            if len(parts) == 1 and not text.endswith(' '):
                for cmd in self.commands:
                    if cmd.lower().startswith(text.lower()) and len(cmd) > len(text):
                        return Suggestion(cmd[len(text):])
            return super().get_suggestion(buffer, document)

    click_group = typer.main.get_group(app)
    
    commands = {}
    for name in click_group.list_commands(None):
        cmd = click_group.get_command(None, name)
        help_text = cmd.help or ""
        if help_text:
            help_text = help_text.split("\n")[0].strip()
        commands[name] = help_text
        
    import sys
    is_pytest = "pytest" in sys.modules
    is_interactive = is_interactive_stdin()
    session = None
    if is_interactive and not is_pytest:
        session = PromptSession(
            history=InMemoryHistory(),
            auto_suggest=CommandAndHistoryAutoSuggest(
                [*list(commands.keys()), "exit", "quit", "help"]
            ),
            completer=WordCompleter(
                words=[*list(commands.keys()), "exit", "quit", "help"],
                meta_dict={
                    **commands,
                    "exit": "Exit the shell",
                    "quit": "Exit the shell",
                    "help": "Show help",
                },
                ignore_case=True,
            ),
            complete_while_typing=True
        )
    
    import signal
    original_handler = None
    handler_installed = False
    if is_interactive and not is_pytest:
        try:
            original_handler = signal.getsignal(signal.SIGWINCH)
        except Exception:
            pass

        def sigwinch_handler(signum, frame):
            if original_handler and callable(original_handler):
                try:
                    original_handler(signum, frame)
                except Exception:
                    pass
            try:
                from prompt_toolkit.application import get_app_or_none
                app = get_app_or_none()
            except Exception:
                app = None
            if app and app.renderer:
                try:
                    app.renderer.reset(leave_alternate_screen=False)
                    app.invalidate()
                except Exception:
                    pass

        try:
            signal.signal(signal.SIGWINCH, sigwinch_handler)
            handler_installed = True
        except Exception:
            pass

    try:
        while True:
            try:
                if not is_interactive or is_pytest:
                    line = input("stockodile> ").strip()
                else:
                    line = session.prompt("stockodile> ", complete_while_typing=True).strip()
                if not line:
                    continue
                if line.lower() in ("exit", "quit"):
                    break
                if line.lower() == "shell":
                    typer.echo("You are already in the Stockodile shell.")
                    continue
                
                if line.lower() in ("help", "?", "-h"):
                    args = ["--help"]
                else:
                    args = shlex.split(line)
                try:
                    click_group(args, standalone_mode=False)
                except click.exceptions.ClickException as e:
                    e.show()
                except click.exceptions.Exit:
                    pass
                except SystemExit:
                    pass
                except Exception as e:
                    typer.echo(f"Error executing command: {e}", err=True)
            except (KeyboardInterrupt, EOFError):
                typer.echo("\nGoodbye!")
                break
    finally:
        if handler_installed:
            try:
                signal.signal(signal.SIGWINCH, original_handler)
            except Exception:
                pass



# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
LOGO_ART = r"""     _             _             _ _ _      
 ___| |_ ___   ___| | _____   __| (_) | ___ 
/ __| __/ _ \ / __| |/ / _ \ / _` | | |/ _ \
\__ \ || (_) | (__|   < (_) | (_| | | |  __/
|___/\__\___/ \___|_|\_\___/ \__,_|_|_|\___|"""

LOGO = f"\033[36m{LOGO_ART}\033[0m"


def main() -> None:
    """Entry-point called by the ``stockodile`` script."""
    import sys

    from stockodile import __version__

    # Defer loading of rich styling unless help is requested
    if any(arg in sys.argv for arg in ("--help", "-h")):
        try:
            import typer.rich_utils
            typer.rich_utils.STYLE_OPTION = "bold cyan"
            typer.rich_utils.STYLE_COMMANDS_PANEL_BORDER = "cyan"
            typer.rich_utils.STYLE_OPTIONS_PANEL_BORDER = "cyan"
            typer.rich_utils.STYLE_COMMANDS_TABLE_FIRST_COLUMN = "bold cyan"
        except Exception:
            pass

    # Print the logo always to stderr, unless running the mcp command or tests
    if "mcp" not in sys.argv and "pytest" not in sys.modules:
        if sys.stderr.isatty():
            sys.stderr.write(LOGO + f"\n             (v{__version__})\n\n")
            sys.stderr.flush()

    if len(sys.argv) == 1:
        sys.argv.append("shell")

    app()


if __name__ == "__main__":
    main()
