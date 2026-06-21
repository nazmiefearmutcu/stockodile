import time
from datetime import UTC, datetime


def ms_to_ns(ms: int | float) -> int:
    return int(ms) * 1_000_000


def us_to_ns(us: int | float) -> int:
    return int(us) * 1_000


def now_ns() -> int:
    """Capture clock for local_ts. Realtime so it's comparable to source_ts."""
    return time.clock_gettime_ns(time.CLOCK_REALTIME)


def rfc3339_to_ns(dt_str: str) -> int:
    """Parse RFC-3339 timestamp with arbitrary subsecond precision to nanosecond timestamp."""
    # Handle timezone offset if any, Alpaca uses Z
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1]
    
    if "." in dt_str:
        base, frac = dt_str.split(".", 1)
        frac = frac[:9].ljust(9, "0")
        subseconds_ns = int(frac)
    else:
        base = dt_str
        subseconds_ns = 0
    
    dt = datetime.fromisoformat(base)
    secs = int(dt.replace(tzinfo=UTC).timestamp())
    return secs * 1_000_000_000 + subseconds_ns

