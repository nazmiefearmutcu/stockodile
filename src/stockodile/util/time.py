import time
from datetime import UTC, datetime


def ms_to_ns(ms: int | float) -> int:
    return int(ms * 1_000_000)


def us_to_ns(us: int | float) -> int:
    return int(us * 1_000)


def now_ns() -> int:
    """Capture clock for local_ts. Realtime so it's comparable to source_ts."""
    return time.clock_gettime_ns(time.CLOCK_REALTIME)


def rfc3339_to_ns(dt_str: str) -> int:
    """Parse RFC-3339 timestamp with arbitrary subsecond precision to nanosecond timestamp."""
    offset_str = "+00:00"
    dt_part = dt_str

    if dt_str.endswith("Z"):
        offset_str = "+00:00"
        dt_part = dt_str[:-1]
    else:
        t_idx = dt_str.find("T")
        if t_idx != -1:
            plus_idx = dt_str.rfind("+", t_idx)
            minus_idx = dt_str.rfind("-", t_idx)
            idx = max(plus_idx, minus_idx)
            if idx != -1:
                offset_str = dt_str[idx:]
                dt_part = dt_str[:idx]

    if "." in dt_part:
        base, frac = dt_part.split(".", 1)
        frac = frac[:9].ljust(9, "0")
        subseconds_ns = int(frac)
    else:
        base = dt_part
        subseconds_ns = 0

    dt_with_offset = datetime.fromisoformat(f"{base}{offset_str}")
    secs = int(dt_with_offset.astimezone(UTC).timestamp())
    return secs * 1_000_000_000 + subseconds_ns
