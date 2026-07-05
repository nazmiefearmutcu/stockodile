"""OHLCV resampling from trades, quotes, or lower-resolution bars."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

import duckdb
import polars as pl

from stockodile.resample._interval import parse_interval as _parse_interval
from stockodile.schema.records import Bar, Quote, Trade
from stockodile.store.catalog import Catalog

# ---------------------------------------------------------------------------
# DuckDB Catalog Resampling
# ---------------------------------------------------------------------------


def _build_no_fill_sql(interval_sql: str, interval_label: str) -> str:
    """Return the aggregation SQL for non-empty bars only."""
    return (
        "SELECT\n"
        f"    epoch_ns(time_bucket({interval_sql}, make_timestamp(local_ts // 1000))) AS bar,\n"
        "    symbol,\n"
        f"    '{interval_label}' AS interval,\n"
        "    first(price ORDER BY local_ts)          AS open,\n"
        "    max(price)                              AS high,\n"
        "    min(price)                              AS low,\n"
        "    last(price ORDER BY local_ts)           AS close,\n"
        "    sum(size)                               AS volume,\n"
        "    (sum(price * size) / sum(size))         AS vwap,\n"
        "    count(*)::BIGINT                        AS trade_count\n"
        "FROM trade\n"
        "WHERE symbol = ?\n"
        "  AND local_ts >= ?\n"
        "  AND local_ts <= ?\n"
        "GROUP BY 1, 2, 3\n"
        "ORDER BY 1"
    )


def _build_fill_sql(interval_sql: str, interval_label: str, start_ns: int, end_ns: int) -> str:
    """Return the fill-enabled SQL."""
    return (
        "WITH\n"
        "agg AS (\n"
        "    SELECT\n"
        f"        time_bucket({interval_sql}, make_timestamp(local_ts // 1000)) AS bar_ts,\n"
        "        symbol,\n"
        "        first(price ORDER BY local_ts)          AS open,\n"
        "        max(price)                              AS high,\n"
        "        min(price)                              AS low,\n"
        "        last(price ORDER BY local_ts)           AS close,\n"
        "        sum(size)                               AS volume,\n"
        "        (sum(price * size) / sum(size))         AS vwap,\n"
        "        count(*)::BIGINT                        AS trade_count\n"
        "    FROM trade\n"
        "    WHERE symbol = ?\n"
        "      AND local_ts >= ?\n"
        "      AND local_ts <= ?\n"
        "    GROUP BY 1, 2\n"
        "),\n"
        "grid AS (\n"
        "    SELECT generate_series AS bar_ts\n"
        "    FROM generate_series(\n"
        f"        time_bucket({interval_sql}, make_timestamp({start_ns}::BIGINT // 1000)),\n"
        f"        time_bucket({interval_sql}, make_timestamp({end_ns}::BIGINT // 1000)),\n"
        f"        {interval_sql}\n"
        "    )\n"
        "),\n"
        "filled AS (\n"
        "    SELECT\n"
        "        epoch_ns(g.bar_ts)          AS bar,\n"
        f"        ? AS symbol,\n"
        f"        '{interval_label}'          AS interval,\n"
        "        a.open,\n"
        "        a.high,\n"
        "        a.low,\n"
        "        a.close,\n"
        "        coalesce(a.volume, 0.0)      AS volume,\n"
        "        a.vwap,\n"
        "        coalesce(a.trade_count, 0)    AS trade_count\n"
        "    FROM grid g\n"
        "    LEFT JOIN agg a USING (bar_ts)\n"
        ")\n"
        "SELECT * FROM filled ORDER BY bar"
    )


def resample_ohlcv(
    catalog: Catalog,
    symbol: str,
    start_ns: int,
    end_ns: int,
    interval: str,
    *,
    fill_empty: bool = False,
) -> pl.DataFrame:
    """Resample trade records from the DuckDB Catalog into OHLCV bars.

    Queries the ``trade`` view in the DuckDB Catalog, groups by time bucket,
    and returns OHLCV bars as a Polars DataFrame.
    """
    _ns, interval_sql, _polars_str = _parse_interval(interval)
    interval_label = interval.strip().lower()

    catalog.refresh_views()
    conn = catalog.connection

    if fill_empty:
        sql = _build_fill_sql(interval_sql, interval_label, start_ns, end_ns)
        params: list[object] = [symbol, start_ns, end_ns, symbol]
    else:
        sql = _build_no_fill_sql(interval_sql, interval_label)
        params = [symbol, start_ns, end_ns]

    try:
        result = conn.execute(sql, params)
        df: pl.DataFrame = result.pl()
    except (duckdb.CatalogException, duckdb.IOException):
        return pl.DataFrame()

    return df


# ---------------------------------------------------------------------------
# Stream/Record-Level Resampling
# ---------------------------------------------------------------------------


def _detect_scale_and_adjust_interval(ts: int, interval_ns: int) -> int:
    """Detect timestamp unit and return adjusted_interval in the same unit.

    If ts is a small offset or mock timestamp (ts < 1e11), we default to nanoseconds.
    Otherwise, we use the epoch ranges:
    - ts > 1e17: nanoseconds
    - 1e14 < ts <= 1e17: microseconds
    - 1e11 < ts <= 1e14: milliseconds
    """
    if ts < 1e11:
        return interval_ns
    elif ts > 1e17:  # nanoseconds
        return interval_ns
    elif ts > 1e14:  # microseconds
        return max(1, interval_ns // 1000)
    else:  # milliseconds
        return max(1, interval_ns // 1_000_000)


def resample_trades_to_bars(trades: Iterable[Trade], interval: str) -> Iterator[Bar]:
    """Resample an iterable of Trade records into Bar records.

    Assumes Trade records are ordered by local_ts.
    """
    interval_ns, _, interval_label = _parse_interval(interval)
    adjusted_interval: int | None = None

    current_bucket: int | None = None
    open_px = 0.0
    high_px = 0.0
    low_px = 0.0
    close_px = 0.0
    volume = 0.0
    vwap_sum = 0.0
    trade_count = 0

    provider = ""
    symbol = ""
    symbol_raw = ""

    previous_ts: int | None = None

    for trade in trades:
        if previous_ts is not None and trade.local_ts < previous_ts:
            raise ValueError(
                f"Unsorted stream: trade local_ts {trade.local_ts} "
                f"is less than previous_ts {previous_ts}"
            )
        previous_ts = trade.local_ts

        if not provider:
            provider = trade.provider
            symbol = trade.symbol
            symbol_raw = trade.symbol_raw

        if adjusted_interval is None:
            adjusted_interval = _detect_scale_and_adjust_interval(trade.local_ts, interval_ns)

        bucket = (trade.local_ts // adjusted_interval) * adjusted_interval

        if current_bucket is None:
            current_bucket = bucket
            open_px = trade.price
            high_px = trade.price
            low_px = trade.price
            close_px = trade.price
            volume = trade.size
            vwap_sum = trade.price * trade.size
            trade_count = 1
        elif bucket == current_bucket:
            high_px = max(high_px, trade.price)
            low_px = min(low_px, trade.price)
            close_px = trade.price
            volume += trade.size
            vwap_sum += trade.price * trade.size
            trade_count += 1
        else:
            vwap = (vwap_sum / volume) if volume > 0 else None
            yield Bar(
                provider=provider,
                symbol=symbol,
                symbol_raw=symbol_raw,
                source_ts=None,
                local_ts=current_bucket,
                interval=interval_label,
                open=open_px,
                high=high_px,
                low=low_px,
                close=close_px,
                volume=volume,
                vwap=vwap,
                trade_count=trade_count,
            )
            current_bucket = bucket
            open_px = trade.price
            high_px = trade.price
            low_px = trade.price
            close_px = trade.price
            volume = trade.size
            vwap_sum = trade.price * trade.size
            trade_count = 1

    if current_bucket is not None:
        vwap = (vwap_sum / volume) if volume > 0 else None
        yield Bar(
            provider=provider,
            symbol=symbol,
            symbol_raw=symbol_raw,
            source_ts=None,
            local_ts=current_bucket,
            interval=interval_label,
            open=open_px,
            high=high_px,
            low=low_px,
            close=close_px,
            volume=volume,
            vwap=vwap,
            trade_count=trade_count,
        )


def resample_quotes_to_bars(
    quotes: Iterable[Quote], interval: str, price_type: str = "mid"
) -> Iterator[Bar]:
    """Resample Quote records into Bar records based on bid, ask, or mid-price.

    Assumes Quote records are ordered by local_ts.
    """
    interval_ns, _, interval_label = _parse_interval(interval)
    adjusted_interval: int | None = None

    current_bucket: int | None = None
    open_px = 0.0
    high_px = 0.0
    low_px = 0.0
    close_px = 0.0
    volume = 0.0
    quote_count = 0
    price_sum = 0.0

    provider = ""
    symbol = ""
    symbol_raw = ""

    previous_ts: int | None = None

    for quote in quotes:
        if previous_ts is not None and quote.local_ts < previous_ts:
            raise ValueError(
                f"Unsorted stream: quote local_ts {quote.local_ts} "
                f"is less than previous_ts {previous_ts}"
            )
        previous_ts = quote.local_ts

        if not provider:
            provider = quote.provider
            symbol = quote.symbol
            symbol_raw = quote.symbol_raw

        if adjusted_interval is None:
            adjusted_interval = _detect_scale_and_adjust_interval(quote.local_ts, interval_ns)

        bucket = (quote.local_ts // adjusted_interval) * adjusted_interval

        if price_type == "mid":
            price = (quote.bid_px + quote.ask_px) / 2.0
        elif price_type == "bid":
            price = quote.bid_px
        elif price_type == "ask":
            price = quote.ask_px
        else:
            raise ValueError(f"Unknown price_type: {price_type!r}")

        if current_bucket is None:
            current_bucket = bucket
            open_px = price
            high_px = price
            low_px = price
            close_px = price
            volume = 0.0
            price_sum = price
            quote_count = 1
        elif bucket == current_bucket:
            high_px = max(high_px, price)
            low_px = min(low_px, price)
            close_px = price
            price_sum += price
            quote_count += 1
        else:
            vwap = (price_sum / quote_count) if quote_count > 0 else None
            yield Bar(
                provider=provider,
                symbol=symbol,
                symbol_raw=symbol_raw,
                source_ts=None,
                local_ts=current_bucket,
                interval=interval_label,
                open=open_px,
                high=high_px,
                low=low_px,
                close=close_px,
                volume=volume,
                vwap=vwap,
                trade_count=quote_count,
            )
            current_bucket = bucket
            open_px = price
            high_px = price
            low_px = price
            close_px = price
            volume = 0.0
            price_sum = price
            quote_count = 1

    if current_bucket is not None:
        vwap = (price_sum / quote_count) if quote_count > 0 else None
        yield Bar(
            provider=provider,
            symbol=symbol,
            symbol_raw=symbol_raw,
            source_ts=None,
            local_ts=current_bucket,
            interval=interval_label,
            open=open_px,
            high=high_px,
            low=low_px,
            close=close_px,
            volume=volume,
            vwap=vwap,
            trade_count=quote_count,
        )


def resample_bars_to_bars(bars: Iterable[Bar], interval: str) -> Iterator[Bar]:
    """Resample lower-resolution Bar records into higher-resolution Bar records.

    Assumes Bar records are ordered by local_ts.
    """
    interval_ns, _, interval_label = _parse_interval(interval)
    adjusted_interval: int | None = None

    current_bucket: int | None = None
    open_px = 0.0
    high_px = 0.0
    low_px = 0.0
    close_px = 0.0
    volume = 0.0
    vwap_vol_sum = 0.0
    trade_count_sum = 0
    has_trade_count = False

    provider = ""
    symbol = ""
    symbol_raw = ""

    previous_ts: int | None = None

    for bar in bars:
        if previous_ts is not None and bar.local_ts < previous_ts:
            raise ValueError(
                f"Unsorted stream: bar local_ts {bar.local_ts} "
                f"is less than previous_ts {previous_ts}"
            )
        previous_ts = bar.local_ts

        if not provider:
            provider = bar.provider
            symbol = bar.symbol
            symbol_raw = bar.symbol_raw

        if adjusted_interval is None:
            adjusted_interval = _detect_scale_and_adjust_interval(bar.local_ts, interval_ns)

        bucket = (bar.local_ts // adjusted_interval) * adjusted_interval

        if current_bucket is None:
            current_bucket = bucket
            open_px = bar.open
            high_px = bar.high
            low_px = bar.low
            close_px = bar.close
            volume = bar.volume
            vwap_val = bar.vwap if bar.vwap is not None else bar.close
            vwap_vol_sum = vwap_val * bar.volume
            if bar.trade_count is not None:
                trade_count_sum = bar.trade_count
                has_trade_count = True
            else:
                trade_count_sum = 1
                has_trade_count = False
        elif bucket == current_bucket:
            high_px = max(high_px, bar.high)
            low_px = min(low_px, bar.low)
            close_px = bar.close
            volume += bar.volume
            vwap_val = bar.vwap if bar.vwap is not None else bar.close
            vwap_vol_sum += vwap_val * bar.volume
            if bar.trade_count is not None:
                trade_count_sum += bar.trade_count
                has_trade_count = True
            else:
                trade_count_sum += 1
        else:
            vwap = (
                (vwap_vol_sum / volume)
                if volume > 0
                else (vwap_vol_sum if vwap_vol_sum > 0 else None)
            )
            yield Bar(
                provider=provider,
                symbol=symbol,
                symbol_raw=symbol_raw,
                source_ts=None,
                local_ts=current_bucket,
                interval=interval_label,
                open=open_px,
                high=high_px,
                low=low_px,
                close=close_px,
                volume=volume,
                vwap=vwap,
                trade_count=trade_count_sum if has_trade_count else None,
            )
            current_bucket = bucket
            open_px = bar.open
            high_px = bar.high
            low_px = bar.low
            close_px = bar.close
            volume = bar.volume
            vwap_val = bar.vwap if bar.vwap is not None else bar.close
            vwap_vol_sum = vwap_val * bar.volume
            if bar.trade_count is not None:
                trade_count_sum = bar.trade_count
                has_trade_count = True
            else:
                trade_count_sum = 1
                has_trade_count = False

    if current_bucket is not None:
        vwap = (
            (vwap_vol_sum / volume) if volume > 0 else (vwap_vol_sum if vwap_vol_sum > 0 else None)
        )
        yield Bar(
            provider=provider,
            symbol=symbol,
            symbol_raw=symbol_raw,
            source_ts=None,
            local_ts=current_bucket,
            interval=interval_label,
            open=open_px,
            high=high_px,
            low=low_px,
            close=close_px,
            volume=volume,
            vwap=vwap,
            trade_count=trade_count_sum if has_trade_count else None,
        )


# ---------------------------------------------------------------------------
# Polars-Based Resampling
# ---------------------------------------------------------------------------


def resample_trades_df(df: pl.DataFrame, interval: str) -> pl.DataFrame:
    """Resample trades DataFrame using Polars.

    Expects columns: local_ts, price, size, and optionally symbol, provider, symbol_raw.
    """
    if len(df) == 0:
        return pl.DataFrame(
            schema={
                "bar": pl.Int64,
                "symbol": pl.String,
                "interval": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "vwap": pl.Float64,
                "trade_count": pl.Int64,
            }
        )

    _ns, _sql, polars_str = _parse_interval(interval)
    interval_label = interval.strip().lower()

    df_dt = df.with_columns(pl.from_epoch("local_ts", time_unit="ns").alias("datetime")).sort(
        "datetime"
    )

    group_keys = [k for k in ["symbol", "provider", "symbol_raw"] if k in df.columns]

    resampled = df_dt.group_by_dynamic(
        "datetime",
        every=polars_str,
        group_by=group_keys,
        closed="left",
    ).agg(
        [
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("size").sum().alias("volume"),
            pl.when(pl.col("size").sum() > 0.0)
            .then((pl.col("price") * pl.col("size")).sum() / pl.col("size").sum())
            .otherwise(None)
            .alias("vwap"),
            pl.len().cast(pl.Int64).alias("trade_count"),
        ]
    )

    resampled = resampled.with_columns(
        pl.col("datetime").dt.epoch("ns").alias("bar"),
        pl.lit(interval_label).alias("interval"),
    ).drop("datetime")

    desired_cols = [
        "bar",
        "symbol",
        "interval",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "trade_count",
    ]
    out_cols = [c for c in desired_cols if c in resampled.columns] + [
        c for c in resampled.columns if c not in desired_cols
    ]
    return resampled.select(out_cols)


def resample_quotes_df(df: pl.DataFrame, interval: str, price_type: str = "mid") -> pl.DataFrame:
    """Resample quotes DataFrame using Polars.

    Expects columns: local_ts, bid_px, ask_px, and optionally symbol, provider, symbol_raw.
    """
    if len(df) == 0:
        return pl.DataFrame(
            schema={
                "bar": pl.Int64,
                "symbol": pl.String,
                "interval": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "vwap": pl.Float64,
                "trade_count": pl.Int64,
            }
        )

    _ns, _sql, polars_str = _parse_interval(interval)
    interval_label = interval.strip().lower()

    if price_type == "mid":
        df = df.with_columns(((pl.col("bid_px") + pl.col("ask_px")) / 2.0).alias("price"))
    elif price_type == "bid":
        df = df.with_columns(pl.col("bid_px").alias("price"))
    elif price_type == "ask":
        df = df.with_columns(pl.col("ask_px").alias("price"))
    else:
        raise ValueError(f"Unknown price_type: {price_type!r}")

    df_dt = df.with_columns(pl.from_epoch("local_ts", time_unit="ns").alias("datetime")).sort(
        "datetime"
    )

    group_keys = [k for k in ["symbol", "provider", "symbol_raw"] if k in df.columns]

    resampled = df_dt.group_by_dynamic(
        "datetime",
        every=polars_str,
        group_by=group_keys,
        closed="left",
    ).agg(
        [
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.lit(0.0).alias("volume"),
            pl.col("price").mean().alias("vwap"),
            pl.len().cast(pl.Int64).alias("trade_count"),
        ]
    )

    resampled = resampled.with_columns(
        pl.col("datetime").dt.epoch("ns").alias("bar"),
        pl.lit(interval_label).alias("interval"),
    ).drop("datetime")

    desired_cols = [
        "bar",
        "symbol",
        "interval",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "trade_count",
    ]
    out_cols = [c for c in desired_cols if c in resampled.columns] + [
        c for c in resampled.columns if c not in desired_cols
    ]
    return resampled.select(out_cols)


def resample_bars_df(df: pl.DataFrame, interval: str) -> pl.DataFrame:
    """Resample lower-resolution bars DataFrame into higher-resolution bars using Polars.

    Expects columns: local_ts (or bar), open, high, low, close, volume, and optionally
    vwap, trade_count, symbol.
    """
    if len(df) == 0:
        return pl.DataFrame(
            schema={
                "bar": pl.Int64,
                "symbol": pl.String,
                "interval": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "vwap": pl.Float64,
                "trade_count": pl.Int64,
            }
        )

    _ns, _sql, polars_str = _parse_interval(interval)
    interval_label = interval.strip().lower()

    if "local_ts" not in df.columns and "bar" in df.columns:
        df = df.with_columns(pl.col("bar").alias("local_ts"))

    df_dt = df.with_columns(pl.from_epoch("local_ts", time_unit="ns").alias("datetime")).sort(
        "datetime"
    )

    group_keys = [k for k in ["symbol", "provider", "symbol_raw"] if k in df.columns]

    if "vwap" not in df.columns:
        df_dt = df_dt.with_columns(pl.col("close").alias("vwap"))

    if "trade_count" not in df.columns:
        df_dt = df_dt.with_columns(pl.lit(1).alias("trade_count"))

    resampled = df_dt.group_by_dynamic(
        "datetime",
        every=polars_str,
        group_by=group_keys,
        closed="left",
    ).agg(
        [
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            pl.when(pl.col("volume").sum() > 0.0)
            .then((pl.col("vwap") * pl.col("volume")).sum() / pl.col("volume").sum())
            .otherwise(None)
            .alias("vwap"),
            pl.col("trade_count").sum().cast(pl.Int64).alias("trade_count"),
        ]
    )

    resampled = resampled.with_columns(
        pl.col("datetime").dt.epoch("ns").alias("bar"),
        pl.lit(interval_label).alias("interval"),
    ).drop("datetime")

    desired_cols = [
        "bar",
        "symbol",
        "interval",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "trade_count",
    ]
    out_cols = [c for c in desired_cols if c in resampled.columns] + [
        c for c in resampled.columns if c not in desired_cols
    ]
    return resampled.select(out_cols)
