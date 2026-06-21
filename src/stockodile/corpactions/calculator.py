"""Corporate action price/volume adjustment calculator following CRSP formulas."""

import datetime
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Any

import polars as pl

from stockodile.schema.enums import CorpActionType
from stockodile.schema.records import Bar, CorporateAction


def calculate_cumulative_factors(
    actions: Iterable[CorporateAction],
    dates: Sequence[datetime.date | str],
    base_date: datetime.date | str | None = None,
) -> dict[datetime.date, tuple[float, float]]:
    """
    Calculate the cumulative adjustment factors CFACPR and CFACSHR for a sequence of dates.

    Args:
        actions: Iterable of CorporateAction objects.
        dates: Sequence of dates (datetime.date or YYYY-MM-DD string).
        base_date: Optional base date at which cumulative factors are 1.0.
                  Defaults to the maximum date in `dates`.

    Returns:
        A dictionary mapping each date (as datetime.date) to a tuple of (CFACPR, CFACSHR).
    """
    # 1. Parse and sort all unique dates
    parsed_dates = sorted(
        {d if isinstance(d, datetime.date) else datetime.date.fromisoformat(d) for d in dates}
    )

    if not parsed_dates:
        return {}

    # 2. Determine base date
    if base_date is None:
        base_dt = parsed_dates[-1]
    elif isinstance(base_date, datetime.date):
        base_dt = base_date
    else:
        base_dt = datetime.date.fromisoformat(base_date)

    # 3. Group actions by ex_date and calculate daily multiplier f
    actions_by_date = defaultdict(list)
    for act in actions:
        ex_dt = (
            act.ex_date
            if isinstance(act.ex_date, datetime.date)
            else datetime.date.fromisoformat(act.ex_date)
        )
        actions_by_date[ex_dt].append(act)

    # Calculate raw cumulative factors walking backward from the maximum date.
    desc_dates = sorted(parsed_dates, reverse=True)

    c_raw_pr: dict[datetime.date, float] = {}
    c_raw_shr: dict[datetime.date, float] = {}

    curr_pr = 1.0
    curr_shr = 1.0

    for d in desc_dates:
        c_raw_pr[d] = curr_pr
        c_raw_shr[d] = curr_shr

        # Events on ex_date `d` affect all dates `< d` (i.e. dates after `d` in desc_dates)
        if d in actions_by_date:
            for act in actions_by_date[d]:
                f_pr = 1.0
                f_shr = 1.0
                if act.type in (CorpActionType.SPLIT, CorpActionType.DIVIDEND_STOCK):
                    # CRSP split factor: f = FACPR + 1
                    f_pr = act.value + 1.0
                    f_shr = act.value + 1.0
                elif act.type == CorpActionType.SPINOFF:
                    f_pr = act.value + 1.0

                curr_pr *= f_pr
                curr_shr *= f_shr

    # Normalize relative to base_date
    if base_dt in c_raw_pr:
        norm_pr = c_raw_pr[base_dt]
        norm_shr = c_raw_shr[base_dt]
    else:
        # Fallback to the closest date <= base_dt, or the first date if none
        closest_dates = [d for d in parsed_dates if d <= base_dt]
        if closest_dates:
            ref_dt = closest_dates[-1]
            norm_pr = c_raw_pr[ref_dt]
            norm_shr = c_raw_shr[ref_dt]
        else:
            ref_dt = parsed_dates[0]
            norm_pr = c_raw_pr[ref_dt]
            norm_shr = c_raw_shr[ref_dt]

    return {d: (c_raw_pr[d] / norm_pr, c_raw_shr[d] / norm_shr) for d in parsed_dates}


def adjust_bars(
    bars: Sequence[Bar],
    factors: dict[datetime.date, tuple[float, float]],
) -> list[Bar]:
    """
    Adjust Bar open, high, low, close, and volume using cumulative factors.

    Args:
        bars: Sequence of Bar records.
        factors: A dictionary mapping date to (CFACPR, CFACSHR).

    Returns:
        A list of new Bar records with adjusted fields.
    """
    adjusted_bars = []
    for bar in bars:
        ts = bar.source_ts if bar.source_ts is not None else bar.local_ts
        if ts > 1e11:  # Milliseconds
            dt = datetime.datetime.fromtimestamp(ts / 1000.0, tz=datetime.UTC).date()
        else:
            dt = datetime.datetime.fromtimestamp(ts, tz=datetime.UTC).date()

        cfacpr, cfacshr = factors.get(dt, (1.0, 1.0))

        adj_bar = Bar(
            provider=bar.provider,
            symbol=bar.symbol,
            symbol_raw=bar.symbol_raw,
            source_ts=bar.source_ts,
            local_ts=bar.local_ts,
            interval=bar.interval,
            open=bar.open / cfacpr,
            high=bar.high / cfacpr,
            low=bar.low / cfacpr,
            close=bar.close / cfacpr,
            volume=bar.volume * cfacshr,
            vwap=bar.vwap / cfacpr if bar.vwap is not None else None,
            trade_count=bar.trade_count,
        )
        adjusted_bars.append(adj_bar)

    return adjusted_bars


def calculate_total_returns(
    bars: Sequence[Bar],
    actions: Iterable[CorporateAction],
    factors: dict[datetime.date, tuple[float, float]],
) -> list[float | None]:
    """
    Calculate the split and dividend-adjusted total returns for a series of bars.

    TotalReturn(t) = (adj_price(t) + adj_div(t)) / adj_price(t-1) - 1
    where adj_div(t) = div_cash(t) / CFACPR(t).

    Returns a list of returns of the same length as `bars`, with the first element being None.
    """
    if len(bars) < 2:
        return [None] * len(bars)

    # Sort bars by date/time
    sorted_bars = sorted(
        bars,
        key=lambda b: b.source_ts if b.source_ts is not None else b.local_ts,
    )

    # Group cash dividends by ex-date
    cash_divs: dict[datetime.date, float] = defaultdict(float)
    for act in actions:
        if act.type == CorpActionType.DIVIDEND_CASH:
            ex_dt = (
                act.ex_date
                if isinstance(act.ex_date, datetime.date)
                else datetime.date.fromisoformat(act.ex_date)
            )
            cash_divs[ex_dt] += act.value

    returns: list[float | None] = [None]

    for i in range(1, len(sorted_bars)):
        b_prev = sorted_bars[i - 1]
        b_curr = sorted_bars[i]

        ts_curr = b_curr.source_ts if b_curr.source_ts is not None else b_curr.local_ts
        ts_prev = b_prev.source_ts if b_prev.source_ts is not None else b_prev.local_ts

        # Get dates
        if ts_curr > 1e11:
            dt_curr = datetime.datetime.fromtimestamp(ts_curr / 1000.0, tz=datetime.UTC).date()
        else:
            dt_curr = datetime.datetime.fromtimestamp(ts_curr, tz=datetime.UTC).date()

        if ts_prev > 1e11:
            dt_prev = datetime.datetime.fromtimestamp(ts_prev / 1000.0, tz=datetime.UTC).date()
        else:
            dt_prev = datetime.datetime.fromtimestamp(ts_prev, tz=datetime.UTC).date()

        cfacpr_curr, _ = factors.get(dt_curr, (1.0, 1.0))
        cfacpr_prev, _ = factors.get(dt_prev, (1.0, 1.0))

        adj_p_curr = b_curr.close / cfacpr_curr
        adj_p_prev = b_prev.close / cfacpr_prev

        div_cash = cash_divs.get(dt_curr, 0.0)
        adj_div = div_cash / cfacpr_curr

        if adj_p_prev == 0.0:
            returns.append(None)
        else:
            ret = (adj_p_curr + adj_div) / adj_p_prev - 1.0
            returns.append(ret)

    return returns


def adjust_dataframe(
    df: pl.DataFrame,
    actions: list[CorporateAction] | pl.DataFrame,
    base_date: str | datetime.date | None = None,
) -> pl.DataFrame:
    """
    Adjust a Polars DataFrame of daily stock prices and volumes using CRSP adjustments.

    The input DataFrame `df` must contain:
    - 'date': Date or String type
    - 'open', 'high', 'low', 'close', 'volume': Float type

    The function adds:
    - 'cfacpr', 'cfacshr': Cumulative adjustment factors
    - 'adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_volume'
    - 'total_return': Total return accounting for splits and dividends

    Args:
        df: Polars DataFrame of raw bar data.
        actions: list of CorporateAction objects or Polars DataFrame of actions
                 containing 'ex_date', 'type', 'value'.
        base_date: Base date YYYY-MM-DD or datetime.date at which factors are 1.0.

    Returns:
        Polars DataFrame with added adjustment and return columns.
    """
    if df.is_empty():
        return df.clone()

    # Extract actions as a list of CorporateAction if it's a DataFrame
    action_list: list[CorporateAction] = []
    cash_div_list: list[dict[str, Any]] = []

    if isinstance(actions, pl.DataFrame):
        for row in actions.iter_rows(named=True):
            act_type_str = str(row["type"])
            try:
                act_type = CorpActionType(act_type_str)
            except ValueError:
                act_type = CorpActionType.SPLIT

            ex_dt_val = row["ex_date"]
            if isinstance(ex_dt_val, datetime.date):
                ex_date_str = ex_dt_val.isoformat()
            else:
                ex_date_str = str(ex_dt_val)

            val = float(row["value"])
            action_list.append(
                CorporateAction(
                    provider="dataframe",
                    symbol="DF",
                    symbol_raw="DF",
                    source_ts=None,
                    local_ts=0,
                    ex_date=ex_date_str,
                    type=act_type,
                    value=val,
                )
            )
            if act_type == CorpActionType.DIVIDEND_CASH:
                cash_div_list.append({"ex_date": ex_date_str, "div_cash": val})
    else:
        action_list = list(actions)
        for act in action_list:
            if act.type == CorpActionType.DIVIDEND_CASH:
                ex_dt_str = (
                    act.ex_date.isoformat()
                    if isinstance(act.ex_date, datetime.date)
                    else str(act.ex_date)
                )
                cash_div_list.append({"ex_date": ex_dt_str, "div_cash": act.value})

    # Get all unique dates from df and parse them
    df_with_dt = df.with_columns(
        pl.col("date").cast(pl.String).str.to_date("%Y-%m-%d", strict=False).alias("_parsed_date")
    )

    unique_dates = df_with_dt["_parsed_date"].drop_nulls().unique().to_list()
    factors = calculate_cumulative_factors(action_list, unique_dates, base_date=base_date)

    # Convert factors dictionary to a DataFrame for joining
    factor_rows = []
    for dt, (pr_fac, shr_fac) in factors.items():
        factor_rows.append({"_parsed_date": dt, "cfacpr": pr_fac, "cfacshr": shr_fac})

    if not factor_rows:
        factors_df = pl.DataFrame(
            schema={"_parsed_date": pl.Date, "cfacpr": pl.Float64, "cfacshr": pl.Float64}
        )
    else:
        factors_df = pl.DataFrame(factor_rows)

    # Build cash dividends DataFrame
    cash_div_grouped: dict[str, float] = defaultdict(float)
    for item in cash_div_list:
        cash_div_grouped[item["ex_date"]] += item["div_cash"]

    cash_div_rows = [
        {
            "_parsed_date": datetime.date.fromisoformat(k),
            "div_cash": v,
        }
        for k, v in cash_div_grouped.items()
    ]

    if not cash_div_rows:
        div_df = pl.DataFrame(schema={"_parsed_date": pl.Date, "div_cash": pl.Float64})
    else:
        div_df = pl.DataFrame(cash_div_rows)

    # Join factors and dividends
    res_df = df_with_dt.join(factors_df, on="_parsed_date", how="left")
    res_df = res_df.join(div_df, on="_parsed_date", how="left")

    # Fill null factors with 1.0, null dividend with 0.0
    res_df = res_df.with_columns(
        pl.col("cfacpr").fill_null(1.0),
        pl.col("cfacshr").fill_null(1.0),
        pl.col("div_cash").fill_null(0.0),
    )

    # Calculate adjusted values
    res_df = res_df.with_columns(
        (pl.col("open") / pl.col("cfacpr")).alias("adj_open"),
        (pl.col("high") / pl.col("cfacpr")).alias("adj_high"),
        (pl.col("low") / pl.col("cfacpr")).alias("adj_low"),
        (pl.col("close") / pl.col("cfacpr")).alias("adj_close"),
        (pl.col("volume") * pl.col("cfacshr")).alias("adj_volume"),
    )

    # Sort by date ascending to calculate returns
    res_df = res_df.sort("_parsed_date")

    # Calculate adjusted dividend cash: adj_div = div_cash / cfacpr
    res_df = res_df.with_columns((pl.col("div_cash") / pl.col("cfacpr")).alias("adj_div_cash"))

    # TotalReturn(t) = (adj_close(t) + adj_div_cash(t)) / adj_close(t-1) - 1
    res_df = res_df.with_columns(
        ((pl.col("adj_close") + pl.col("adj_div_cash")) / pl.col("adj_close").shift(1) - 1.0).alias(
            "total_return"
        )
    )

    res_df = res_df.drop(["_parsed_date", "adj_div_cash"])

    return res_df
