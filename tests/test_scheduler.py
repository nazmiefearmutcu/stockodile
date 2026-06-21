"""Unit tests for the low-frequency Scheduled Pull Coordinator and US Market Calendar."""

import datetime
import os
import tempfile

import pytest

from stockodile.scheduler.calendar import MARKET_TZ, USMarketCalendar
from stockodile.scheduler.coordinator import ScheduledPullCoordinator
from stockodile.scheduler.state import InMemoryStateStore, JSONFileStateStore


def test_calendar_holidays() -> None:
    """Test standard US Equity market holidays and observation rules."""
    cal = USMarketCalendar()

    # 2026 Holidays
    # New Year's Day: Jan 1 (Thursday)
    assert cal.is_holiday(datetime.date(2026, 1, 1))

    # MLK Day: Jan 19 (3rd Monday)
    assert cal.is_holiday(datetime.date(2026, 1, 19))
    assert not cal.is_holiday(datetime.date(2026, 1, 12))  # 2nd Monday

    # Presidents' Day: Feb 16 (3rd Monday)
    assert cal.is_holiday(datetime.date(2026, 2, 16))

    # Good Friday: April 3 (Easter is April 5, 2026)
    assert cal.is_holiday(datetime.date(2026, 4, 3))

    # Memorial Day: May 25 (Last Monday in May 2026)
    assert cal.is_holiday(datetime.date(2026, 5, 25))

    # Juneteenth: June 19 (Friday)
    assert cal.is_holiday(datetime.date(2026, 6, 19))

    # Independence Day: July 4 is Saturday, observed July 3
    assert cal.is_holiday(datetime.date(2026, 7, 3))
    assert not cal.is_trading_day(datetime.date(2026, 7, 3))

    # Labor Day: Sept 7 (1st Monday)
    assert cal.is_holiday(datetime.date(2026, 9, 7))

    # Thanksgiving: Nov 26 (4th Thursday)
    assert cal.is_holiday(datetime.date(2026, 11, 26))

    # Christmas Day: Dec 25 (Friday)
    assert cal.is_holiday(datetime.date(2026, 12, 25))


def test_calendar_early_closes() -> None:
    """Test detection of early close days (Black Friday, Christmas Eve, etc.)."""
    cal = USMarketCalendar()

    # Black Friday (day after Thanksgiving) in 2026: Nov 27
    assert cal.is_early_close(datetime.date(2026, 11, 27))
    hours = cal.get_market_hours(datetime.date(2026, 11, 27))
    assert hours is not None
    open_dt, close_dt = hours
    assert open_dt.time() == datetime.time(9, 30)
    assert close_dt.time() == datetime.time(13, 0)

    # Christmas Eve 2026: Dec 24 (Thursday) - early close
    assert cal.is_early_close(datetime.date(2026, 12, 24))

    # Normal trading day hours: Jan 2, 2026 (Friday)
    hours = cal.get_market_hours(datetime.date(2026, 1, 2))
    assert hours is not None
    open_dt, close_dt = hours
    assert open_dt.time() == datetime.time(9, 30)
    assert close_dt.time() == datetime.time(16, 0)


def test_calendar_market_open() -> None:
    """Test checking if the market is open at specific times."""
    cal = USMarketCalendar()

    # Wed, Jan 7, 2026 - normal trading day
    # 9:00 AM ET - pre-market (closed)
    dt_pre = datetime.datetime(2026, 1, 7, 9, 0, tzinfo=MARKET_TZ)
    assert not cal.is_market_open(dt_pre)

    # 10:00 AM ET - open
    dt_open = datetime.datetime(2026, 1, 7, 10, 0, tzinfo=MARKET_TZ)
    assert cal.is_market_open(dt_open)

    # 4:30 PM ET - post-market (closed)
    dt_post = datetime.datetime(2026, 1, 7, 16, 30, tzinfo=MARKET_TZ)
    assert not cal.is_market_open(dt_post)

    # Sunday, Jan 4, 2026 (closed)
    dt_sun = datetime.datetime(2026, 1, 4, 12, 0, tzinfo=MARKET_TZ)
    assert not cal.is_market_open(dt_sun)


def test_json_state_store() -> None:
    """Test serialization/deserialization of JSONFileStateStore."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "state.json")
        store = JSONFileStateStore(filepath)

        # Retrieve empty state
        ts, dt = store.get_last_run("test_task")
        assert ts is None
        assert dt is None

        # Write state
        run_ts = datetime.datetime(2026, 6, 21, 12, 0, 0, tzinfo=MARKET_TZ)
        run_dt = datetime.date(2026, 6, 20)
        store.update_last_run("test_task", run_ts, run_dt)

        # Read back from same instance
        ts, dt = store.get_last_run("test_task")
        assert ts == run_ts
        assert dt == run_dt

        # Read back from new instance loading same file
        new_store = JSONFileStateStore(filepath)
        ts, dt = new_store.get_last_run("test_task")
        assert ts == run_ts
        assert dt == run_dt


@pytest.mark.asyncio
async def test_coordinator_simple_run() -> None:
    """Test coordinator executes simple registered tasks."""
    state = InMemoryStateStore()
    coord = ScheduledPullCoordinator(state)

    sync_runs = []
    async_runs = []

    def sync_task(run_date: datetime.date) -> None:
        sync_runs.append(run_date)

    async def async_task(run_date: datetime.date) -> None:
        async_runs.append(run_date)

    # Wed, Jan 7, 2026
    start_time = datetime.datetime(2026, 1, 7, 18, 0, tzinfo=MARKET_TZ)

    coord.register_task(
        "sync_t",
        sync_task,
        interval="daily",
        policy="after_hours",
        start_date=datetime.date(2026, 1, 7),
    )
    coord.register_task(
        "async_t",
        async_task,
        interval="daily",
        policy="after_hours",
        start_date=datetime.date(2026, 1, 7),
    )

    # Check and run
    runs = await coord.check_and_run_once(start_time)
    assert runs == 2
    assert sync_runs == [datetime.date(2026, 1, 7)]
    assert async_runs == [datetime.date(2026, 1, 7)]

    # Check states are updated
    ts, dt = state.get_last_run("sync_t")
    assert ts == start_time
    assert dt == datetime.date(2026, 1, 7)

    # Check and run again, should not run again for the same period
    runs = await coord.check_and_run_once(start_time)
    assert runs == 0
    assert len(sync_runs) == 1


@pytest.mark.asyncio
async def test_coordinator_catch_up() -> None:
    """Test catch-up functionality when multiple periods are missed."""
    state = InMemoryStateStore()
    coord = ScheduledPullCoordinator(state)

    executed_dates = []

    async def my_task(run_date: datetime.date) -> None:
        executed_dates.append(run_date)

    # Mark as last run on Wed, Jan 7, 2026
    last_run_time = datetime.datetime(2026, 1, 7, 18, 0, tzinfo=MARKET_TZ)
    state.update_last_run("catch_up_task", last_run_time, datetime.date(2026, 1, 7))

    coord.register_task(
        "catch_up_task",
        my_task,
        interval="daily",
        policy="after_hours",
    )

    # Current time is Fri, Jan 9, 2026 at 18:00 (Market closed)
    # The scheduler should run for Jan 8 first, and update state
    now_time = datetime.datetime(2026, 1, 9, 18, 0, tzinfo=MARKET_TZ)

    # First check_and_run_once should trigger for Jan 8
    runs = await coord.check_and_run_once(now_time)
    assert runs == 1
    assert executed_dates == [datetime.date(2026, 1, 8)]

    # State should update to Jan 8
    _, dt = state.get_last_run("catch_up_task")
    assert dt == datetime.date(2026, 1, 8)

    # Second check_and_run_once should trigger for Jan 9 (since Jan 9 is now also due)
    runs = await coord.check_and_run_once(now_time)
    assert runs == 1
    assert executed_dates == [datetime.date(2026, 1, 8), datetime.date(2026, 1, 9)]

    # State should update to Jan 9
    _, dt = state.get_last_run("catch_up_task")
    assert dt == datetime.date(2026, 1, 9)

    # Third check_and_run_once should do nothing
    runs = await coord.check_and_run_once(now_time)
    assert runs == 0


@pytest.mark.asyncio
async def test_coordinator_policies() -> None:
    """Test policy filters (anytime, during_hours, after_hours)."""
    state = InMemoryStateStore()
    coord = ScheduledPullCoordinator(state)

    during_ran = False
    after_ran = False

    async def during_task() -> None:
        nonlocal during_ran
        during_ran = True

    async def after_task() -> None:
        nonlocal after_ran
        after_ran = True

    coord.register_task(
        "during",
        during_task,
        interval="daily",
        policy="during_hours",
        target_time=datetime.time(10, 0),
        start_date=datetime.date(2026, 1, 7),
    )
    coord.register_task(
        "after",
        after_task,
        interval="daily",
        policy="after_hours",
        target_time=datetime.time(16, 30),
        start_date=datetime.date(2026, 1, 7),
    )

    # Scenario A: Jan 7, 2026 at 11:00 AM (Market is open)
    # The "during" task is due (time >= 10:00, policy during_hours met).
    # The "after" task is not due (time < 16:30, policy after_hours not met).
    now_during = datetime.datetime(2026, 1, 7, 11, 0, tzinfo=MARKET_TZ)
    runs = await coord.check_and_run_once(now_during)
    assert runs == 1
    assert during_ran
    assert not after_ran

    # Scenario B: Jan 7, 2026 at 5:00 PM (Market is closed)
    # The "after" task is due (time >= 16:30, policy after_hours met).
    now_after = datetime.datetime(2026, 1, 7, 17, 0, tzinfo=MARKET_TZ)
    runs = await coord.check_and_run_once(now_after)
    assert runs == 1
    assert after_ran


@pytest.mark.asyncio
async def test_coordinator_intervals() -> None:
    """Test interval calculations (weekly, semi_monthly, monthly, bi_monthly)."""
    state = InMemoryStateStore()
    coord = ScheduledPullCoordinator(state)

    weekly_runs = []
    semi_monthly_runs = []
    monthly_runs = []
    bi_monthly_runs = []

    coord.register_task("w", lambda d: weekly_runs.append(d), interval="weekly")
    coord.register_task("s", lambda d: semi_monthly_runs.append(d), interval="semi_monthly")
    coord.register_task("m", lambda d: monthly_runs.append(d), interval="monthly")
    coord.register_task("b", lambda d: bi_monthly_runs.append(d), interval="bi_monthly")

    # Initial runs: Wed, Jan 7, 2026
    t1 = datetime.datetime(2026, 1, 7, 12, 0, tzinfo=MARKET_TZ)
    await coord.check_and_run_once(t1)
    assert weekly_runs == [datetime.date(2026, 1, 7)]
    assert semi_monthly_runs == [datetime.date(2026, 1, 7)]
    assert monthly_runs == [datetime.date(2026, 1, 7)]
    assert bi_monthly_runs == [datetime.date(2026, 1, 7)]

    # Clear run logs for easier verification of next steps
    weekly_runs.clear()
    semi_monthly_runs.clear()
    monthly_runs.clear()
    bi_monthly_runs.clear()

    # Move to Fri, Jan 9, 2026 (same week, same semi-month, same month) - none should run
    t2 = datetime.datetime(2026, 1, 9, 12, 0, tzinfo=MARKET_TZ)
    await coord.check_and_run_once(t2)
    assert not weekly_runs
    assert not semi_monthly_runs
    assert not monthly_runs
    assert not bi_monthly_runs

    # Move to Monday, Jan 12, 2026 (new ISO week, same semi-month, same month) - weekly should run
    t3 = datetime.datetime(2026, 1, 12, 12, 0, tzinfo=MARKET_TZ)
    await coord.check_and_run_once(t3)
    assert weekly_runs == [datetime.date(2026, 1, 12)]
    assert not semi_monthly_runs
    assert not monthly_runs
    assert not bi_monthly_runs
    weekly_runs.clear()

    # Move to Fri, Jan 16, 2026 (same week, new semi-month [>= 15th], same month)
    # semi_monthly should run
    t4 = datetime.datetime(2026, 1, 16, 12, 0, tzinfo=MARKET_TZ)
    await coord.check_and_run_once(t4)
    assert not weekly_runs
    assert semi_monthly_runs == [datetime.date(2026, 1, 15)]
    assert not monthly_runs
    assert not bi_monthly_runs
    semi_monthly_runs.clear()

    # Move to Monday, Feb 2, 2026 (new week, new semi-month [1st], new month,
    # same bi-month [Jan/Feb same period])
    # weekly, semi_monthly, and monthly should run. bi-monthly should NOT.
    t5 = datetime.datetime(2026, 2, 2, 12, 0, tzinfo=MARKET_TZ)
    await coord.check_and_run_once(t5)
    assert weekly_runs == [datetime.date(2026, 1, 20)]
    assert semi_monthly_runs == [datetime.date(2026, 2, 2)]
    assert monthly_runs == [datetime.date(2026, 2, 2)]
    assert not bi_monthly_runs
    weekly_runs.clear()
    semi_monthly_runs.clear()
    monthly_runs.clear()

    # Move to Monday, Mar 2, 2026 (new bi-month [Mar/Apr is new period])
    # bi-monthly should run (along with others)
    t6 = datetime.datetime(2026, 3, 2, 12, 0, tzinfo=MARKET_TZ)
    await coord.check_and_run_once(t6)
    assert weekly_runs == [datetime.date(2026, 1, 26)]
    assert semi_monthly_runs == [datetime.date(2026, 2, 17)]
    assert monthly_runs == [datetime.date(2026, 3, 2)]
    assert bi_monthly_runs == [datetime.date(2026, 3, 2)]
