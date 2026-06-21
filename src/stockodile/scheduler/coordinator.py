"""Coordinator for low-frequency scheduled pulls (daily, weekly, bi-monthly, etc.)."""

import asyncio
import datetime
import inspect
import logging
from collections.abc import Callable
from typing import Any

from stockodile.scheduler.calendar import MARKET_TZ, USMarketCalendar
from stockodile.scheduler.state import SchedulerStateStore

logger = logging.getLogger("stockodile.scheduler")


class PullTask:
    """Represents a scheduled pull task configuration."""

    def __init__(
        self,
        name: str,
        func: Callable[..., Any],
        interval: str = "daily",
        policy: str = "anytime",
        run_on_non_trading_days: bool = False,
        target_time: datetime.time | None = None,
        start_date: datetime.date | None = None,
    ) -> None:
        self.name = name
        self.func = func
        self.interval = interval
        self.policy = policy
        self.run_on_non_trading_days = run_on_non_trading_days

        # Determine default target time based on policy if not specified
        if target_time is None:
            if policy == "after_hours":
                self.target_time = datetime.time(16, 30)
            elif policy == "during_hours":
                self.target_time = datetime.time(9, 30)
            else:
                self.target_time = datetime.time(0, 0)
        else:
            self.target_time = target_time

        self.start_date = start_date


class ScheduledPullCoordinator:
    """Coordinates low-frequency data pulls, respecting trading hours and calendars."""

    def __init__(
        self,
        state_store: SchedulerStateStore,
        calendar: USMarketCalendar | None = None,
    ) -> None:
        self.state_store = state_store
        self.calendar = calendar or USMarketCalendar()
        self.tasks: dict[str, PullTask] = {}

    def register_task(
        self,
        name: str,
        func: Callable[..., Any],
        interval: str = "daily",
        policy: str = "anytime",
        run_on_non_trading_days: bool = False,
        target_time: datetime.time | None = None,
        start_date: datetime.date | None = None,
    ) -> None:
        """Register a new task with the coordinator."""
        if name in self.tasks:
            raise ValueError(f"Task with name '{name}' is already registered.")

        valid_intervals = {"daily", "weekly", "semi_monthly", "monthly", "bi_monthly"}
        if interval not in valid_intervals:
            raise ValueError(
                f"Invalid interval '{interval}'. Must be one of {valid_intervals}"
            )

        valid_policies = {"anytime", "during_hours", "after_hours"}
        if policy not in valid_policies:
            raise ValueError(f"Invalid policy '{policy}'. Must be one of {valid_policies}")

        self.tasks[name] = PullTask(
            name=name,
            func=func,
            interval=interval,
            policy=policy,
            run_on_non_trading_days=run_on_non_trading_days,
            target_time=target_time,
            start_date=start_date,
        )
        logger.info(
            f"Registered scheduled task: {name} (interval={interval}, "
            f"policy={policy}, target_time={target_time})"
        )

    def _is_new_period(
        self, interval: str, prev_date: datetime.date, next_date: datetime.date
    ) -> bool:
        """Determine if a new scheduling period has started between two dates."""
        if interval == "daily":
            return next_date > prev_date

        if interval == "weekly":
            return next_date.isocalendar()[:2] != prev_date.isocalendar()[:2]

        if interval == "semi_monthly":
            prev_period = (
                prev_date.year,
                prev_date.month,
                1 if prev_date.day < 15 else 2,
            )
            next_period = (
                next_date.year,
                next_date.month,
                1 if next_date.day < 15 else 2,
            )
            return next_period != prev_period

        if interval == "monthly":
            return (next_date.year, next_date.month) != (
                prev_date.year,
                prev_date.month,
            )

        if interval == "bi_monthly":
            prev_bi = (prev_date.year, (prev_date.month - 1) // 2)
            next_bi = (next_date.year, (next_date.month - 1) // 2)
            return next_bi != prev_bi

        return False

    def get_next_due_date(
        self, task: PullTask, now_et: datetime.datetime
    ) -> datetime.date | None:
        """Calculate the next date for which the task is due to run.

        Returns None if the task is not due.
        """
        current_date = now_et.date()
        _, last_run_date = self.state_store.get_last_run(task.name)

        if last_run_date is None:
            # If the task has never run, determine the starting reference point
            start_ref = task.start_date if task.start_date is not None else current_date
            # Check date-by-date backward to find the first valid run target
            d = current_date
            while d >= start_ref:
                if task.run_on_non_trading_days or self.calendar.is_trading_day(d):
                    if d < current_date:
                        return d
                    if d == current_date:
                        if now_et.time() >= task.target_time:
                            return d
                    break
                d -= datetime.timedelta(days=1)
            return None

        # Find the next period date to run
        d = last_run_date + datetime.timedelta(days=1)
        while d <= current_date:
            if task.run_on_non_trading_days or self.calendar.is_trading_day(d):
                if self._is_new_period(task.interval, last_run_date, d):
                    if d < current_date:
                        return d
                    if d == current_date:
                        if now_et.time() >= task.target_time:
                            return d
            d += datetime.timedelta(days=1)

        return None

    async def _execute_task(self, task: PullTask, run_date: datetime.date) -> None:
        """Execute a task function, resolving sync/async and parameters."""
        sig = inspect.signature(task.func)
        has_date_param = "run_date" in sig.parameters or any(
            p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values()
        )

        is_async = asyncio.iscoroutinefunction(task.func) or (
            callable(task.func)
            and asyncio.iscoroutinefunction(getattr(task.func, "__call__", None))  # noqa: B004
        )

        if is_async:
            if has_date_param:
                await task.func(run_date)
            else:
                try:
                    await task.func(run_date)
                except TypeError:
                    await task.func()
        else:
            loop = asyncio.get_running_loop()
            if has_date_param:
                await loop.run_in_executor(None, task.func, run_date)
            else:

                def wrapper() -> Any:
                    try:
                        return task.func(run_date)
                    except TypeError:
                        return task.func()

                await loop.run_in_executor(None, wrapper)

    async def check_and_run_once(
        self, current_time: datetime.datetime | None = None
    ) -> int:
        """Scan all registered tasks, check due status, and execute due tasks.

        Returns the number of tasks successfully executed.
        """
        now_et = (
            current_time.astimezone(MARKET_TZ)
            if current_time
            else datetime.datetime.now(MARKET_TZ)
        )
        executed_count = 0

        for task in self.tasks.values():
            run_date = self.get_next_due_date(task, now_et)
            if run_date is None:
                continue

            # Respect policy boundaries
            if task.policy == "during_hours":
                if not self.calendar.is_market_open(now_et):
                    logger.debug(
                        f"Task '{task.name}' is due for {run_date} but market is closed."
                    )
                    continue
            elif task.policy == "after_hours":
                if self.calendar.is_market_open(now_et):
                    logger.debug(
                        f"Task '{task.name}' is due for {run_date} but market is open."
                    )
                    continue

            logger.info(f"Executing task '{task.name}' for logical date: {run_date}")
            try:
                await self._execute_task(task, run_date)
                self.state_store.update_last_run(task.name, now_et, run_date)
                executed_count += 1
                logger.info(f"Successfully completed task '{task.name}' for {run_date}")
            except Exception as e:
                logger.exception(
                    f"Error executing task '{task.name}' for logical date {run_date}: {e}"
                )

        return executed_count

    async def start(self, interval_seconds: float = 60.0) -> None:
        """Run the scheduling coordinator loop continuously."""
        logger.info("Starting scheduled pull coordinator loop.")
        try:
            while True:
                await self.check_and_run_once()
                await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            logger.info("Scheduled pull coordinator loop cancelled.")
            raise
