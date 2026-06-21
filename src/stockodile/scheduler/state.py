"""State management for the scheduled pull coordinator."""

import datetime
import os
from abc import ABC, abstractmethod

import msgspec


class TaskStateRecord(msgspec.Struct):
    """Serialization model for task state."""

    last_run_timestamp: str | None = None  # ISO format datetime
    last_run_date: str | None = None       # ISO format date


class SchedulerStateStore(ABC):
    """Abstract base class for storing scheduler execution state."""

    @abstractmethod
    def get_last_run(
        self, task_name: str
    ) -> tuple[datetime.datetime | None, datetime.date | None]:
        """Retrieve (last_run_timestamp, last_run_date) for a task."""
        pass

    @abstractmethod
    def update_last_run(
        self, task_name: str, run_time: datetime.datetime, run_date: datetime.date
    ) -> None:
        """Persist the last run details for a task."""
        pass


class InMemoryStateStore(SchedulerStateStore):
    """In-memory implementation of state store, useful for tests/ephemeral runs."""

    def __init__(self) -> None:
        self._states: dict[str, tuple[datetime.datetime, datetime.date]] = {}

    def get_last_run(
        self, task_name: str
    ) -> tuple[datetime.datetime | None, datetime.date | None]:
        if task_name in self._states:
            return self._states[task_name]
        return None, None

    def update_last_run(
        self, task_name: str, run_time: datetime.datetime, run_date: datetime.date
    ) -> None:
        self._states[task_name] = (run_time, run_date)


class JSONFileStateStore(SchedulerStateStore):
    """JSON file-based implementation of state store for persistence across restarts."""

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath
        self._cache: dict[str, TaskStateRecord] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.filepath):
            self._cache = {}
            return
        try:
            with open(self.filepath, "rb") as f:
                data = f.read()
                if not data:
                    self._cache = {}
                    return
                self._cache = msgspec.json.decode(data, type=dict[str, TaskStateRecord])
        except Exception:
            # Fallback to empty if decoding fails or file is corrupted
            self._cache = {}

    def _save(self) -> None:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(os.path.abspath(self.filepath)), exist_ok=True)
        with open(self.filepath, "wb") as f:
            f.write(msgspec.json.encode(self._cache))

    def get_last_run(
        self, task_name: str
    ) -> tuple[datetime.datetime | None, datetime.date | None]:
        record = self._cache.get(task_name)
        if not record:
            return None, None

        ts = None
        dt = None
        if record.last_run_timestamp:
            try:
                ts = datetime.datetime.fromisoformat(record.last_run_timestamp)
            except ValueError:
                pass
        if record.last_run_date:
            try:
                dt = datetime.date.fromisoformat(record.last_run_date)
            except ValueError:
                pass

        return ts, dt

    def update_last_run(
        self, task_name: str, run_time: datetime.datetime, run_date: datetime.date
    ) -> None:
        self._cache[task_name] = TaskStateRecord(
            last_run_timestamp=run_time.isoformat(),
            last_run_date=run_date.isoformat(),
        )
        self._save()
