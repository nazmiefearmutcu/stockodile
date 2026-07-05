import logging
import sqlite3
from collections import deque

import msgspec

log = logging.getLogger(__name__)


class DeadLetter(msgspec.Struct, frozen=True):
    local_ts: int
    raw: bytes
    error_type: str
    traceback: str


class DeadLetterQueue:
    def __init__(self, max_size: int = 10_000, db_path: str | None = None) -> None:
        self._dq: deque[DeadLetter] = deque(maxlen=max_size)
        self.db_path = db_path
        if self.db_path is not None:
            self._init_db()

    def _init_db(self) -> None:
        if self.db_path is None:
            return
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dead_letters (
                        local_ts INTEGER,
                        raw BLOB,
                        error_type TEXT,
                        traceback TEXT
                    )
                    """
                )
                conn.commit()
                maxlen = self._dq.maxlen
                limit = maxlen if maxlen is not None else -1
                cursor = conn.execute(
                    "SELECT local_ts, raw, error_type, traceback "
                    "FROM dead_letters ORDER BY rowid DESC LIMIT ?",
                    (limit,),
                )
                rows = cursor.fetchall()
                for local_ts, raw, error_type, traceback in reversed(rows):
                    self._dq.append(
                        DeadLetter(
                            local_ts=local_ts,
                            raw=raw,
                            error_type=error_type,
                            traceback=traceback,
                        )
                    )
        except Exception as exc:
            log.error("Failed to initialize or load from SQLite dead letter queue: %s", exc)

    def put(self, local_ts: int, raw: bytes, error_type: str, traceback: str) -> None:
        maxlen = self._dq.maxlen
        if maxlen is not None and len(self._dq) >= maxlen:
            oldest = self._dq[0]
            log.warning(
                "DeadLetterQueue overflow. Evicting oldest dead letter: local_ts=%d, error_type=%s",
                oldest.local_ts,
                oldest.error_type,
            )

        dl = DeadLetter(local_ts=local_ts, raw=raw, error_type=error_type, traceback=traceback)
        self._dq.append(dl)

        if self.db_path is not None:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        "INSERT INTO dead_letters "
                        "(local_ts, raw, error_type, traceback) VALUES (?, ?, ?, ?)",
                        (local_ts, raw, error_type, traceback),
                    )
                    conn.commit()
            except Exception as exc:
                log.error("Failed to write dead letter to SQLite: %s", exc)

    def drain(self) -> list[DeadLetter]:
        items = list(self._dq)
        self._dq.clear()
        if self.db_path is not None:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("DELETE FROM dead_letters")
                    conn.commit()
            except Exception as exc:
                log.error("Failed to clear dead letters from SQLite: %s", exc)
        return items
