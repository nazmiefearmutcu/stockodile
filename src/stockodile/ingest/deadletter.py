import logging
import sqlite3
import threading
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
        if max_size < 1:
            raise ValueError(f"max_size must be >= 1, got {max_size}")
        self._dq: deque[DeadLetter] = deque(maxlen=max_size)
        self.db_path = db_path
        self._lock = threading.Lock()
        if self.db_path is not None:
            self._init_db()

    def _connect(self) -> sqlite3.Connection:
        assert self.db_path is not None
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        if self.db_path is None:
            return
        try:
            with self._connect() as conn:
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
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_dead_letters_rowid "
                    "ON dead_letters(rowid)"
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
                # Trim durable store to max_size on load
                self._trim_db(conn, maxlen)
                conn.commit()
        except Exception as exc:
            log.error("Failed to initialize or load from SQLite dead letter queue: %s", exc)

    def _trim_db(self, conn: sqlite3.Connection, maxlen: int | None) -> None:
        if maxlen is None:
            return
        count = conn.execute("SELECT COUNT(*) FROM dead_letters").fetchone()[0]
        excess = count - maxlen
        if excess > 0:
            conn.execute(
                "DELETE FROM dead_letters WHERE rowid IN ("
                "SELECT rowid FROM dead_letters ORDER BY rowid ASC LIMIT ?"
                ")",
                (excess,),
            )

    def put(self, local_ts: int, raw: bytes, error_type: str, traceback: str) -> None:
        with self._lock:
            maxlen = self._dq.maxlen
            if maxlen is not None and len(self._dq) >= maxlen and self._dq:
                oldest = self._dq[0]
                log.warning(
                    "DeadLetterQueue overflow. Evicting oldest dead letter: "
                    "local_ts=%d, error_type=%s",
                    oldest.local_ts,
                    oldest.error_type,
                )

            dl = DeadLetter(
                local_ts=local_ts, raw=raw, error_type=error_type, traceback=traceback
            )
            self._dq.append(dl)

            if self.db_path is not None:
                try:
                    with self._connect() as conn:
                        conn.execute(
                            "INSERT INTO dead_letters "
                            "(local_ts, raw, error_type, traceback) VALUES (?, ?, ?, ?)",
                            (local_ts, raw, error_type, traceback),
                        )
                        self._trim_db(conn, maxlen)
                        conn.commit()
                except Exception as exc:
                    log.error("Failed to write dead letter to SQLite: %s", exc)

    def drain(self) -> list[DeadLetter]:
        with self._lock:
            items = list(self._dq)
            if self.db_path is not None:
                try:
                    with self._connect() as conn:
                        conn.execute("DELETE FROM dead_letters")
                        conn.commit()
                except Exception as exc:
                    log.error(
                        "Failed to clear dead letters from SQLite: %s "
                        "(memory drain aborted to avoid duplicate replay)",
                        exc,
                    )
                    # Do not clear memory if durable clear failed
                    return items
            self._dq.clear()
            return items
