"""Security Master implementation for Stockodile."""

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

from stockodile.reference.models import Security, TickerMapping
from stockodile.schema.enums import SecurityType


def _now_iso() -> str:
    """Get the current UTC time as an ISO-8601 string."""
    return datetime.now(UTC).isoformat()


def _parse_iso(iso_str: str) -> datetime:
    """Parse an ISO-8601 string into a UTC datetime object."""
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


class SecurityMaster:
    """Database registry and manager for securities and ticker mappings."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        # Enable foreign key support in SQLite
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database schema and indexes."""
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS securities (
                    symbol TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    name TEXT,
                    security_type TEXT NOT NULL,
                    cik TEXT,
                    figi TEXT,
                    cusip TEXT,
                    isin TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_securities_ticker ON securities (ticker);
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_securities_cik ON securities (cik);
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_securities_figi ON securities (figi);
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_securities_cusip ON securities (cusip);
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS ticker_mappings (
                    ticker TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    exchange TEXT,
                    PRIMARY KEY (ticker, symbol),
                    FOREIGN KEY (symbol) REFERENCES securities (symbol) ON DELETE CASCADE
                )
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticker_mappings_symbol ON ticker_mappings (symbol);
            """)

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: Exception | None,
        exc_tb: type | None,
    ) -> None:
        self.close()


    def _row_to_security(self, row: sqlite3.Row) -> Security:
        """Convert a database Row object to a Security struct."""
        sec_type_str = row["security_type"]
        try:
            sec_type = SecurityType(sec_type_str)
        except ValueError:
            sec_type = SecurityType.UNKNOWN

        return Security(
            symbol=row["symbol"],
            ticker=row["ticker"],
            exchange=row["exchange"],
            name=row["name"],
            security_type=sec_type,
            cik=row["cik"],
            figi=row["figi"],
            cusip=row["cusip"],
            isin=row["isin"],
            is_active=bool(row["is_active"]),
            created_at=_parse_iso(row["created_at"]),
            updated_at=_parse_iso(row["updated_at"]),
        )

    def register_security(self, security: Security) -> None:
        """Register a new security or update an existing one.

        Also automatically registers a ticker mapping for the security's ticker.
        """
        now = _now_iso()
        created_at = security.created_at.isoformat() if security.created_at else now
        updated_at = security.updated_at.isoformat() if security.updated_at else now

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO securities (
                    symbol, ticker, exchange, name, security_type,
                    cik, figi, cusip, isin, is_active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    ticker=excluded.ticker,
                    exchange=excluded.exchange,
                    name=excluded.name,
                    security_type=excluded.security_type,
                    cik=excluded.cik,
                    figi=excluded.figi,
                    cusip=excluded.cusip,
                    isin=excluded.isin,
                    is_active=excluded.is_active,
                    updated_at=excluded.updated_at
                """,
                (
                    security.symbol,
                    security.ticker,
                    security.exchange,
                    security.name,
                    security.security_type.value,
                    security.cik,
                    security.figi,
                    security.cusip,
                    security.isin,
                    1 if security.is_active else 0,
                    created_at,
                    updated_at,
                ),
            )
            # Register mapping for ticker -> symbol
            self.conn.execute(
                """
                INSERT OR IGNORE INTO ticker_mappings (ticker, symbol, exchange)
                VALUES (?, ?, ?)
                """,
                (security.ticker, security.symbol, security.exchange),
            )

    def add_ticker_mapping(self, ticker: str, symbol: str, exchange: str | None = None) -> None:
        """Map a ticker to a symbol (useful for historical ticker changes)."""
        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO ticker_mappings (ticker, symbol, exchange)
                VALUES (?, ?, ?)
                """,
                (ticker, symbol, exchange),
            )

    def remove_ticker_mapping(self, ticker: str, symbol: str) -> None:
        """Remove a ticker to symbol mapping."""
        with self.conn:
            self.conn.execute(
                "DELETE FROM ticker_mappings WHERE ticker = ? AND symbol = ?",
                (ticker, symbol),
            )

    def get_by_symbol(self, symbol: str) -> Security | None:
        """Retrieve a security by its unique symbol."""
        cursor = self.conn.execute("SELECT * FROM securities WHERE symbol = ?", (symbol,))
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_security(row)

    def get_by_figi(self, figi: str) -> Security | None:
        """Retrieve a security by its FIGI identifier."""
        cursor = self.conn.execute("SELECT * FROM securities WHERE figi = ?", (figi,))
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_security(row)

    def get_by_cusip(self, cusip: str) -> Security | None:
        """Retrieve a security by its CUSIP identifier."""
        cursor = self.conn.execute("SELECT * FROM securities WHERE cusip = ?", (cusip,))
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_security(row)

    def get_by_cik(self, cik: str) -> list[Security]:
        """Retrieve securities associated with a CIK."""
        cursor = self.conn.execute("SELECT * FROM securities WHERE cik = ?", (cik,))
        return [self._row_to_security(row) for row in cursor.fetchall()]

    def get_by_ticker(self, ticker: str, exchange: str | None = None) -> list[Security]:
        """Find securities associated with a ticker, either directly or via mapping."""
        query = """
            SELECT DISTINCT s.*
            FROM securities s
            JOIN ticker_mappings m ON s.symbol = m.symbol
            WHERE m.ticker = ?
        """
        params: list[str] = [ticker]
        if exchange is not None:
            query += " AND (m.exchange = ? OR s.exchange = ?)"
            params.extend([exchange, exchange])

        cursor = self.conn.execute(query, params)
        return [self._row_to_security(row) for row in cursor.fetchall()]

    def update_security_status(self, symbol: str, is_active: bool) -> None:
        """Activate or deactivate a security symbol."""
        now = _now_iso()
        with self.conn:
            cursor = self.conn.execute(
                "UPDATE securities SET is_active = ?, updated_at = ? WHERE symbol = ?",
                (1 if is_active else 0, now, symbol),
            )
            if cursor.rowcount == 0:
                raise ValueError(f"Security with symbol '{symbol}' not found.")

    def list_securities(self, active_only: bool = True) -> list[Security]:
        """List securities in the database."""
        if active_only:
            cursor = self.conn.execute("SELECT * FROM securities WHERE is_active = 1")
        else:
            cursor = self.conn.execute("SELECT * FROM securities")
        return [self._row_to_security(row) for row in cursor.fetchall()]

    def get_ticker_mappings(self, symbol: str) -> list[TickerMapping]:
        """Get all ticker mappings for a specific symbol."""
        cursor = self.conn.execute(
            "SELECT ticker, symbol, exchange FROM ticker_mappings WHERE symbol = ?",
            (symbol,),
        )
        return [
            TickerMapping(ticker=row["ticker"], symbol=row["symbol"], exchange=row["exchange"])
            for row in cursor.fetchall()
        ]

    def resolve_ticker(self, ticker: str, exchange: str | None = None) -> str | None:
        """Resolve a ticker to its standard security symbol.

        If an exchange is specified, it will try to find a mapping matching that exchange.
        """
        if exchange is not None:
            cursor = self.conn.execute(
                """
                SELECT symbol FROM ticker_mappings
                WHERE ticker = ? AND (exchange = ? OR exchange IS NULL)
                ORDER BY exchange DESC
                LIMIT 1
                """,
                (ticker, exchange),
            )
        else:
            cursor = self.conn.execute(
                "SELECT symbol FROM ticker_mappings WHERE ticker = ? LIMIT 1",
                (ticker,),
            )
        row = cursor.fetchone()
        if row is not None:
            return str(row["symbol"])
        return None
