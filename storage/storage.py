"""
storage/storage.py
──────────────────
Persistent storage layer using SQLite.
Stores all processed AQI readings for historical queries and data persistence.

Thread-safe — uses connection-per-call pattern for multi-threaded access.
"""

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DB_PATH

_init_lock = threading.Lock()
_initialized = False


def _get_connection() -> sqlite3.Connection:
    """Get a new SQLite connection (thread-safe: one connection per call)."""
    db_path = Path(DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # better concurrent read performance
    return conn


def init_db():
    """Create the readings table if it doesn't exist."""
    global _initialized
    if _initialized:
        return

    with _init_lock:
        if _initialized:
            return

        conn = _get_connection()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS aqi_readings (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    city       TEXT    NOT NULL,
                    timestamp  TEXT    NOT NULL,
                    aqi        REAL    NOT NULL,
                    pm25       REAL,
                    pm10       REAL,
                    no2        REAL,
                    o3         REAL,
                    co         REAL,
                    so2        REAL,
                    trend      TEXT,
                    prediction REAL,
                    is_real    INTEGER DEFAULT 0,
                    category   TEXT,
                    source     TEXT
                )
            """)
            # Index for common queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_city_timestamp
                ON aqi_readings (city, timestamp)
            """)
            conn.commit()
            _initialized = True
            print(f"[Storage] SQLite database initialized at {DB_PATH}")
        finally:
            conn.close()


def insert_reading(data: dict):
    """Insert a single processed AQI reading into the database."""
    init_db()
    conn = _get_connection()
    try:
        conn.execute("""
            INSERT INTO aqi_readings
                (city, timestamp, aqi, pm25, pm10, no2, o3, co, so2,
                 trend, prediction, is_real, category, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("city"),
            data.get("timestamp"),
            data.get("aqi"),
            data.get("pm25"),
            data.get("pm10"),
            data.get("no2"),
            data.get("o3"),
            data.get("co"),
            data.get("so2"),
            data.get("trend"),
            data.get("next_hour_aqi") or data.get("prediction"),
            1 if data.get("is_real") else 0,
            data.get("category"),
            data.get("source", ""),
        ))
        conn.commit()
    except Exception as e:
        print(f"[Storage] Insert error: {e}")
    finally:
        conn.close()


def query_history(city: str, hours: int = 24) -> list[dict]:
    """Query historical readings for a city within the last N hours."""
    init_db()
    conn = _get_connection()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        cursor = conn.execute("""
            SELECT city, timestamp, aqi, pm25, pm10, no2, o3, co, so2,
                   trend, prediction, is_real, category, source
            FROM aqi_readings
            WHERE city = ? AND timestamp >= ?
            ORDER BY timestamp ASC
        """, (city, cutoff))
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def query_latest() -> list[dict]:
    """Get the most recent reading per city."""
    init_db()
    conn = _get_connection()
    try:
        cursor = conn.execute("""
            SELECT r.city, r.timestamp, r.aqi, r.pm25, r.pm10, r.no2, r.o3,
                   r.co, r.so2, r.trend, r.prediction, r.is_real, r.category, r.source
            FROM aqi_readings r
            INNER JOIN (
                SELECT city, MAX(timestamp) as max_ts
                FROM aqi_readings
                GROUP BY city
            ) latest ON r.city = latest.city AND r.timestamp = latest.max_ts
        """)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_reading_count() -> int:
    """Get total number of stored readings."""
    init_db()
    conn = _get_connection()
    try:
        cursor = conn.execute("SELECT COUNT(*) FROM aqi_readings")
        return cursor.fetchone()[0]
    finally:
        conn.close()
