"""
tests/test_storage.py
─────────────────────
Unit tests for the SQLite storage layer.
Run: python -m pytest tests/test_storage.py -v
"""

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest


@pytest.fixture(autouse=True)
def tmp_db(monkeypatch, tmp_path):
    """Use a temporary database for each test."""
    db_file = str(tmp_path / "test_aqi.db")
    monkeypatch.setattr("config.settings.DB_PATH", db_file)

    # Reset module state
    import storage.storage as st
    st._initialized = False
    st.DB_PATH = db_file

    # Re-import to pick up new DB_PATH
    import importlib
    importlib.reload(st)
    st._initialized = False

    yield db_file


class TestStorageInsertQuery:
    def test_insert_and_query(self):
        from storage.storage import init_db, insert_reading, query_history
        from datetime import datetime, timezone

        init_db()

        reading = {
            "city": "TestCity",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "aqi": 75.0,
            "pm25": 23.5,
            "pm10": 45.0,
            "no2": 30.0,
            "o3": 40.0,
            "co": 0.5,
            "so2": 8.0,
            "trend": "→",
            "next_hour_aqi": 80.0,
            "is_real": True,
            "category": "Moderate",
            "source": "Test",
        }

        insert_reading(reading)
        results = query_history("TestCity", hours=1)

        assert len(results) == 1
        assert results[0]["city"] == "TestCity"
        assert results[0]["aqi"] == 75.0
        assert results[0]["is_real"] == 1

    def test_query_empty_city(self):
        from storage.storage import init_db, query_history

        init_db()
        results = query_history("NonExistent", hours=1)
        assert results == []

    def test_multiple_cities(self):
        from storage.storage import init_db, insert_reading, query_latest
        from datetime import datetime, timezone

        init_db()

        for city in ["Delhi", "London", "Tokyo"]:
            insert_reading({
                "city": city,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "aqi": 100.0,
                "trend": "→",
                "category": "Moderate",
            })

        latest = query_latest()
        cities = {r["city"] for r in latest}
        assert "Delhi" in cities
        assert "London" in cities
        assert "Tokyo" in cities

    def test_reading_count(self):
        from storage.storage import init_db, insert_reading, get_reading_count
        from datetime import datetime, timezone

        init_db()
        assert get_reading_count() == 0

        for i in range(5):
            insert_reading({
                "city": "Delhi",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "aqi": 50.0 + i * 10,
                "trend": "↑",
                "category": "Moderate",
            })

        assert get_reading_count() == 5


class TestStoragePersistence:
    def test_data_survives_reconnection(self, tmp_db):
        """Data persists across separate connections (simulates restart)."""
        from storage.storage import init_db, insert_reading
        from datetime import datetime, timezone

        init_db()
        insert_reading({
            "city": "PersistCity",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "aqi": 42.0,
            "trend": "→",
            "category": "Good",
        })

        # Simulate restart by querying fresh
        import sqlite3
        conn = sqlite3.connect(tmp_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM aqi_readings WHERE city='PersistCity'")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 1
        assert dict(rows[0])["aqi"] == 42.0
