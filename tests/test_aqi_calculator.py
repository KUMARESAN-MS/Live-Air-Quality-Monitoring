"""
tests/test_aqi_calculator.py
─────────────────────────────
Unit tests for AQI formula and trend detection.
Run: python -m pytest tests/ -v
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from spark.aqi_stream_processor_simple import pm25_to_aqi, detect_trend, aqi_category


# ─── EPA AQI Breakpoint Tests ─────────────────────────────────────────────────
class TestPm25ToAqi:
    """Verify EPA linear interpolation against known reference values."""

    def test_good_midpoint(self):
        # PM2.5 = 6.0 µg/m³ → AQI should be ~25 (midpoint of 0-50)
        aqi = pm25_to_aqi(6.0)
        assert 20 <= aqi <= 30, f"Expected ~25 for PM2.5=6.0, got {aqi:.1f}"

    def test_good_lower_bound(self):
        assert pm25_to_aqi(0.0) == 0.0

    def test_good_upper_bound(self):
        aqi = pm25_to_aqi(12.0)
        assert abs(aqi - 50.0) < 1.0, f"PM2.5=12.0 should map to ~50 AQI, got {aqi:.1f}"

    def test_moderate_range(self):
        # PM2.5 = 23.8 → midpoint of 12.1-35.4 → AQI ~ 75
        aqi = pm25_to_aqi(23.8)
        assert 65 <= aqi <= 85, f"Expected ~75 for PM2.5=23.8, got {aqi:.1f}"

    def test_unhealthy_for_sensitive(self):
        # PM2.5 = 45.0 → should be in 101-150 range
        aqi = pm25_to_aqi(45.0)
        assert 101 <= aqi <= 150, f"Expected 101-150 for PM2.5=45.0, got {aqi:.1f}"

    def test_unhealthy(self):
        # PM2.5 = 100.0 → should be 151-200 range
        aqi = pm25_to_aqi(100.0)
        assert 151 <= aqi <= 200, f"Expected 151-200 for PM2.5=100, got {aqi:.1f}"

    def test_very_unhealthy(self):
        # PM2.5 = 200.0 → should be 201-300 range
        aqi = pm25_to_aqi(200.0)
        assert 201 <= aqi <= 300, f"Expected 201-300 for PM2.5=200, got {aqi:.1f}"

    def test_hazardous_clamp(self):
        aqi = pm25_to_aqi(500.0)
        assert aqi >= 400, f"PM2.5=500 should give AQI >= 400, got {aqi:.1f}"

    def test_negative_input(self):
        # Negative PM2.5 should not crash
        aqi = pm25_to_aqi(-5.0)
        assert aqi >= 0


# ─── Category Label Tests ─────────────────────────────────────────────────────
class TestAqiCategory:
    def test_good(self):         assert aqi_category(25)["css"]  == "good"
    def test_moderate(self):     assert aqi_category(75)["css"]  == "moderate"
    def test_sensitive(self):    assert aqi_category(125)["css"] == "sensitive"
    def test_unhealthy(self):    assert aqi_category(175)["css"] == "unhealthy"
    def test_very_unhealthy(self): assert aqi_category(250)["css"] == "very-unhealthy"
    def test_hazardous(self):    assert aqi_category(350)["css"] == "hazardous"


# ─── Trend Detection Tests ────────────────────────────────────────────────────
class TestTrendDetection:
    def test_rising_trend(self):
        # AQI climbing fast → ↑
        history = [100, 105, 110, 115, 120, 130]
        assert detect_trend(history) == "↑"

    def test_falling_trend(self):
        # AQI dropping fast → ↓
        history = [130, 125, 118, 112, 108, 100]
        assert detect_trend(history) == "↓"

    def test_flat_trend(self):
        # AQI stable within ±5 → →
        history = [100, 101, 100, 102, 100, 101]
        assert detect_trend(history) == "→"

    def test_insufficient_data(self):
        # Less than 3 data points → flat
        assert detect_trend([100]) == "→"
        assert detect_trend([100, 200]) == "→"

    def test_empty(self):
        assert detect_trend([]) == "→"
