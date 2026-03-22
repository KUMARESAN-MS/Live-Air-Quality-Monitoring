"""
tests/test_decision_engine.py
──────────────────────────────
Unit tests for the decision engine logic.
Must pass before Phase 3 integration.
"""

import sys
from pathlib import Path

# Add project root to sys.path for isolated test execution
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.decision_engine import generate_insight, generate_alert


# ─── Insight Tests ────────────────────────────────────────────────────────────

def test_insight_baseline_insufficient_data():
    """Short history should return baseline message."""
    assert generate_insight([10.0], 10.0, "→") == "Establishing baseline data pattern..."


def test_insight_stable_good():
    """Stable, low AQI should return excellent message."""
    history = [40.0, 42.0, 41.0, 40.0, 39.0, 40.0]
    result = generate_insight(history, 40.0, "→")
    assert result == "Air quality is stable and excellent for outdoor activities."


def test_insight_stable_moderate():
    """Stable trend above 50 should return generic stable message."""
    history = [70.0, 72.0, 71.0, 70.0, 69.0, 70.0]
    result = generate_insight(history, 70.0, "→")
    assert result == "Stable air quality patterns observed."


def test_insight_rising_elevated():
    """Rising trend with AQI > 100 should warn of worsening conditions."""
    history = [120.0, 125.0, 130.0, 135.0, 140.0, 145.0]
    result = generate_insight(history, 145.0, "↑")
    assert result == "Pollution levels are increasing steadily. Expect worsening conditions."


def test_insight_rising_low():
    """Rising trend with AQI <= 100 should give a minor upward message."""
    history = [30.0, 32.0, 34.0, 36.0, 38.0, 40.0]
    result = generate_insight(history, 40.0, "↑")
    assert result == "Noticeable upward trend in particulate matter."


def test_insight_high_variance():
    """High variance (range > 30) should flag rapid changes."""
    history = [100.0, 150.0, 110.0, 160.0, 120.0, 170.0]
    result = generate_insight(history, 170.0, "→")
    assert result == "High variability in air quality detected. Conditions are changing rapidly."


def test_insight_critical():
    """AQI > 200 should trigger critical insight regardless of trend."""
    history = [210.0, 220.0, 230.0, 240.0, 250.0, 260.0]
    result = generate_insight(history, 260.0, "→")
    assert result == "Critically high pollution levels detected in this area."


def test_insight_improving_elevated():
    """Downward trend with AQI still above 100."""
    history = [140.0, 135.0, 130.0, 125.0, 120.0, 115.0]
    result = generate_insight(history, 115.0, "↓")
    assert result == "Air quality is beginning to improve, but remains elevated."


def test_insight_downward_normal():
    """Downward trend with AQI below 100."""
    history = [60.0, 58.0, 56.0, 54.0, 52.0, 50.0]
    result = generate_insight(history, 50.0, "↓")
    assert result == "Slight downward drift in pollution detected."


# ─── Alert Tests ──────────────────────────────────────────────────────────────

def test_alert_spike():
    """Spike >25 AQI across recent readings should trigger spike alert."""
    history = [50.0, 60.0, 70.0, 80.0, 90.0]
    assert generate_alert(90.0, history) == "⚠️ Rapid Pollution Spike!"


def test_alert_hazardous():
    """AQI > 300 should trigger emergency alert."""
    assert generate_alert(350.0, [300.0, 310.0, 320.0]) == "🚨 EMERGENCY: Hazardous Air Quality"


def test_alert_very_unhealthy():
    """AQI > 200 should trigger very unhealthy alert."""
    assert generate_alert(250.0, [240.0, 245.0, 250.0]) == "🔴 Very Unhealthy Air Quality"


def test_alert_unhealthy():
    """AQI > 150 should trigger unhealthy alert."""
    assert generate_alert(180.0, [170.0, 175.0, 180.0]) == "🔴 Unhealthy Air Quality"


def test_alert_sensitive():
    """AQI > 100 should trigger sensitive groups alert."""
    assert generate_alert(120.0, [115.0, 118.0, 120.0]) == "🟠 Sensitive Groups at Risk"


def test_alert_none_healthy():
    """Healthy AQI should trigger no alert."""
    assert generate_alert(40.0, [38.0, 39.0, 40.0]) is None


def test_alert_none_insufficient_data():
    """Insufficient data should trigger no alert."""
    assert generate_alert(40.0, [40.0]) is None
