"""
tests/test_message_composer.py
──────────────────────────────
Unit tests for the structured message composer and priority scoring.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.message_composer import compose_message, compute_priority


# ── compose_message tests ────────────────────────────────────────────────────

def test_good_stable():
    msg = compose_message(aqi=35, trend="→", prediction=37, traffic="Low", category="Good")
    assert msg["severity"] == "good"
    assert "excellent" in msg["title"].lower() or "good" in msg["title"].lower()
    assert msg["confidence"] == "low"  # no history


def test_good_rising():
    msg = compose_message(aqi=42, trend="↑", prediction=55, traffic="Low", category="Good")
    assert msg["severity"] == "good"
    assert "change" in msg["title"].lower() or "sign" in msg["title"].lower()


def test_moderate_falling():
    msg = compose_message(aqi=75, trend="↓", prediction=60, traffic="Medium", category="Moderate")
    assert msg["severity"] == "moderate"
    assert "improv" in msg["title"].lower()


def test_sensitive_worsening():
    msg = compose_message(aqi=120, trend="↑", prediction=145, traffic="High", category="Sensitive")
    assert msg["severity"] == "sensitive"
    assert "worsening" in msg["title"].lower()
    assert "traffic" in msg["summary"].lower()


def test_unhealthy_worsening():
    msg = compose_message(aqi=160, trend="↑", prediction=185, traffic="High", category="Unhealthy")
    assert msg["severity"] == "unhealthy"
    assert "worsening" in msg["title"].lower()
    assert "traffic" in msg["summary"].lower()


def test_hazardous_stable():
    msg = compose_message(aqi=350, trend="→", prediction=340, traffic="Low", category="Hazardous")
    assert msg["severity"] == "hazardous"
    assert "emergency" in msg["title"].lower() or "hazardous" in msg["title"].lower()
    assert "indoors" in msg["advice"].lower()


def test_prediction_note_rising_significantly():
    msg = compose_message(aqi=100, trend="↑", prediction=125, traffic="Low", category="Moderate")
    assert "worsen" in msg["prediction_note"].lower() or "rise" in msg["prediction_note"].lower()


def test_prediction_note_improving():
    msg = compose_message(aqi=150, trend="↓", prediction=120, traffic="Low", category="Sensitive")
    assert "improve" in msg["prediction_note"].lower()


def test_prediction_note_stable():
    msg = compose_message(aqi=80, trend="→", prediction=82, traffic="Low", category="Moderate")
    assert "remain" in msg["prediction_note"].lower() or "current" in msg["prediction_note"].lower()


def test_confidence_high():
    msg = compose_message(aqi=50, trend="→", prediction=52, traffic="Low", category="Good", history_len=20)
    assert msg["confidence"] == "high"


def test_confidence_medium():
    msg = compose_message(aqi=50, trend="→", prediction=52, traffic="Low", category="Good", history_len=10)
    assert msg["confidence"] == "medium"


def test_all_fields_present():
    msg = compose_message(aqi=120, trend="↑", prediction=140, traffic="High", category="Sensitive")
    required = {"severity", "title", "summary", "prediction_note", "advice", "confidence"}
    assert required.issubset(msg.keys())


# ── compute_priority tests ───────────────────────────────────────────────────

def test_priority_critical():
    p = compute_priority(aqi=300, trend="↑", prediction=350)
    assert p["priority"] == "critical"
    assert p["score"] >= 150


def test_priority_high():
    p = compute_priority(aqi=200, trend="↑", prediction=210)
    assert p["priority"] == "high"


def test_priority_medium():
    p = compute_priority(aqi=100, trend="→", prediction=100)
    assert p["priority"] == "medium"


def test_priority_low():
    p = compute_priority(aqi=30, trend="↓", prediction=25)
    assert p["priority"] == "low"


def test_rising_trend_boosts_priority():
    p_flat   = compute_priority(aqi=120, trend="→", prediction=120)
    p_rising = compute_priority(aqi=120, trend="↑", prediction=120)
    assert p_rising["score"] > p_flat["score"]
