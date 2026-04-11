"""
spark/aqi_stream_processor_simple.py
──────────────────────────────────────
AQI utility functions (used by tests and shared across modules).

NOTE: The simulation processing loop has been removed.
      All processing now flows through:
        Producer → Kafka (raw-aqi) → Spark → Kafka (processed-aqi) → Dashboard

This file is kept ONLY for:
  - pm25_to_aqi()    — EPA breakpoint calculation
  - aqi_category()   — AQI → category mapping
  - detect_trend()   — trend detection from AQI history
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    PM25_BREAKPOINTS,
    AQI_CATEGORIES,
)


# ─── AQI helpers (used by tests and other modules) ──────────────────────────
def pm25_to_aqi(pm25: float) -> float:
    """EPA linear interpolation: PM2.5 µg/m³ → AQI."""
    for lo_pm, hi_pm, lo_aqi, hi_aqi in PM25_BREAKPOINTS:
        if lo_pm <= pm25 <= hi_pm:
            return lo_aqi + (pm25 - lo_pm) / (hi_pm - lo_pm) * (hi_aqi - lo_aqi)
    return 500.0


def aqi_category(aqi: float) -> dict:
    """Map AQI value to category dict (label, css, health_advisory)."""
    for cat in AQI_CATEGORIES:
        if aqi <= cat["max"]:
            return cat
    return AQI_CATEGORIES[-1]


def detect_trend(history_aqi: list[float]) -> str:
    """↑ ↓ → based on linear slope over last readings."""
    if len(history_aqi) < 3:
        return "→"
    recent = history_aqi[-6:]
    avg_early = sum(recent[:3]) / 3
    avg_late  = sum(recent[-3:]) / 3
    delta     = avg_late - avg_early
    if   delta >  5: return "↑"
    elif delta < -5: return "↓"
    return "→"
