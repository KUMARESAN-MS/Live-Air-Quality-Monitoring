"""
spark/aqi_stream_processor_simple.py
──────────────────────────────────────
Python-only simulation of the Spark Structured Streaming job.
Runs in a background thread — no PySpark, no Docker needed.

Pipeline:
  generate_reading() → AQI calculation → rolling window → trend detection
                     → ML prediction → city_state dict (consumed by dashboard)
"""

import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    CITIES,
    HISTORY_WINDOW,
    PRODUCER_INTERVAL_SEC,
    PM25_BREAKPOINTS,
    AQI_CATEGORIES,
)
from producer.aqi_producer import generate_reading
from ml.predict import predict_next_hour


# ─── Shared state (read by the dashboard) ────────────────────────────────────
city_state: dict[str, dict] = {}
_state_lock = threading.Lock()


# ─── AQI helpers ─────────────────────────────────────────────────────────────
def pm25_to_aqi(pm25: float) -> float:
    for lo_pm, hi_pm, lo_aqi, hi_aqi in PM25_BREAKPOINTS:
        if lo_pm <= pm25 <= hi_pm:
            return lo_aqi + (pm25 - lo_pm) / (hi_pm - lo_pm) * (hi_aqi - lo_aqi)
    return 500.0


def aqi_category(aqi: float) -> dict:
    for cat in AQI_CATEGORIES:
        if aqi <= cat["max"]:
            return cat
    return AQI_CATEGORIES[-1]


def detect_trend(history_aqi: list[float]) -> str:
    """↑ ↓ → based on linear slope over last readings."""
    if len(history_aqi) < 3:
        return "→"
    recent = history_aqi[-6:]          # last 6 readings ≈ 30 seconds
    avg_early = sum(recent[:3]) / 3
    avg_late  = sum(recent[-3:]) / 3
    delta     = avg_late - avg_early
    if   delta >  5: return "↑"
    elif delta < -5: return "↓"
    return "→"


# ─── Per-city rolling history ─────────────────────────────────────────────────
_histories:     dict[str, deque] = defaultdict(lambda: deque(maxlen=HISTORY_WINDOW * 3))
_aqi_histories: dict[str, deque] = defaultdict(lambda: deque(maxlen=30))


def _process_reading(reading: dict):
    city  = reading["city"]
    pm25  = reading["pm25"]
    aqi   = round(pm25_to_aqi(pm25), 1)
    cat   = aqi_category(aqi)

    _histories[city].append(reading)
    _aqi_histories[city].append(aqi)

    trend     = detect_trend(list(_aqi_histories[city]))
    next_hour = predict_next_hour(list(_histories[city])) or round(aqi * 1.05, 1)
    next_cat  = aqi_category(next_hour)

    payload = {
        "city":            city,
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "aqi":             aqi,
        "pm25":            pm25,
        "pm10":            round(reading.get("pm10", 0), 1),
        "no2":             round(reading.get("no2", 0), 1),
        "o3":              round(reading.get("o3", 0), 1),
        "co":              round(reading.get("co", 0), 3),
        "category":        cat["label"],
        "css_class":       cat["css"],
        "trend":           trend,
        "next_hour_aqi":   next_hour,
        "next_hour_label": next_cat["label"],
        "next_hour_css":   next_cat["css"],
        "history_aqi":     list(_aqi_histories[city])[-20:],  # last 20 for chart
    }

    with _state_lock:
        city_state[city] = payload


def _run_loop():
    print(f"[Processor] Simulation mode — processing {len(CITIES)} cities every {PRODUCER_INTERVAL_SEC}s")
    while True:
        for city in CITIES:
            reading = generate_reading(city)
            _process_reading(reading)
        time.sleep(PRODUCER_INTERVAL_SEC)


def start_background():
    """Start the processing loop in a daemon thread."""
    print("[Processor] Pre-filling history with simulation for instant start...")
    for _ in range(HISTORY_WINDOW):
        for city in CITIES:
            # force_sim=True makes this instant and avoids API rate limits
            reading = generate_reading(city, force_sim=True)
            _process_reading(reading)

    t = threading.Thread(target=_run_loop, name="AQI-Processor", daemon=True)
    t.start()
    print("[Processor] Background thread started.")
    return t
