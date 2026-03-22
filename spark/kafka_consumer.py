"""
spark/kafka_consumer.py
───────────────────────
Pure-Python Kafka consumer that reads from 'raw-aqi', computes AQI,
detects trend, runs ML prediction and writes to city_state (shared
with the dashboard — identical contract to aqi_stream_processor_simple).

This replaces PySpark on Windows where winutils.exe is unavailable.

Architecture (KAFKA mode):
    aqi_producer.py → [raw-aqi topic] → kafka_consumer.py → city_state → dashboard
"""

import json
import sys
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    KAFKA_BROKER, KAFKA_TOPIC_RAW, KAFKA_GROUP_ID,
    HISTORY_WINDOW, PRODUCER_INTERVAL_SEC,
    PM25_BREAKPOINTS, AQI_CATEGORIES,
)

# ─── Shared state read by the dashboard ──────────────────────────────────────
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


def detect_trend(history_aqi: list) -> str:
    if len(history_aqi) < 3:
        return "→"
    recent = history_aqi[-6:]
    avg_early = sum(recent[:3]) / 3
    avg_late  = sum(recent[-3:]) / 3
    delta     = avg_late - avg_early
    if   delta >  5: return "↑"
    elif delta < -5: return "↓"
    return "→"

def _trend_fallback(city: str, current_aqi: float) -> float:
    """Predict next hour based on recent simple trend if ML history is short."""
    hist = _aqi_histories[city]
    if len(hist) < 2:
        return current_aqi
    # Calculate average change over last 5 readings
    recent = list(hist)[-5:]
    avg_change = (recent[-1] - recent[0]) / max(len(recent) - 1, 1)
    # Extrapolate roughly 12 steps (1 hour) ahead, capped at realistic bounds
    pred = current_aqi + (avg_change * 12)
    return max(0.0, min(500.0, round(pred, 1)))

# ─── Per-city rolling history ─────────────────────────────────────────────────
_histories:     dict = defaultdict(lambda: deque(maxlen=HISTORY_WINDOW * 3))
_aqi_histories: dict = defaultdict(lambda: deque(maxlen=40))


def _process_reading(reading: dict):
    city = reading.get("city")
    pm25 = reading.get("pm25", 0.0)
    if not city:
        return

    aqi = round(pm25_to_aqi(pm25), 1)
    cat = aqi_category(aqi)

    _histories[city].append(reading)
    _aqi_histories[city].append(aqi)

    trend = detect_trend(list(_aqi_histories[city]))

    # ML prediction (lazy import — model auto-trains if missing)
    try:
        from ml.predict import predict_next_hour
        next_hour = predict_next_hour(list(_histories[city]))
        if next_hour is None:
            next_hour = _trend_fallback(city, aqi)
    except Exception:
        next_hour = _trend_fallback(city, aqi)

    next_cat = aqi_category(next_hour)

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
        "history_aqi":     list(_aqi_histories[city])[-40:],
        "source":          "Kafka",
    }

    with _state_lock:
        city_state[city] = payload


def _consumer_loop():
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    print(f"[Consumer] Connecting to Kafka at {KAFKA_BROKER} (topic: {KAFKA_TOPIC_RAW})...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_RAW,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
        )
    except NoBrokersAvailable:
        print(f"[Consumer] ERROR: Kafka broker not reachable at {KAFKA_BROKER}.")
        return

    print(f"[Consumer] Connected — listening for AQI readings...")
    for message in consumer:
        try:
            _process_reading(message.value)
        except Exception as e:
            print(f"[Consumer] Error processing message: {e}")


def start_consumer():
    """Start the Kafka consumer as an eventlet greenlet (compatible with monkey-patch)."""
    print("[Consumer] Pre-filling history with simulation for instant start...")
    try:
        from producer.aqi_producer import generate_reading
        from config.settings import CITIES, HISTORY_WINDOW
        for _ in range(HISTORY_WINDOW):
            for city in CITIES:
                # force_sim=True makes this instant and avoids API rate limits
                reading = generate_reading(city, force_sim=True)
                _process_reading(reading)
    except Exception as e:
        print(f"[Consumer] Pre-fill failed: {e}")

    try:
        import eventlet
        eventlet.spawn(_consumer_loop)
        print("[Consumer] Background greenlet started (eventlet).")
    except ImportError:
        t = threading.Thread(target=_consumer_loop, name="Kafka-Consumer", daemon=True)
        t.start()
        print("[Consumer] Background thread started.")
    return None


