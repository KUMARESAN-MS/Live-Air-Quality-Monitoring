"""
dashboard/app.py
────────────────
Flask + Flask-SocketIO server.
  GET  /           → serves the live dashboard page
  GET  /api/current → JSON snapshot of all cities
  GET  /api/history → historical data from SQLite storage
  WebSocket        → pushes 'city_update' every PUSH_INTERVAL_SEC seconds

ALL data arrives via Kafka consumer reading from 'processed-aqi' topic.
No simulation mode — Kafka + Spark are mandatory.
"""

import eventlet
eventlet.monkey_patch()

import sys
import time
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO

from config.settings import (
    DASHBOARD_HOST, DASHBOARD_PORT, PUSH_INTERVAL_SEC,
    WORLD_CITIES_CATALOG, MAX_ACTIVE_CITIES, DEFAULT_CITIES,
)
import config.settings as settings

app     = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# ─── State import: always from Kafka consumer (reads processed-aqi topic) ────
from spark.kafka_consumer import city_state, start_consumer
start_background = start_consumer

# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/current")
def api_current():
    """REST snapshot — useful for initial page load."""
    return jsonify(list(city_state.values()))

@app.route("/api/mode", methods=["POST"])
def api_mode():
    """Toggle between Live Data and High-Fluctuation Demo Mode."""
    try:
        import json
        is_demo = request.json.get("demo", False)
        mode_file = Path("config/mode.json")
        mode_file.write_text(json.dumps({"demo_mode": is_demo}))
        return jsonify({"status": "ok", "demo_mode": is_demo})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/catalog")
def api_catalog():
    """Return full world cities catalog for the city picker."""
    catalog = []
    active = set(settings.CITIES)
    for name, info in WORLD_CITIES_CATALOG.items():
        catalog.append({
            "name": name,
            "region": info.get("region", ""),
            "active": name in active,
        })
    catalog.sort(key=lambda c: (c["region"], c["name"]))
    return jsonify({"cities": catalog, "max": MAX_ACTIVE_CITIES})


@app.route("/api/cities", methods=["POST"])
def api_cities():
    """Update active cities list. Accepts JSON: {"cities": ["Delhi", "London", ...]}"""
    selected = request.json.get("cities", [])
    valid = [c for c in selected if c in WORLD_CITIES_CATALOG]
    if len(valid) > MAX_ACTIVE_CITIES:
        return jsonify({"error": f"Maximum {MAX_ACTIVE_CITIES} cities allowed"}), 400
    if len(valid) == 0:
        return jsonify({"error": "Select at least 1 city"}), 400

    settings.CITIES_CONFIG = {c: WORLD_CITIES_CATALOG[c] for c in valid}
    settings.CITIES = valid

    try:
        import json
        from pathlib import Path
        Path("config/active_cities.json").write_text(json.dumps(settings.CITIES_CONFIG))
    except Exception as e:
        print(f"Failed to write active cities config: {e}")

    stale = [k for k in city_state if k not in valid]
    for k in stale:
        del city_state[k]

    return jsonify({"status": "ok", "active": valid})


@app.route("/api/custom_city", methods=["POST"])
def api_custom_city():
    """Reverse geocode a custom lat/lon and add it to the catalog."""
    lat = request.json.get("lat")
    lon = request.json.get("lon")
    if lat is None or lon is None:
        return jsonify({"error": "Missing lat or lon"}), 400

    api_key = settings.OPENWEATHER_API_KEY
    if not api_key:
        return jsonify({"error": "OPENWEATHER_API_KEY is not configured"}), 500

    try:
        import requests
        url = f"http://api.openweathermap.org/geo/1.0/reverse?lat={lat}&lon={lon}&limit=1&appid={api_key}"
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            return jsonify({"error": "No location found for these coordinates"}), 404

        loc = data[0]
        name = loc.get("name", f"Unknown ({lat:.2f},{lon:.2f})")
        suffix = loc.get("state") or loc.get("country") or ""
        display_name = f"{name}, {suffix}" if suffix else name

        WORLD_CITIES_CATALOG[display_name] = {
            "lat": float(lat),
            "lon": float(lon),
            "region": "Custom"
        }

        return jsonify({"status": "ok", "name": display_name, "region": "Custom"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/history")
def api_history():
    """Query historical AQI data from persistent storage."""
    city = request.args.get("city")
    hours = int(request.args.get("hours", 24))
    if not city:
        return jsonify({"error": "Missing 'city' parameter"}), 400
    try:
        from storage.storage import query_history
        data = query_history(city, hours)
        return jsonify({"city": city, "hours": hours, "readings": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/storage_stats")
def api_storage_stats():
    """Get storage statistics."""
    try:
        from storage.storage import get_reading_count
        return jsonify({"total_readings": get_reading_count()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── WebSocket push loop (runs as eventlet greenlet, NOT a thread) ────────────
def _push_loop():
    """
    Background greenlet: push live city data to all connected clients.
    MUST run via socketio.start_background_task() so that emits happen
    inside the eventlet context.  Using threading.Thread + time.sleep()
    causes emits to be silently dropped.
    """
    socketio.sleep(3)  # initial delay — use socketio.sleep, NOT time.sleep
    push_count = 0
    while True:
        cities = list(city_state.values())
        if cities:
            socketio.emit("city_update", cities)
            push_count += 1
            if push_count % 20 == 1:  # log every ~60s (20 × 3s)
                print(f"[Pusher] Emitted city_update to clients ({len(cities)} cities, push #{push_count})")
        socketio.sleep(PUSH_INTERVAL_SEC)  # yield to eventlet event loop


# ─── Startup ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("[Dashboard] Starting in KAFKA mode (Kafka + Spark required)...")

    # Initialize storage
    try:
        from storage.storage import init_db
        init_db()
    except Exception as e:
        print(f"[Dashboard] Storage init warning: {e}")

    start_background()

    # CRITICAL: use socketio.start_background_task — NOT threading.Thread.
    # This runs _push_loop as an eventlet greenlet so socketio.emit()
    # actually reaches connected clients.
    socketio.start_background_task(_push_loop)

    print(f"[Dashboard] Open: http://localhost:{DASHBOARD_PORT}")
    socketio.run(app, host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=False)
