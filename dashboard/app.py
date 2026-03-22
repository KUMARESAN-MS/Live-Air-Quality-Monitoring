"""
dashboard/app.py
────────────────
Flask + Flask-SocketIO server.
  GET  /           → serves the live dashboard page
  GET  /api/current → JSON snapshot of all cities
  WebSocket        → pushes 'city_update' every PUSH_INTERVAL_SEC seconds
"""

import eventlet
eventlet.monkey_patch()

import sys
import time
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO

from config.settings import (
    DASHBOARD_HOST, DASHBOARD_PORT, PUSH_INTERVAL_SEC, SIMULATION_MODE,
)

app     = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# ─── State import: simulation vs full Spark pipeline ──────────────────────────
if SIMULATION_MODE:
    from spark.aqi_stream_processor_simple import city_state, start_background
    _processor_thread = None
else:
    # In full mode, city_state is populated by Kafka consumer reading raw-aqi
    from spark.kafka_consumer import city_state, start_consumer  # type: ignore
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
        from flask import request
        import json
        is_demo = request.json.get("demo", False)
        mode_file = Path("config/mode.json")
        mode_file.write_text(json.dumps({"demo_mode": is_demo}))
        return jsonify({"status": "ok", "demo_mode": is_demo})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── WebSocket push loop ──────────────────────────────────────────────────────
def _push_loop():
    """Background thread: push live city data to all connected clients."""
    time.sleep(3)   # give processor time to generate first readings
    while True:
        cities = list(city_state.values())
        if cities:
            socketio.emit("city_update", cities)
        time.sleep(PUSH_INTERVAL_SEC)


# ─── Startup ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"[Dashboard] Starting in {'SIMULATION' if SIMULATION_MODE else 'KAFKA'} mode...")

    start_background()

    push_thread = threading.Thread(target=_push_loop, name="Pusher", daemon=True)
    push_thread.start()

    print(f"[Dashboard] Open: http://localhost:{DASHBOARD_PORT}")
    socketio.run(app, host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=False)
