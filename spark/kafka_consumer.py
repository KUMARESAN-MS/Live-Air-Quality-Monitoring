"""
spark/kafka_consumer.py
───────────────────────
Kafka consumer that reads enriched data from 'processed-aqi' topic
(output of Spark Structured Streaming) and feeds the dashboard.

Architecture:
    Spark → [processed-aqi topic] → kafka_consumer.py → city_state → Dashboard

NO deque, NO manual AQI calculation — Spark handles all processing.
This consumer only:
  1. Reads pre-processed records from Kafka
  2. Updates city_state dict for WebSocket push
  3. Persists data to SQLite storage

NOTE: The KafkaConsumer is run inside a REAL OS thread (not an eventlet
greenlet) because eventlet.monkey_patch() breaks kafka-python's internal
selectors/poll calls.  Communication back to eventlet-land is thread-safe
via the shared `city_state` dict protected by `_state_lock`.
"""

import json
import sys
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    KAFKA_BROKER, KAFKA_TOPIC_PROCESSED, KAFKA_PROCESSED_GROUP_ID,
)

# ─── Shared state read by the dashboard ──────────────────────────────────────
city_state = {}
_state_lock = threading.Lock()

# Retry settings
_MAX_RETRIES = 0        # 0 = retry forever
_INITIAL_BACKOFF = 2    # seconds
_MAX_BACKOFF = 30       # seconds


def _process_message(data: dict):
    """Update city_state and persist to storage."""
    city = data.get("city")
    if not city:
        return

    # Update in-memory state for WebSocket push
    with _state_lock:
        city_state[city] = data

    # Persist to SQLite
    try:
        from storage.storage import insert_reading
        insert_reading(data)
    except Exception as e:
        print(f"[Consumer] Storage write error: {e}")


def _consumer_loop():
    """
    Blocking Kafka consumer loop — MUST run in a real OS thread,
    never inside an eventlet greenlet (monkey-patched sockets break it).
    Includes exponential-backoff reconnection.
    """
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    attempt = 0
    backoff = _INITIAL_BACKOFF

    while True:
        attempt += 1
        print(f"[Consumer] Connecting to Kafka at {KAFKA_BROKER} "
              f"(topic: {KAFKA_TOPIC_PROCESSED}) [attempt {attempt}]...")
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_PROCESSED,
                bootstrap_servers=[KAFKA_BROKER],
                group_id=KAFKA_PROCESSED_GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                consumer_timeout_ms=10000,   # yield every 10 s so the loop can react
                reconnect_backoff_ms=1000,
                reconnect_backoff_max_ms=10000,
            )
        except NoBrokersAvailable:
            print(f"[Consumer] Kafka broker not reachable at {KAFKA_BROKER}. "
                  f"Retrying in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)
            continue

        # Connected — reset backoff
        backoff = _INITIAL_BACKOFF
        print("[Consumer] Connected — listening for processed AQI data...")

        try:
            while True:
                # poll() returns in ≤ consumer_timeout_ms even if no messages
                for message in consumer:
                    try:
                        _process_message(message.value)
                    except Exception as e:
                        print(f"[Consumer] Error processing message: {e}")
                # StopIteration from consumer_timeout_ms — just re-enter loop
        except Exception as e:
            print(f"[Consumer] Connection lost ({e}). Reconnecting in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)
        finally:
            try:
                consumer.close()
            except Exception:
                pass


def _load_from_storage():
    """On startup, load latest readings from SQLite for warm start."""
    try:
        from storage.storage import query_latest
        rows = query_latest()
        with _state_lock:
            for row in rows:
                city_state[row["city"]] = row
        if rows:
            print(f"[Consumer] Warm start: loaded {len(rows)} cities from storage")
    except Exception as e:
        print(f"[Consumer] Could not load from storage: {e}")


def start_consumer():
    """
    Start the Kafka consumer in a REAL native OS thread.

    Why not eventlet.spawn()?
      eventlet.monkey_patch() replaces Python's socket/select modules.
      kafka-python uses selectors.DefaultSelector internally, which breaks
      under monkey-patching and causes the consumer to silently hang.
      Running in a real thread (daemon=True) bypasses the patched modules.
    """
    print("[Consumer] Starting consumer for processed-aqi topic...")

    # Warm start from storage
    _load_from_storage()

    # Always use a real OS thread — never an eventlet greenlet
    t = threading.Thread(target=_consumer_loop, name="Kafka-Consumer", daemon=True)
    t.start()
    print("[Consumer] Background OS thread started (bypasses eventlet monkey-patch).")
    return None
