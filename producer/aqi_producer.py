"""
producer/aqi_producer.py
────────────────────────
Kafka Producer — publishes live AQI sensor readings to 'raw-aqi' topic.

Two modes:
  SIMULATION_MODE = True  → generates realistic synthetic data (no Kafka needed)
  SIMULATION_MODE = False → publishes real JSON to Kafka broker
"""

import json
import math
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from config.settings import (
    CITIES,
    CITIES_CONFIG,
    KAFKA_BROKER,
    KAFKA_TOPIC_RAW,
    OPENWEATHER_API_KEY,
    PRODUCER_INTERVAL_SEC,
    SIMULATION_MODE,
)

# ─── Realistic baseline ranges per city (µg/m³ / ppb) ────────────────────────
CITY_PROFILES: dict[str, dict] = {
    "Delhi":    {"pm25": (80,  160), "pm10": (150, 280), "no2": (60,  120), "o3": (20, 50),  "co": (1.5, 3.0), "so2": (15, 40)},
    "Mumbai":   {"pm25": (40,  90),  "pm10": (80,  160), "no2": (40,  90),  "o3": (25, 55),  "co": (0.8, 2.0), "so2": (10, 25)},
    "Beijing":  {"pm25": (90,  200), "pm10": (180, 320), "no2": (70,  140), "o3": (15, 45),  "co": (2.0, 4.0), "so2": (20, 50)},
    "London":   {"pm25": (8,   25),  "pm10": (15,  45),  "no2": (25,  60),  "o3": (30, 70),  "co": (0.2, 0.6), "so2": (2,  8)},
    "New York": {"pm25": (10,  30),  "pm10": (20,  55),  "no2": (30,  70),  "o3": (35, 75),  "co": (0.3, 0.8), "so2": (3,  10)},
    "Shanghai": {"pm25": (50,  120), "pm10": (100, 200), "no2": (55,  110), "o3": (20, 50),  "co": (1.2, 2.5), "so2": (12, 30)},
    "Tokyo":    {"pm25": (12,  35),  "pm10": (25,  60),  "no2": (30,  65),  "o3": (40, 80),  "co": (0.3, 0.7), "so2": (4,  12)},
    "Sydney":   {"pm25": (5,   18),  "pm10": (10,  35),  "no2": (15,  40),  "o3": (25, 65),  "co": (0.1, 0.4), "so2": (1,  5)},
}

# Ensure every city in CITIES has a profile (default to moderate values)
_DEFAULT_PROFILE = {"pm25": (30, 80), "pm10": (60, 140), "no2": (30, 80),
                    "o3": (25, 60), "co": (0.5, 1.5), "so2": (5, 20)}

# ─── Time-of-day pollution multiplier (rush hours = more pollution) ───────────
def _time_multiplier() -> float:
    hour = datetime.now().hour
    # Morning rush: 7-10, Evening rush: 17-20 → up to 40% more pollution
    if 7 <= hour <= 10 or 17 <= hour <= 20:
        return random.uniform(1.15, 1.40)
    if 2 <= hour <= 5:   # late night — cleanest air
        return random.uniform(0.60, 0.80)
    return random.uniform(0.85, 1.10)


def fetch_real_aqi(city: str) -> dict:
    """Fetch live air pollution data from OpenWeatherMap."""
    if not OPENWEATHER_API_KEY:
        return None

    config = CITIES_CONFIG.get(city)
    if not config:
        return None

    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={config['lat']}&lon={config['lon']}&appid={OPENWEATHER_API_KEY}"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        
        # OpenWeather schema: list[0].components mapping
        # { co: ppb, no: ppb, no2: ppb, o3: ppb, so2: ppb, pm2_5: µg/m3, pm10: µg/m3, nh3: ppb }
        comp = data["list"][0]["components"]

        # ── Natural jitter (±3-8%) to reflect real sensor fluctuation ────
        def jitter(val, pct=0.05):
            return round(val * random.uniform(1 - pct, 1 + pct), 2) if val else 0.0

        return {
            "city":      city,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pm25":      jitter(comp.get("pm2_5", 0.0), 0.08),
            "pm10":      jitter(comp.get("pm10", 0.0), 0.06),
            "no2":       jitter(comp.get("no2", 0.0), 0.05),
            "o3":        jitter(comp.get("o3", 0.0), 0.05),
            "co":        round(jitter(comp.get("co", 0.0), 0.04) / 1000, 3) if "co" in comp else 0.0,
            "so2":       jitter(comp.get("so2", 0.0), 0.05),
            "source":    "OpenWeatherMap"
        }
    except Exception as e:
        print(f"[Producer] Error fetching {city}: {e}")
        return None


def generate_reading(city: str, force_sim: bool = False) -> dict:
    """Get air quality reading: tries real API first, then falls back to simulation."""
    # Check if we are in forced demo mode (high fluctuation)
    demo_mode = False
    try:
        import json
        from pathlib import Path
        mode_file = Path("config/mode.json")
        if mode_file.exists():
            demo_mode = json.loads(mode_file.read_text()).get("demo_mode", False)
    except Exception:
        pass

    # Try real data if key exists and not in demo mode
    if OPENWEATHER_API_KEY and not demo_mode and not force_sim:
        real_data = fetch_real_aqi(city)
        if real_data:
            return real_data

    # Fallback to simulation
    profile = CITY_PROFILES.get(city, _DEFAULT_PROFILE)
    mult    = _time_multiplier()

    def sample(key: str) -> float:
        lo, hi = profile[key]
        base   = random.uniform(lo, hi)
        # Add a smooth sine wave for gradual daily variation
        phase  = math.sin(time.time() / 3600 * math.pi)
        # Increase noise for more visible live movement in demo
        noise  = random.uniform(-(hi - lo) * 0.15, (hi - lo) * 0.15)
        return max(0.0, round(base * mult + phase * (hi - lo) * 0.1 + noise, 2))

    return {
        "city":      city,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pm25":      sample("pm25"),
        "pm10":      sample("pm10"),
        "no2":       sample("no2"),
        "o3":        sample("o3"),
        "co":        round(sample("co"), 3),
        "so2":       sample("so2"),
    }


def run_simulation(callback=None):
    """
    Pure-Python simulation loop (no Kafka).
    Calls callback(reading_dict) each tick so other modules can consume data.
    """
    print(f"[Producer] SIMULATION MODE — generating data for {len(CITIES)} cities")
    print(f"[Producer] Interval: {PRODUCER_INTERVAL_SEC}s | Cities: {', '.join(CITIES)}")
    while True:
        for city in CITIES:
            reading = generate_reading(city)
            if callback:
                callback(reading)
            else:
                print(json.dumps(reading, indent=2))
        time.sleep(PRODUCER_INTERVAL_SEC)


def run_kafka():
    """Real Kafka producer — requires broker running at KAFKA_BROKER."""
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    print(f"[Producer] Connecting to Kafka at {KAFKA_BROKER} ...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",           # wait for full replication acknowledgement
            retries=5,
            compression_type="gzip",
        )
    except NoBrokersAvailable:
        print("[Producer] ERROR: Kafka broker not reachable. Set SIMULATION_MODE=True in config/settings.py")
        sys.exit(1)

    print(f"[Producer] Publishing to topic '{KAFKA_TOPIC_RAW}' every {PRODUCER_INTERVAL_SEC}s ...")

    try:
        while True:
            for city in CITIES:
                reading = generate_reading(city)
                future  = producer.send(KAFKA_TOPIC_RAW, value=reading)
                meta    = future.get(timeout=10)
                print(f"[Producer] {reading['city']:12s} → partition={meta.partition} offset={meta.offset}")
            producer.flush()
            time.sleep(PRODUCER_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\n[Producer] Stopped.")
    finally:
        producer.close()


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if SIMULATION_MODE:
        run_simulation()
    else:
        run_kafka()
