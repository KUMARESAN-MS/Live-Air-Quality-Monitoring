"""
producer/aqi_producer.py
────────────────────────
Kafka Producer — publishes AQI sensor readings to 'raw-aqi' topic.

ALL data flows through Kafka regardless of data source:
  DATA_SOURCE = "api"        → real OpenWeather API (every 60s) + interpolated readings
  DATA_SOURCE = "simulation" → synthetic data (no API key needed)

Architecture:  Producer → Kafka (raw-aqi) → Spark → Kafka (processed-aqi) → Dashboard
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
    DATA_SOURCE,
    KAFKA_BROKER,
    KAFKA_TOPIC_RAW,
    OPENWEATHER_API_KEY,
    PRODUCER_INTERVAL_SEC,
    INTERPOLATION_INTERVAL_SEC,
)

# ─── Realistic baseline ranges per city (µg/m³ / ppb) ────────────────────────
CITY_PROFILES = {
    "Delhi":    {"pm25": (80,  160), "pm10": (150, 280), "no2": (60,  120), "o3": (20, 50),  "co": (1.5, 3.0), "so2": (15, 40)},
    "Mumbai":   {"pm25": (40,  90),  "pm10": (80,  160), "no2": (40,  90),  "o3": (25, 55),  "co": (0.8, 2.0), "so2": (10, 25)},
    "Beijing":  {"pm25": (90,  200), "pm10": (180, 320), "no2": (70,  140), "o3": (15, 45),  "co": (2.0, 4.0), "so2": (20, 50)},
    "London":   {"pm25": (8,   25),  "pm10": (15,  45),  "no2": (25,  60),  "o3": (30, 70),  "co": (0.2, 0.6), "so2": (2,  8)},
    "New York": {"pm25": (10,  30),  "pm10": (20,  55),  "no2": (30,  70),  "o3": (35, 75),  "co": (0.3, 0.8), "so2": (3,  10)},
    "Shanghai": {"pm25": (50,  120), "pm10": (100, 200), "no2": (55,  110), "o3": (20, 50),  "co": (1.2, 2.5), "so2": (12, 30)},
    "Tokyo":    {"pm25": (12,  35),  "pm10": (25,  60),  "no2": (30,  65),  "o3": (40, 80),  "co": (0.3, 0.7), "so2": (4,  12)},
    "Sydney":   {"pm25": (5,   18),  "pm10": (10,  35),  "no2": (15,  40),  "o3": (25, 65),  "co": (0.1, 0.4), "so2": (1,  5)},
    "Cairo":    {"pm25": (40,  95),  "pm10": (90,  180), "no2": (45,  100), "o3": (30, 70),  "co": (1.0, 2.2), "so2": (12, 30)},
    "Casablanca":{"pm25": (20,  55),  "pm10": (40,  90),  "no2": (30,  70),  "o3": (35, 75),  "co": (0.4, 1.2), "so2": (5,  15)},
    "Johannesburg":{"pm25": (25,  65), "pm10": (50,  110), "no2": (35,  80),  "o3": (30, 70),  "co": (0.6, 1.5), "so2": (8,  20)},
    "Lagos":    {"pm25": (45,  110), "pm10": (100, 220), "no2": (50,  110), "o3": (25, 65),  "co": (1.2, 2.5), "so2": (15, 35)},
    "Nairobi":  {"pm25": (15,  40),  "pm10": (30,  75),  "no2": (20,  55),  "o3": (35, 80),  "co": (0.3, 0.9), "so2": (4,  10)},
    "Bogota":   {"pm25": (10,  35),  "pm10": (20,  60),  "no2": (25,  60),  "o3": (40, 85),  "co": (0.4, 1.0), "so2": (3,  12)},
    "Toronto":  {"pm25": (8,   22),  "pm10": (15,  45),  "no2": (20,  50),  "o3": (35, 75),  "co": (0.2, 0.6), "so2": (2,  8)},
}

# Ensure every city in CITIES has a profile (default to moderate values)
_DEFAULT_PROFILE = {"pm25": (30, 80), "pm10": (60, 140), "no2": (30, 80),
                    "o3": (25, 60), "co": (0.5, 1.5), "so2": (5, 20)}

# Track last real reading per city for interpolation
_last_real_reading = {}


# ─── Time-of-day pollution multiplier (rush hours = more pollution) ───────────
def _time_multiplier() -> float:
    hour = datetime.now().hour
    if 7 <= hour <= 10 or 17 <= hour <= 20:
        return random.uniform(1.15, 1.40)
    if 2 <= hour <= 5:
        return random.uniform(0.60, 0.80)
    return random.uniform(0.85, 1.10)


def _simulate_traffic(hour: int) -> str:
    """Simulate traffic volume based on time-of-day. Does NOT affect AQI values."""
    if 7 <= hour <= 10 or 17 <= hour <= 20:
        return "High"
    if 2 <= hour <= 5:
        return "Low"
    return "Medium"


def fetch_real_aqi(city: str) -> dict:
    """Fetch live air pollution data from OpenWeatherMap."""
    if not OPENWEATHER_API_KEY:
        return None

    import config.settings as settings
    config = settings.CITIES_CONFIG.get(city)
    if not config:
        return None

    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={config['lat']}&lon={config['lon']}&appid={OPENWEATHER_API_KEY}"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()

        comp = data["list"][0]["components"]

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
            "traffic":   _simulate_traffic(datetime.now().hour),
            "source":    "OpenWeatherMap",
            "is_real":   True,
        }
    except Exception as e:
        print(f"[Producer] Error fetching {city}: {e}")
        return None


def generate_reading(city: str, force_sim: bool = False) -> dict:
    """Get air quality reading: tries real API first, then falls back to simulation."""
    demo_mode = False
    try:
        import json
        from pathlib import Path
        mode_file = Path("config/mode.json")
        if mode_file.exists():
            demo_mode = json.loads(mode_file.read_text()).get("demo_mode", False)
    except Exception:
        pass

    # Try real data if API source and key exists
    if DATA_SOURCE == "api" and OPENWEATHER_API_KEY and not demo_mode and not force_sim:
        real_data = fetch_real_aqi(city)
        if real_data:
            _last_real_reading[city] = real_data
            return real_data

    # Fallback to simulation
    profile = CITY_PROFILES.get(city, _DEFAULT_PROFILE)
    mult    = _time_multiplier()

    def sample(key: str) -> float:
        lo, hi = profile[key]
        base   = random.uniform(lo, hi)
        phase  = math.sin(time.time() / 3600 * math.pi)
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
        "traffic":   _simulate_traffic(datetime.now().hour),
        "source":    "Simulation",
        "is_real":   False,
    }


def interpolate_reading(city: str) -> dict:
    """
    Generate an intermediate reading between real API polls.
    Adds slight noise to the last known reading for realistic variation.
    """
    last = _last_real_reading.get(city)
    if last is None:
        # No previous real reading — fall back to full simulation
        return generate_reading(city, force_sim=True)

    def noisy(val, noise_pct=0.05):
        """Add slight Gaussian noise to avoid unrealistic smooth data."""
        if val == 0:
            return 0.0
        noise = random.gauss(0, abs(val) * noise_pct)
        return max(0.0, round(val + noise, 2))

    return {
        "city":      city,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pm25":      noisy(last["pm25"], 0.06),
        "pm10":      noisy(last["pm10"], 0.05),
        "no2":       noisy(last["no2"],  0.04),
        "o3":        noisy(last["o3"],   0.04),
        "co":        round(noisy(last["co"], 0.03), 3),
        "so2":       noisy(last["so2"],  0.04),
        "traffic":   _simulate_traffic(datetime.now().hour),
        "source":    "Interpolated",
        "is_real":   False,
    }


def run_kafka():
    """
    Kafka producer — the ONLY entry point.
    ALL data (real API, simulation, or interpolated) flows through Kafka.

    For DATA_SOURCE="api":
      - Every PRODUCER_INTERVAL_SEC (60s): fetch real API data
      - Every INTERPOLATION_INTERVAL_SEC (10s) in between: publish interpolated readings
    For DATA_SOURCE="simulation":
      - Every INTERPOLATION_INTERVAL_SEC (10s): publish simulated readings
    """
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    print(f"[Producer] Connecting to Kafka at {KAFKA_BROKER} ...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            compression_type="gzip",
        )
    except NoBrokersAvailable:
        print("[Producer] ERROR: Kafka broker not reachable at " + KAFKA_BROKER)
        print("[Producer] Start Kafka with: docker-compose up -d")
        sys.exit(1)

    print(f"[Producer] DATA_SOURCE={DATA_SOURCE} | Publishing to '{KAFKA_TOPIC_RAW}'")
    print(f"[Producer] API interval: {PRODUCER_INTERVAL_SEC}s | Interpolation: {INTERPOLATION_INTERVAL_SEC}s")

    try:
        import config.settings as settings
        tick = 0  # counts interpolation ticks within each API cycle

        while True:
            current_config, current_cities = settings.load_dynamic_cities()
            settings.CITIES_CONFIG = current_config
            settings.CITIES = current_cities

            is_api_tick = (DATA_SOURCE == "api" and tick == 0)

            for city in current_cities:
                if DATA_SOURCE == "simulation":
                    reading = generate_reading(city, force_sim=True)
                elif is_api_tick:
                    reading = generate_reading(city)
                else:
                    reading = interpolate_reading(city)

                future = producer.send(KAFKA_TOPIC_RAW, value=reading)
                meta   = future.get(timeout=10)
                src    = reading.get("source", "?")
                print(f"[Producer] {reading['city']:12s} ({src:14s}) → partition={meta.partition} offset={meta.offset}")

            producer.flush()

            # Advance tick for API mode interpolation cycle
            if DATA_SOURCE == "api":
                ticks_per_cycle = PRODUCER_INTERVAL_SEC // INTERPOLATION_INTERVAL_SEC
                tick = (tick + 1) % ticks_per_cycle
                time.sleep(INTERPOLATION_INTERVAL_SEC)
            else:
                time.sleep(INTERPOLATION_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n[Producer] Stopped.")
    finally:
        producer.close()


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_kafka()
