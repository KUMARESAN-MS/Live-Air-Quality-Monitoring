"""
config/settings.py
──────────────────
Central configuration for the Real-Time AQI Monitoring System.
Change values here to affect the whole pipeline.
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─── OpenWeather API ──────────────────────────────────────────────────────────
# Get API Key from .env
OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")

# ─── Simulation Mode ──────────────────────────────────────────────────────────
# Set True  → pure Python demo, no Kafka/Spark/Docker needed
# Set False → full Kafka + Spark pipeline (requires docker-compose up)
SIMULATION_MODE: bool = True

# ─── Cities & Coordinates ─────────────────────────────────────────────────────
# OpenWeather Pollution API requires lat/lon for each city.
CITIES_CONFIG: dict[str, dict] = {
    "Delhi":    {"lat": 28.6139, "lon": 77.2090},
    "Mumbai":   {"lat": 19.0760, "lon": 72.8777},
    "Beijing":  {"lat": 39.9042, "lon": 116.4074},
    "London":   {"lat": 51.5074, "lon": -0.1278},
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Shanghai": {"lat": 31.2304, "lon": 121.4737},
    "Tokyo":    {"lat": 35.6895, "lon": 139.6917},
    "Sydney":   {"lat": -33.8688, "lon": 151.2093},
}

CITIES: list[str] = list(CITIES_CONFIG.keys())

# ─── Kafka Configuration ──────────────────────────────────────────────────────
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9094")
KAFKA_TOPIC_RAW: str     = "raw-aqi"          # producer → spark
KAFKA_TOPIC_PROCESSED: str = "processed-aqi"  # spark → dashboard (optional)
KAFKA_GROUP_ID: str      = "aqi-consumer-group"

# ─── Streaming ────────────────────────────────────────────────────────────────
PRODUCER_INTERVAL_SEC: int   = 5    # seconds between sensor readings
WINDOW_DURATION_SEC: int     = 300  # 5-minute rolling window
WATERMARK_DELAY_SEC: int     = 600  # 10-minute late-data tolerance

# ─── Dashboard ────────────────────────────────────────────────────────────────
DASHBOARD_HOST: str  = "0.0.0.0"
DASHBOARD_PORT: int  = 5001
PUSH_INTERVAL_SEC: int = 3   # how often dashboard receives updates

# ─── ML Model ─────────────────────────────────────────────────────────────────
MODEL_PATH: str          = "ml/models/aqi_model.pkl"
HISTORY_WINDOW: int      = 12   # number of past readings used as features
PREDICTION_HORIZON: int  = 12   # steps ahead to predict (5-sec intervals ≈ 1 hour)

# ─── AQI Breakpoints (EPA Standard) ──────────────────────────────────────────
# PM2.5 µg/m³ → AQI  (lo_pm25, hi_pm25, lo_aqi, hi_aqi)
PM25_BREAKPOINTS: list[tuple] = [
    (0.0,   12.0,   0,   50),
    (12.1,  35.4,  51,  100),
    (35.5,  55.4, 101,  150),
    (55.5, 150.4, 151,  200),
    (150.5, 250.4, 201, 300),
    (250.5, 350.4, 301, 400),
    (350.5, 500.4, 401, 500),
]

# ─── AQI Category Labels & CSS Classes ───────────────────────────────────────
AQI_CATEGORIES: list[dict] = [
    {"max": 50,  "label": "Good",                         "css": "good"},
    {"max": 100, "label": "Moderate",                     "css": "moderate"},
    {"max": 150, "label": "Unhealthy for Sensitive",      "css": "sensitive"},
    {"max": 200, "label": "Unhealthy",                    "css": "unhealthy"},
    {"max": 300, "label": "Very Unhealthy",               "css": "very-unhealthy"},
    {"max": 999, "label": "Hazardous",                    "css": "hazardous"},
]
