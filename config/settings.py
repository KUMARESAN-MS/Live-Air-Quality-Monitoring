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

# ─── Data Source ──────────────────────────────────────────────────────────────
# "api"        → fetch real data from OpenWeatherMap API
# "simulation" → generate realistic synthetic data (no API key needed)
# In BOTH cases, data always flows through Kafka → Spark → Dashboard.
DATA_SOURCE: str = os.getenv("DATA_SOURCE", "api" if OPENWEATHER_API_KEY else "simulation")

# ─── Cities & Coordinates ─────────────────────────────────────────────────────
# OpenWeather Pollution API requires lat/lon for each city.
# Full catalog of ~50 world cities. Users select up to MAX_ACTIVE_CITIES.
MAX_ACTIVE_CITIES: int = 8

WORLD_CITIES_CATALOG = {
    # Asia
    "Delhi":       {"lat": 28.6139, "lon": 77.2090, "region": "Asia"},
    "Mumbai":      {"lat": 19.0760, "lon": 72.8777, "region": "Asia"},
    "Chennai":     {"lat": 13.0827, "lon": 80.2707, "region": "Asia"},
    "Kolkata":     {"lat": 22.5726, "lon": 88.3639, "region": "Asia"},
    "Bangalore":   {"lat": 12.9716, "lon": 77.5946, "region": "Asia"},
    "Hyderabad":   {"lat": 17.3850, "lon": 78.4867, "region": "Asia"},
    "Beijing":     {"lat": 39.9042, "lon": 116.4074, "region": "Asia"},
    "Shanghai":    {"lat": 31.2304, "lon": 121.4737, "region": "Asia"},
    "Tokyo":       {"lat": 35.6895, "lon": 139.6917, "region": "Asia"},
    "Seoul":       {"lat": 37.5665, "lon": 126.9780, "region": "Asia"},
    "Bangkok":     {"lat": 13.7563, "lon": 100.5018, "region": "Asia"},
    "Jakarta":     {"lat": -6.2088, "lon": 106.8456, "region": "Asia"},
    "Singapore":   {"lat": 1.3521,  "lon": 103.8198, "region": "Asia"},
    "Dhaka":       {"lat": 23.8103, "lon": 90.4125,  "region": "Asia"},
    "Karachi":     {"lat": 24.8607, "lon": 67.0011,  "region": "Asia"},
    "Hanoi":       {"lat": 21.0285, "lon": 105.8542, "region": "Asia"},
    "Dubai":       {"lat": 25.2048, "lon": 55.2708,  "region": "Asia"},
    # Europe
    "London":      {"lat": 51.5074, "lon": -0.1278,  "region": "Europe"},
    "Paris":       {"lat": 48.8566, "lon": 2.3522,   "region": "Europe"},
    "Berlin":      {"lat": 52.5200, "lon": 13.4050,  "region": "Europe"},
    "Madrid":      {"lat": 40.4168, "lon": -3.7038,  "region": "Europe"},
    "Rome":        {"lat": 41.9028, "lon": 12.4964,  "region": "Europe"},
    "Moscow":      {"lat": 55.7558, "lon": 37.6173,  "region": "Europe"},
    "Istanbul":    {"lat": 41.0082, "lon": 28.9784,  "region": "Europe"},
    "Warsaw":      {"lat": 52.2297, "lon": 21.0122,  "region": "Europe"},
    "Amsterdam":   {"lat": 52.3676, "lon": 4.9041,   "region": "Europe"},
    # Americas
    "New York":    {"lat": 40.7128, "lon": -74.0060, "region": "Americas"},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437, "region": "Americas"},
    "Chicago":     {"lat": 41.8781, "lon": -87.6298, "region": "Americas"},
    "Houston":     {"lat": 29.7604, "lon": -95.3698, "region": "Americas"},
    "Mexico City": {"lat": 19.4326, "lon": -99.1332, "region": "Americas"},
    "São Paulo":   {"lat": -23.5505, "lon": -46.6333, "region": "Americas"},
    "Buenos Aires":{"lat": -34.6037, "lon": -58.3816, "region": "Americas"},
    "Lima":        {"lat": -12.0464, "lon": -77.0428, "region": "Americas"},
    "Bogota":      {"lat": 4.7110,  "lon": -74.0721, "region": "Americas"},
    "Toronto":     {"lat": 43.6532, "lon": -79.3832, "region": "Americas"},
    # Africa
    "Cairo":       {"lat": 30.0444, "lon": 31.2357,  "region": "Africa"},
    "Lagos":       {"lat": 6.5244,  "lon": 3.3792,   "region": "Africa"},
    "Nairobi":     {"lat": -1.2921, "lon": 36.8219,  "region": "Africa"},
    "Johannesburg":{"lat": -26.2041,"lon": 28.0473,  "region": "Africa"},
    "Casablanca":  {"lat": 33.5731, "lon": -7.5898,  "region": "Africa"},
    # Oceania
    "Sydney":      {"lat": -33.8688, "lon": 151.2093, "region": "Oceania"},
    "Melbourne":   {"lat": -37.8136, "lon": 144.9631, "region": "Oceania"},
    "Auckland":    {"lat": -36.8485, "lon": 174.7633, "region": "Oceania"},
}

# Default 8 cities for first-time users
DEFAULT_CITIES = ["Delhi", "Mumbai", "Beijing", "London", "New York", "Shanghai", "Tokyo", "Sydney"]

def load_dynamic_cities():
    import json
    from pathlib import Path
    try:
        p = Path("config/active_cities.json")
        if p.exists():
            config = json.loads(p.read_text())
            if isinstance(config, dict):
                return config, list(config.keys())
    except Exception:
        pass
    
    # Fallback to defaults
    default_config = {c: WORLD_CITIES_CATALOG[c] for c in DEFAULT_CITIES if c in WORLD_CITIES_CATALOG}
    return default_config, list(default_config.keys())

# Active cities list (mutable — updated by the /api/cities endpoint)
CITIES_CONFIG, CITIES = load_dynamic_cities()

# ─── Kafka Configuration ──────────────────────────────────────────────────────
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9094")
KAFKA_TOPIC_RAW: str     = "raw-aqi"          # producer → spark
KAFKA_TOPIC_PROCESSED: str = "processed-aqi"  # spark → dashboard (mandatory)
KAFKA_GROUP_ID: str      = "aqi-consumer-group"
KAFKA_PROCESSED_GROUP_ID: str = "aqi-dashboard-group"

# ─── Streaming ────────────────────────────────────────────────────────────────
PRODUCER_INTERVAL_SEC: int       = 60    # Real API poll interval (seconds)
INTERPOLATION_INTERVAL_SEC: int  = 10    # Intermediate readings between API polls
WINDOW_DURATION_SEC: int         = 300   # 5-minute rolling window
WATERMARK_DELAY_SEC: int         = 600   # 10-minute late-data tolerance

# ─── Dashboard ────────────────────────────────────────────────────────────────
DASHBOARD_HOST: str  = "0.0.0.0"
DASHBOARD_PORT: int  = 5001
PUSH_INTERVAL_SEC: int = 3   # how often dashboard receives updates

# ─── ML Model ─────────────────────────────────────────────────────────────────
MODEL_PATH: str          = "ml/models/aqi_model.pkl"
DB_PATH: str             = os.getenv("DB_PATH", "data/aqi_readings.db")
HISTORY_WINDOW: int      = 12   # number of past readings used as features
PREDICTION_HORIZON: int  = 12   # steps ahead to predict (5-sec intervals ≈ 1 hour)

# ─── AQI Breakpoints (EPA Standard) ──────────────────────────────────────────
# PM2.5 µg/m³ → AQI  (lo_pm25, hi_pm25, lo_aqi, hi_aqi)
PM25_BREAKPOINTS = [
    (0.0,   12.0,   0,   50),
    (12.0,  35.4,  51,  100),
    (35.4,  55.4, 101,  150),
    (55.4, 150.4, 151,  200),
    (150.4, 250.4, 201, 300),
    (250.4, 350.4, 301, 400),
    (350.4, 500.4, 401, 500),
]

# ─── AQI Category Labels, CSS Classes & Health Advisories ────────────────────
AQI_CATEGORIES = [
    {"max": 50,  "label": "Good",                    "css": "good",           "health_advisory": "Air quality is satisfactory. Enjoy outdoor activities!"},
    {"max": 100, "label": "Moderate",                 "css": "moderate",       "health_advisory": "Sensitive individuals should consider reducing prolonged outdoor exertion."},
    {"max": 150, "label": "Unhealthy for Sensitive",  "css": "sensitive",      "health_advisory": "Children and people with respiratory diseases should limit outdoor exertion."},
    {"max": 200, "label": "Unhealthy",                "css": "unhealthy",      "health_advisory": "Everyone may experience health effects. Wear a mask outdoors."},
    {"max": 300, "label": "Very Unhealthy",           "css": "very-unhealthy", "health_advisory": "Health warnings of emergency conditions. Avoid outdoor activities."},
    {"max": 999, "label": "Hazardous",                "css": "hazardous",      "health_advisory": "Health alert: stay indoors, keep windows closed, use air purifiers."},
]
