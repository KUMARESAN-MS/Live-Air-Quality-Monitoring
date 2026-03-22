"""
ml/aqi_predictor.py
────────────────────
Trains a GradientBoostingRegressor on synthetic historical data and saves
the model to ml/models/aqi_model.pkl.

Run once before starting the dashboard:
    python ml/aqi_predictor.py
"""

import os
import sys
import random
import math
import numpy as np
import joblib
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

from ml.feature_engineering import extract_features, feature_dim
from config.settings import MODEL_PATH, HISTORY_WINDOW, PM25_BREAKPOINTS

# city profiles for data generation (mirrors producer)
CITY_PROFILES: dict = {
    "Delhi":    {"pm25": (80, 160), "pm10": (150, 280), "no2": (60, 120), "o3": (20, 50),  "co": (1.5, 3.0), "so2": (15, 40)},
    "Mumbai":   {"pm25": (40, 90),  "pm10": (80, 160),  "no2": (40, 90),  "o3": (25, 55),  "co": (0.8, 2.0), "so2": (10, 25)},
    "Beijing":  {"pm25": (90, 200), "pm10": (180, 320), "no2": (70, 140), "o3": (15, 45),  "co": (2.0, 4.0), "so2": (20, 50)},
    "London":   {"pm25": (8, 25),   "pm10": (15, 45),   "no2": (25, 60),  "o3": (30, 70),  "co": (0.2, 0.6), "so2": (2, 8)},
    "New York": {"pm25": (10, 30),  "pm10": (20, 55),   "no2": (30, 70),  "o3": (35, 75),  "co": (0.3, 0.8), "so2": (3, 10)},
    "Shanghai": {"pm25": (50, 120), "pm10": (100, 200), "no2": (55, 110), "o3": (20, 50),  "co": (1.2, 2.5), "so2": (12, 30)},
    "Tokyo":    {"pm25": (12, 35),  "pm10": (25, 60),   "no2": (30, 65),  "o3": (40, 80),  "co": (0.3, 0.7), "so2": (4, 12)},
    "Sydney":   {"pm25": (5, 18),   "pm10": (10, 35),   "no2": (15, 40),  "o3": (25, 65),  "co": (0.1, 0.4), "so2": (1, 5)},
}


def _pm25_to_aqi(pm25: float) -> float:
    """EPA linear interpolation for PM2.5 → AQI."""
    from config.settings import PM25_BREAKPOINTS
    for lo_pm, hi_pm, lo_aqi, hi_aqi in PM25_BREAKPOINTS:
        if lo_pm <= pm25 <= hi_pm:
            return lo_aqi + (pm25 - lo_pm) / (hi_pm - lo_pm) * (hi_aqi - lo_aqi)
    return 500.0


def _synthetic_reading(city: str, hour: int, t: int) -> dict:
    profile = CITY_PROFILES[city]
    mult    = 1.3 if (7 <= hour <= 10 or 17 <= hour <= 20) else (0.7 if 2 <= hour <= 5 else 1.0)
    phase   = math.sin(t / 720 * math.pi)

    def sample(key):
        lo, hi = profile[key]
        return max(0, random.gauss((lo + hi) / 2 * mult + phase * (hi - lo) * 0.1, (hi - lo) * 0.05))

    return {
        "city":      city,
        "timestamp": (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=t * 5)).isoformat(),
        "pm25": round(sample("pm25"), 2), "pm10": round(sample("pm10"), 2),
        "no2":  round(sample("no2"),  2), "o3":   round(sample("o3"),   2),
        "co":   round(sample("co"),   3), "so2":  round(sample("so2"),  2),
    }


def generate_dataset(n_days: float = 30) -> tuple[np.ndarray, np.ndarray]:
    """
    Simulate n_days of sensor data per city and build (X, y) for training.
    y = AQI 12 steps (1 hour) into the future.
    """
    X_all, y_all = [], []
    steps_per_day = 24 * 60 * 60 // 5  # readings every 5 seconds

    for city in CITY_PROFILES:
        history: list[dict] = []
        # Reduced from 30 days to 2 days for faster startup
        for t in range(int(n_days * steps_per_day)):
            hour    = (t * 5 // 3600) % 24
            reading = _synthetic_reading(city, hour, t)
            history.append(reading)

            if len(history) < HISTORY_WINDOW + 12:
                continue

            window = history[-HISTORY_WINDOW:]
            feats  = extract_features(window)
            if feats is None:
                continue

            # target = AQI 12 steps ahead
            future_reading = history[-1] if t + 12 >= len(history) else history[-(1)]
            # Use current reading's pm25 trend to estimate future AQI
            recent_pm25 = [r["pm25"] for r in history[-6:]]
            trend       = (recent_pm25[-1] - recent_pm25[0]) / max(len(recent_pm25) - 1, 1)
            future_pm25 = max(0, recent_pm25[-1] + trend * 12)
            target_aqi  = _pm25_to_aqi(future_pm25)

            X_all.append(feats)
            y_all.append(target_aqi)

    return np.array(X_all, dtype=np.float32), np.array(y_all, dtype=np.float32)


def train_and_save():
    print("[ML] Generating synthetic training data (6 hours × 8 cities)…")
    # Reduced to 0.25 days (6 hours) for instant startup in demo
    X, y = generate_dataset(n_days=0.25)
    print(f"[ML] Dataset: {X.shape[0]:,} samples, {X.shape[1]} features")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = GradientBoostingRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=4,
        subsample=0.8,
        random_state=42,
    )
    print("[ML] Training GradientBoostingRegressor…")
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    # Compatibility fix for sklearn 1.4+: squared=False is deprecated
    mse   = mean_squared_error(y_test, preds)
    rmse  = np.sqrt(mse)
    print(f"[ML] ✅  RMSE on test set: {rmse:.2f} AQI units")

    Path(MODEL_PATH).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"[ML] Model saved → {MODEL_PATH}")
    return model


if __name__ == "__main__":
    train_and_save()
