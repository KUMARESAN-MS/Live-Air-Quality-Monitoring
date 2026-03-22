"""
ml/feature_engineering.py
─────────────────────────
Converts a rolling window of raw sensor readings into ML-ready features.
"""

import math
import numpy as np
import pandas as pd
from datetime import datetime


def encode_time(dt: datetime) -> tuple[float, float]:
    """Encode hour-of-day as sine/cosine so 23:00 and 01:00 are 'close'."""
    angle = 2 * math.pi * dt.hour / 24
    return math.sin(angle), math.cos(angle)


def extract_features(history: list[dict]) -> np.ndarray | None:
    """
    history : list of raw reading dicts (oldest first), len >= 3
    Returns  : 1-D numpy feature vector, or None if insufficient data
    """
    if len(history) < 3:
        return None

    df = pd.DataFrame(history)
    required = {"pm25", "pm10", "no2", "o3", "co", "so2"}
    if not required.issubset(df.columns):
        return None

    # ── Rolling statistics for primary pollutants ──────────────────────────
    features: list[float] = []
    for col in ["pm25", "pm10", "no2", "o3", "co"]:
        series = df[col].astype(float)
        features += [
            series.mean(),
            series.std(ddof=0),
            series.iloc[-1],          # latest reading
            series.diff().mean(),     # average rate of change
        ]

    # ── Lag features (t-1, t-2 PM2.5) ────────────────────────────────────
    pm25 = df["pm25"].astype(float).tolist()
    features.append(pm25[-1])
    features.append(pm25[-2] if len(pm25) >= 2 else pm25[-1])
    features.append(pm25[-3] if len(pm25) >= 3 else pm25[-1])

    # ── Time encoding ─────────────────────────────────────────────────────
    try:
        ts  = pd.to_datetime(df["timestamp"].iloc[-1], utc=True)
        sin_h, cos_h = encode_time(ts.to_pydatetime())
    except Exception:
        sin_h, cos_h = 0.0, 1.0
    features += [sin_h, cos_h]

    return np.array(features, dtype=np.float32)


def feature_dim() -> int:
    """Return the feature vector length so the model knows input size."""
    # 5 pollutants × 4 stats + 3 lags + 2 time = 25
    return 5 * 4 + 3 + 2
