"""
ml/predict.py
─────────────
Thread-safe, real-time AQI prediction.
Loads the saved model once and exposes a predict() function.
"""

import sys
import threading
from pathlib import Path

import joblib
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import MODEL_PATH
from ml.feature_engineering import extract_features

_model      = None
_model_lock = threading.Lock()


def _load_model():
    global _model
    if _model is not None:
        return _model
    model_path = Path(MODEL_PATH)
    if not model_path.exists():
        # Auto-train if model file is missing
        print("[Predict] Model not found — running auto-training...")
        from ml.aqi_predictor import train_and_save
        train_and_save()
    with _model_lock:
        if _model is None:
            _model = joblib.load(MODEL_PATH)
            print(f"[Predict] Model loaded from {MODEL_PATH}")
    return _model


def predict_next_hour(history: list[dict]) -> float | None:
    """
    Given a list of recent raw sensor reading dicts (oldest → newest),
    returns the predicted AQI value for ~1 hour ahead.
    Returns None if insufficient history.
    """
    feats = extract_features(history)
    if feats is None:
        return None
    model = _load_model()
    with _model_lock:
        pred = model.predict(feats.reshape(1, -1))[0]
    return round(float(max(0, min(500, pred))), 1)
