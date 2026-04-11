"""
spark/python_processor.py
──────────────────────────
Pure-Python Kafka stream processor — FALLBACK when Spark doesn't work.

Performs the same enrichment as aqi_stream_processor.py but without
Spark (no Java/Hadoop required). Uses kafka-python directly.

Architecture:
  Kafka (raw-aqi) → python_processor.py → Kafka (processed-aqi) → Dashboard

Usage:
  python spark/python_processor.py
"""

import json
import logging
import os
import sys
from datetime import datetime

# Add root directory to sys.path to allow imports from config, ml, utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer, KafkaProducer
from config.settings import (
    KAFKA_BROKER, KAFKA_TOPIC_RAW, KAFKA_TOPIC_PROCESSED,
    PM25_BREAKPOINTS, AQI_CATEGORIES,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AQIProcessor:
    def __init__(self):
        logger.info(f"Connecting to Kafka at {KAFKA_BROKER}...")
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC_RAW,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='python-processor-group'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.city_history = {}

    def calculate_aqi(self, pm25):
        """Standard AQI calculation logic using PM25_BREAKPOINTS (EPA formula)."""
        for low, high, aqi_low, aqi_high in PM25_BREAKPOINTS:
            if low <= pm25 <= high:
                aqi = ((aqi_high - aqi_low) / (high - low)) * (pm25 - low) + aqi_low
                return round(aqi, 1)
        return 500.0  # Cap at 500

    def get_category_info(self, aqi):
        """Map AQI value to category dict (label, css, health_advisory)."""
        for cat in AQI_CATEGORIES:
            if aqi <= cat["max"]:
                return cat
        return AQI_CATEGORIES[-1]

    def _detect_trend(self, history):
        """Detect trend from AQI history: ↑ ↓ →"""
        if len(history) < 3:
            return "→"
        recent = history[-6:]
        avg_early = sum(recent[:3]) / 3
        avg_late = sum(recent[-3:]) / 3
        delta = avg_late - avg_early
        if delta > 5:
            return "↑"
        elif delta < -5:
            return "↓"
        return "→"

    def _get_prediction(self, data, fallback_aqi):
        """Run ML prediction (graceful fallback to current AQI)."""
        try:
            from ml.predict import predict_from_features
            result = predict_from_features(data)
            if result is not None:
                return result
        except Exception as e:
            logger.debug(f"ML prediction fallback: {e}")
        return fallback_aqi

    def _generate_insight(self, aqi, trend, history):
        """Generate insight string using decision engine."""
        try:
            from utils.decision_engine import generate_insight
            return generate_insight(history, aqi, trend)
        except Exception:
            return "Monitoring air quality..."

    def _generate_alert(self, aqi, history):
        """Generate alert string using decision engine."""
        try:
            from utils.decision_engine import generate_alert
            result = generate_alert(aqi, history)
            return result if result else ""
        except Exception:
            return ""

    def _compose_message(self, aqi, trend, prediction, traffic, category):
        """Compose structured message using message composer."""
        try:
            from utils.message_composer import compose_message
            return compose_message(
                aqi=aqi, trend=trend, prediction=prediction,
                traffic=traffic, category=category, history_len=10,
            )
        except Exception:
            return {"severity": "unknown", "title": "Processing...", "summary": "",
                    "prediction_note": "", "advice": "", "confidence": "low"}

    def _compute_priority(self, aqi, trend, prediction):
        """Compute priority using message composer."""
        try:
            from utils.message_composer import compute_priority
            return compute_priority(aqi=aqi, trend=trend, prediction=prediction)
        except Exception:
            return {"priority": "low", "score": 0}

    def process_message(self, data):
        """Process a single raw AQI message and return enriched data."""
        city = data.get("city")
        if not city:
            return None

        pm25 = data.get("pm25", 0)
        aqi = self.calculate_aqi(pm25)
        cat_info = self.get_category_info(aqi)

        # Accumulate AQI history per city
        if city not in self.city_history:
            self.city_history[city] = []
        self.city_history[city].append(aqi)
        self.city_history[city] = self.city_history[city][-30:]

        history = self.city_history[city]
        trend = self._detect_trend(history)

        # ML Prediction (graceful fallback)
        prediction = self._get_prediction({
            "pm25": pm25,
            "pm10": data.get("pm10", 0),
            "no2": data.get("no2", 0),
            "o3": data.get("o3", 0),
            "co": data.get("co", 0),
            "so2": data.get("so2", 0),
            "timestamp": data.get("timestamp", ""),
        }, aqi)

        pred_cat = self.get_category_info(prediction)
        traffic = data.get("traffic", "Medium")

        # Generate intelligence
        insight = self._generate_insight(aqi, trend, history)
        alert = self._generate_alert(aqi, history)
        message = self._compose_message(aqi, trend, prediction, traffic, cat_info["label"])
        priority = self._compute_priority(aqi, trend, prediction)

        # Compose enriched payload (same format as Spark processor output)
        enriched = {
            "city": city,
            "timestamp": data.get("timestamp", datetime.now().isoformat()),
            "aqi": aqi,
            "pm25": pm25,
            "pm10": data.get("pm10", 0),
            "no2": data.get("no2", 0),
            "o3": data.get("o3", 0),
            "co": data.get("co", 0),
            "so2": data.get("so2", 0),
            "category": cat_info["label"],
            "css_class": cat_info["css"],
            "health_advisory": cat_info.get("health_advisory", ""),
            "trend": trend,
            "insight": insight,
            "alert": alert,
            "traffic": traffic,
            "source": data.get("source", "Unknown"),
            "is_real": data.get("is_real", True),
            "next_hour_aqi": round(prediction, 1),
            "next_hour_label": pred_cat["label"],
            "next_hour_css": pred_cat["css"],
            "history_aqi": history[-20:],
            "message": message,
            "priority": priority,
            "avg_aqi": round(sum(history) / len(history), 1) if history else 0,
            "max_aqi": round(max(history), 1) if history else 0,
            "min_aqi": round(min(history), 1) if history else 0,
            "reading_count": len(history),
        }
        return enriched

    def run(self):
        logger.info("Python AQI Processor started (Spark-free fallback)")
        logger.info(f"Consuming: {KAFKA_TOPIC_RAW} → Publishing: {KAFKA_TOPIC_PROCESSED}")
        try:
            for message in self.consumer:
                processed = self.process_message(message.value)
                if processed:
                    self.producer.send(KAFKA_TOPIC_PROCESSED, value=processed)
                    logger.info(f"Enriched: {processed['city']:12s} | AQI: {processed['aqi']:6.1f} | {processed['category']}")
        except KeyboardInterrupt:
            logger.info("Stopping processor...")
        finally:
            self.consumer.close()
            self.producer.close()


if __name__ == "__main__":
    processor = AQIProcessor()
    processor.run()
