"""
utils/decision_engine.py
────────────────────────
Standalone, isolated decision logic for generating human-readable insights
and real-time alerts from AQI data streams.

Isolated from Kafka, Spark, and Dashboard — fully testable standalone.

Priority Logic:
  Alerts:  Spike > Hazardous > Unhealthy > Sensitive > None
  Insights: Critical > High Variance > Elevated Trend > Stability
"""

import logging

log = logging.getLogger("DecisionEngine")
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[DecisionEngine] %(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)


def generate_insight(history_aqi: list[float], current_aqi: float, trend: str) -> str:
    """
    Generate a human-readable insight string based on air quality patterns.

    Args:
        history_aqi: Rolling window of recent AQI readings (last 20-50 points).
        current_aqi: The latest computed AQI value.
        trend: One of "↑", "↓", "→".

    Returns:
        A descriptive insight string.
    """
    if len(history_aqi) < 3:
        return "Establishing baseline data pattern..."

    recent = list(history_aqi)[-6:]
    variance = max(recent) - min(recent)

    # Priority 1: Critical Level
    if current_aqi > 200:
        log.info("Critical AQI=%.1f detected", current_aqi)
        return "Critically high pollution levels detected in this area."

    # Priority 2: High Variability
    if variance > 30:
        return "High variability in air quality detected. Conditions are changing rapidly."

    # Priority 3: Elevated Trends
    if trend == "↑" and current_aqi > 100:
        return "Pollution levels are increasing steadily. Expect worsening conditions."
    if trend == "↓" and current_aqi > 100:
        return "Air quality is beginning to improve, but remains elevated."

    # Priority 4: Normal Stability / Minor Trends
    if trend == "→" and current_aqi <= 50:
        return "Air quality is stable and excellent for outdoor activities."
    if trend == "→":
        return "Stable air quality patterns observed."
    if trend == "↑":
        return "Noticeable upward trend in particulate matter."

    return "Slight downward drift in pollution detected."


def generate_alert(current_aqi: float, history_aqi: list[float]) -> str | None:
    """
    Generate an alert string for severe threshold crossings or anomalies.

    Args:
        current_aqi: The latest computed AQI value.
        history_aqi: Rolling window of recent AQI readings.

    Returns:
        An alert string if conditions are met, otherwise None.

    Priority Order:
        1. Anomaly / Spike (>25 AQI jump in small window)
        2. Hazardous Threshold (>300)
        3. Very Unhealthy (>200)
        4. Unhealthy (>150)
        5. Sensitive Groups (>100)
    """
    if not history_aqi or len(history_aqi) < 2:
        return None

    recent = list(history_aqi)[-5:]

    # Priority 1: Anomaly / Spike
    if (recent[-1] - recent[0]) > 25:
        log.warning("Spike detected: AQI jumped %.1f -> %.1f", recent[0], recent[-1])
        return "⚠️ Rapid Pollution Spike!"

    # Priority 2: Hazardous
    if current_aqi > 300:
        log.warning("EMERGENCY: AQI=%.1f", current_aqi)
        return "🚨 EMERGENCY: Hazardous Air Quality"

    # Priority 3: Very Unhealthy
    if current_aqi > 200:
        return "🔴 Very Unhealthy Air Quality"

    # Priority 4: Unhealthy
    if current_aqi > 150:
        return "🔴 Unhealthy Air Quality"

    # Priority 5: Sensitive Groups
    if current_aqi > 100:
        return "🟠 Sensitive Groups at Risk"

    return None
