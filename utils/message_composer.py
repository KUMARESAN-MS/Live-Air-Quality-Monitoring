"""
utils/message_composer.py
─────────────────────────
Composes structured, context-rich message objects from raw AQI signals.
Replaces flat insight/alert strings with a single coherent JSON message.

No LLM — pure deterministic rule composition.
"""

# ── Severity Mapping ──────────────────────────────────────────────────────────
_SEVERITY_TIERS = [
    (50,  "good"),
    (100, "moderate"),
    (150, "sensitive"),
    (200, "unhealthy"),
    (300, "very-unhealthy"),
    (999, "hazardous"),
]

_ADVICE_MAP = {
    "good":            "Great day for outdoor activities.",
    "moderate":        "Sensitive individuals should consider reducing prolonged outdoor exertion.",
    "sensitive":       "Children and people with respiratory diseases should limit outdoor exertion.",
    "unhealthy":       "Limit outdoor exposure. Wear a mask if going outside.",
    "very-unhealthy":  "Avoid outdoor activities. Keep windows closed.",
    "hazardous":       "Stay indoors. Use air purifiers. Health emergency conditions.",
}

_TITLE_TEMPLATES = {
    ("good",            "↑"): "Air quality is good but showing early signs of change",
    ("good",            "↓"): "Air quality is excellent and improving",
    ("good",            "→"): "Air quality is excellent and stable",
    ("moderate",        "↑"): "Air quality declining toward unhealthy levels",
    ("moderate",        "↓"): "Air quality improving from moderate levels",
    ("moderate",        "→"): "Air quality is moderate and holding steady",
    ("sensitive",       "↑"): "Air quality is unhealthy for sensitive groups and worsening",
    ("sensitive",       "↓"): "Air quality improving but still affects sensitive groups",
    ("sensitive",       "→"): "Air quality remains a concern for sensitive groups",
    ("unhealthy",       "↑"): "Air quality is unhealthy and worsening",
    ("unhealthy",       "↓"): "Air quality is beginning to recover from unhealthy levels",
    ("unhealthy",       "→"): "Air quality remains unhealthy with no improvement",
    ("very-unhealthy",  "↑"): "Dangerous air quality and still rising",
    ("very-unhealthy",  "↓"): "Very unhealthy air but showing signs of recovery",
    ("very-unhealthy",  "→"): "Very unhealthy air quality persisting",
    ("hazardous",       "↑"): "EMERGENCY: Hazardous air quality and worsening",
    ("hazardous",       "↓"): "Hazardous conditions easing slightly",
    ("hazardous",       "→"): "EMERGENCY: Hazardous air quality persisting",
}


def _get_severity(aqi: float) -> str:
    for threshold, label in _SEVERITY_TIERS:
        if aqi <= threshold:
            return label
    return "hazardous"


def _build_prediction_note(aqi: float, prediction: float) -> str:
    delta = prediction - aqi
    if delta > 15:
        return "Expected to worsen significantly in the next hour."
    if delta > 5:
        return "Expected to rise further in the next hour."
    if delta < -15:
        return "Expected to improve significantly in the next hour."
    if delta < -5:
        return "Expected to improve in the next hour."
    return "Expected to remain around current levels."


def _build_summary(severity: str, trend: str, traffic: str) -> str:
    trend_word = {"↑": "upward", "↓": "downward", "→": "stable"}.get(trend, "stable")
    base = f"{severity.replace('-', ' ').title()} air quality with {trend_word} trend."
    if traffic and traffic.lower() == "high":
        base += " Likely influenced by heavy traffic conditions."
    return base


def _get_confidence(history_len: int) -> str:
    if history_len >= 15:
        return "high"
    if history_len >= 5:
        return "medium"
    return "low"


def compose_message(
    aqi: float,
    trend: str,
    prediction: float,
    traffic: str,
    category: str,
    history_len: int = 0,
) -> dict:
    """
    Compose a structured message object from raw AQI signals.

    Returns:
        dict with keys: severity, title, summary, prediction_note, advice, confidence
    """
    severity = _get_severity(aqi)
    _trend_verb = {"↑": "worsening", "↓": "improving", "→": "unchanged"}.get(trend, "unchanged")
    fallback_title = f"Air quality is {severity.replace('-', ' ')} and {_trend_verb}"
    title = _TITLE_TEMPLATES.get((severity, trend), fallback_title)

    return {
        "severity":        severity,
        "title":           title,
        "summary":         _build_summary(severity, trend, traffic),
        "prediction_note": _build_prediction_note(aqi, prediction),
        "advice":          _ADVICE_MAP.get(severity, "Monitor conditions."),
        "confidence":      _get_confidence(history_len),
    }


def compute_priority(aqi: float, trend: str, prediction: float) -> dict:
    """
    Compute a priority score and tier for a city.

    Returns:
        dict with keys: priority ("critical"|"high"|"medium"|"low"), score (int)
    """
    score = aqi * 0.5
    if trend == "↑":
        score += 20
    if prediction > aqi:
        score += (prediction - aqi) * 0.3

    if   score >= 150: level = "critical"
    elif score >= 100: level = "high"
    elif score >= 40:  level = "medium"
    else:              level = "low"

    return {"priority": level, "score": round(score)}
