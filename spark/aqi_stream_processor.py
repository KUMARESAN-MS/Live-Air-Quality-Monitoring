"""
spark/aqi_stream_processor.py
──────────────────────────────
Apache Spark Structured Streaming — the SINGLE processing authority.

Reads raw sensor data from Kafka topic 'raw-aqi', computes:
  1. Instantaneous AQI (PM2.5 → EPA formula)
  2. Category label
  3. Windowed aggregations (5-min rolling avg/min/max)
  4. Trend detection
  5. ML prediction (decoupled — failure doesn't crash the stream)

Publishes fully enriched JSON to Kafka topic 'processed-aqi'.

Requires: docker-compose up (Kafka broker) + spark-submit

  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    spark/aqi_stream_processor.py
"""

import json
import sys
import os
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType, TimestampType,
)

from config.settings import (
    KAFKA_BROKER, KAFKA_TOPIC_RAW, KAFKA_TOPIC_PROCESSED,
    WINDOW_DURATION_SEC, WATERMARK_DELAY_SEC,
    PM25_BREAKPOINTS, AQI_CATEGORIES,
)

# ─── Schema for incoming JSON messages ────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("city",      StringType(),  True),
    StructField("timestamp", StringType(),  True),
    StructField("pm25",      DoubleType(),  True),
    StructField("pm10",      DoubleType(),  True),
    StructField("no2",       DoubleType(),  True),
    StructField("o3",        DoubleType(),  True),
    StructField("co",        DoubleType(),  True),
    StructField("so2",       DoubleType(),  True),
    StructField("traffic",   StringType(),  True),
    StructField("source",    StringType(),  True),
    StructField("is_real",   BooleanType(), True),
])


# ─── UDF: PM2.5 → AQI  ────────────────────────────────────────────────────────
@F.udf(returnType=DoubleType())
def pm25_to_aqi_udf(pm25: float) -> float:
    if pm25 is None:
        return 0.0
    for lo_pm, hi_pm, lo_aqi, hi_aqi in PM25_BREAKPOINTS:
        if lo_pm <= pm25 <= hi_pm:
            return float(lo_aqi + (pm25 - lo_pm) / (hi_pm - lo_pm) * (hi_aqi - lo_aqi))
    return 500.0


# ─── UDF: AQI → category label ────────────────────────────────────────────────
@F.udf(returnType=StringType())
def aqi_label_udf(aqi: float) -> str:
    if aqi is None:
        return "Unknown"
    for cat in AQI_CATEGORIES:
        if aqi <= cat["max"]:
            return cat["label"]
    return "Hazardous"


# ─── UDF: AQI → CSS class ─────────────────────────────────────────────────────
@F.udf(returnType=StringType())
def aqi_css_udf(aqi: float) -> str:
    if aqi is None:
        return "good"
    for cat in AQI_CATEGORIES:
        if aqi <= cat["max"]:
            return cat["css"]
    return "hazardous"


# ─── UDF: AQI → health advisory ───────────────────────────────────────────────
@F.udf(returnType=StringType())
def aqi_advisory_udf(aqi: float) -> str:
    if aqi is None:
        return ""
    for cat in AQI_CATEGORIES:
        if aqi <= cat["max"]:
            return cat.get("health_advisory", "")
    return AQI_CATEGORIES[-1].get("health_advisory", "")


# ─── ML Prediction (decoupled — called in foreachBatch) ──────────────────────
_ml_model = None
_ml_lock = None

def _get_ml_model():
    """Lazy-load ML model. Failure returns None (graceful fallback)."""
    global _ml_model, _ml_lock
    import threading
    if _ml_lock is None:
        _ml_lock = threading.Lock()

    if _ml_model is not None:
        return _ml_model

    try:
        import joblib
        from config.settings import MODEL_PATH
        model_path = Path(MODEL_PATH)
        if not model_path.exists():
            print("[Spark-ML] Model not found — running auto-training...")
            from ml.aqi_predictor import train_and_save
            train_and_save()
        with _ml_lock:
            if _ml_model is None:
                _ml_model = joblib.load(MODEL_PATH)
                print(f"[Spark-ML] Model loaded from {MODEL_PATH}")
        return _ml_model
    except Exception as e:
        print(f"[Spark-ML] Could not load model: {e}")
        return None


def _predict_from_row(row_dict: dict) -> float:
    """
    Run ML prediction on a single enriched row.
    Returns predicted AQI for next hour, or current AQI as fallback.
    """
    try:
        import numpy as np
        model = _get_ml_model()
        if model is None:
            return row_dict.get("aqi", 0.0)

        from ml.feature_engineering import extract_features

        # Build a minimal history-like structure from the windowed data
        # The Spark window gives us aggregated stats; we construct a feature-ready dict
        fake_history = []
        for _ in range(3):  # minimum 3 readings for extract_features
            fake_history.append({
                "pm25": row_dict.get("pm25", 0),
                "pm10": row_dict.get("pm10", 0),
                "no2":  row_dict.get("no2", 0),
                "o3":   row_dict.get("o3", 0),
                "co":   row_dict.get("co", 0),
                "so2":  row_dict.get("so2", 0),
                "timestamp": row_dict.get("timestamp", ""),
            })

        feats = extract_features(fake_history)
        if feats is None:
            return row_dict.get("aqi", 0.0)

        pred = model.predict(feats.reshape(1, -1))[0]
        return round(float(max(0, min(500, pred))), 1)
    except Exception as e:
        print(f"[Spark-ML] Prediction error: {e}")
        return row_dict.get("aqi", 0.0)


# ─── Decision engine helpers (imported from utils) ───────────────────────────
def _generate_insight(aqi: float, trend: str) -> str:
    """Generate insight string using utils decision engine."""
    try:
        from utils.decision_engine import generate_insight
        return generate_insight([aqi], aqi, trend)
    except Exception:
        return "Monitoring air quality..."


def _generate_alert(aqi: float) -> str:
    """Generate alert string using utils decision engine."""
    try:
        from utils.decision_engine import generate_alert
        result = generate_alert(aqi, [aqi])
        return result if result else ""
    except Exception:
        return ""


def _compose_message(aqi: float, trend: str, prediction: float,
                      traffic: str, category: str) -> dict:
    """Compose structured message using utils message composer."""
    try:
        from utils.message_composer import compose_message, compute_priority
        msg = compose_message(
            aqi=aqi, trend=trend, prediction=prediction,
            traffic=traffic, category=category, history_len=10,
        )
        return msg
    except Exception:
        return {"severity": "unknown", "title": "Processing...", "summary": "",
                "prediction_note": "", "advice": "", "confidence": "low"}


def _compute_priority(aqi: float, trend: str, prediction: float) -> dict:
    """Compute priority using utils message composer."""
    try:
        from utils.message_composer import compute_priority
        return compute_priority(aqi=aqi, trend=trend, prediction=prediction)
    except Exception:
        return {"priority": "low", "score": 0}


def main():
    # ─── Java 17+ / Java 23+ Compatibility Patches ─────────────────────────────
    # Spark 3.x on Java 17+ requires explicit module access and security manager permission.
    # We set these in PYSPARK_SUBMIT_ARGS *before* the gateway starts.
    jvm_flags = (
        "-Djava.security.manager=allow "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.base/java.security=ALL-UNNAMED"
    )
    
    # ─── Windows HADOOP_HOME Patch ─────────────────────────────────────────────
    # Windows requires winutils.exe in HADOOP_HOME/bin/ to handle file permissions.
    if os.name == "nt":
        base_dir = Path(__file__).resolve().parent.parent
        hadoop_dir = base_dir / "hadoop"
        if hadoop_dir.exists():
            os.environ["HADOOP_HOME"] = str(hadoop_dir)
            # Also add to PATH so JVM can find hadoop.dll
            bin_path = str(hadoop_dir / "bin")
            if bin_path not in os.environ["PATH"]:
                os.environ["PATH"] = bin_path + os.pathsep + os.environ["PATH"]
            print(f"[Spark-Env] Local HADOOP_HOME set to: {hadoop_dir}")
            print(f"[Spark-Env] Added to PATH: {bin_path}")

    # ─── Python Worker Connection Patches (Windows) ──────────────────────────
    # Ensure Spark workers use the current virtual environment's Python
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    print(f"[Spark-Env] Python worker path set to: {sys.executable}")

    # Ensure flags are passed to the JVM
    if "PYSPARK_SUBMIT_ARGS" not in os.environ:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
            f"--driver-java-options \"{jvm_flags}\" "
            f"pyspark-shell"
        )
    else:
        # Append logic if args already exist
        if "--driver-java-options" not in os.environ["PYSPARK_SUBMIT_ARGS"]:
             os.environ["PYSPARK_SUBMIT_ARGS"] += f' --driver-java-options "{jvm_flags}"'

    spark = (
        SparkSession.builder
        .appName("LiveAQIMonitor")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.driver.extraJavaOptions", jvm_flags)
        .config("spark.executor.extraJavaOptions", jvm_flags)
        # Bypassing Windows NativeIO linking issues
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.hadoop.hadoop.native.lib", "false")
        # Python Worker stability on Windows
        .config("spark.python.worker.reuse", "true")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/aqi_checkpoint")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[Spark] Session started. Listening to Kafka...")

    # ── 1. Read from Kafka ────────────────────────────────────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Parse JSON payload ─────────────────────────────────────────────────
    parsed_df = (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withWatermark("event_time", f"{WATERMARK_DELAY_SEC} seconds")
    )

    # ── 3. Compute instantaneous AQI, category, css, advisory ─────────────────
    enriched_df = (
        parsed_df
        .withColumn("aqi",             pm25_to_aqi_udf(F.col("pm25")))
        .withColumn("category",        aqi_label_udf(F.col("aqi")))
        .withColumn("css_class",       aqi_css_udf(F.col("aqi")))
        .withColumn("health_advisory", aqi_advisory_udf(F.col("aqi")))
    )

    # ── 4. 5-minute tumbling window → rolling aggregates per city ─────────────
    window_duration = f"{WINDOW_DURATION_SEC} seconds"
    windowed_df = (
        enriched_df
        .groupBy(
            F.window(F.col("event_time"), window_duration),
            F.col("city"),
        )
        .agg(
            F.round(F.avg("aqi"), 1).alias("avg_aqi"),
            F.round(F.avg("pm25"), 2).alias("avg_pm25"),
            F.round(F.avg("pm10"), 2).alias("avg_pm10"),
            F.round(F.avg("no2"), 2).alias("avg_no2"),
            F.round(F.avg("o3"), 2).alias("avg_o3"),
            F.round(F.avg("co"), 3).alias("avg_co"),
            F.round(F.avg("so2"), 2).alias("avg_so2"),
            F.max("aqi").alias("max_aqi"),
            F.min("aqi").alias("min_aqi"),
            F.count("*").alias("reading_count"),
            F.last("aqi").alias("latest_aqi"),
            F.last("pm25").alias("latest_pm25"),
            F.last("pm10").alias("latest_pm10"),
            F.last("no2").alias("latest_no2"),
            F.last("o3").alias("latest_o3"),
            F.last("co").alias("latest_co"),
            F.last("so2").alias("latest_so2"),
            F.last("category").alias("category"),
            F.last("css_class").alias("css_class"),
            F.last("health_advisory").alias("health_advisory"),
            F.last("traffic").alias("traffic"),
            F.last("source").alias("source"),
            F.last("is_real").alias("is_real"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    # ── 5. foreachBatch: enrich with trend + ML + write to Kafka ──────────────
    # Track per-city AQI history for trend detection within the driver
    _city_aqi_history = {}

    def process_and_publish(batch_df, epoch_id):
        """
        Process each micro-batch:
          - Detect trend from accumulated AQI history
          - Run ML prediction (decoupled)
          - Compose insight/alert/message
          - Publish to processed-aqi topic
        """
        if batch_df.isEmpty():
            return

        rows = batch_df.collect()
        enriched_records = []

        for row in rows:
            row_dict = row.asDict()
            city = row_dict["city"]
            aqi = round(float(row_dict.get("latest_aqi") or row_dict.get("avg_aqi", 0)), 1)

            # Accumulate AQI history for trend detection (Spark-managed state)
            if city not in _city_aqi_history:
                _city_aqi_history[city] = []
            _city_aqi_history[city].append(aqi)
            # Keep last 30 readings
            _city_aqi_history[city] = _city_aqi_history[city][-30:]

            # Trend detection from windowed history
            hist = _city_aqi_history[city]
            if len(hist) < 3:
                trend = "→"
            else:
                recent = hist[-6:]
                avg_early = sum(recent[:3]) / 3
                avg_late = sum(recent[-3:]) / 3
                delta = avg_late - avg_early
                if delta > 5:
                    trend = "↑"
                elif delta < -5:
                    trend = "↓"
                else:
                    trend = "→"

            # ML prediction (decoupled — failure = graceful fallback)
            prediction = _predict_from_row({
                "aqi": aqi,
                "pm25": row_dict.get("latest_pm25", 0),
                "pm10": row_dict.get("latest_pm10", 0),
                "no2":  row_dict.get("latest_no2", 0),
                "o3":   row_dict.get("latest_o3", 0),
                "co":   row_dict.get("latest_co", 0),
                "so2":  row_dict.get("latest_so2", 0),
                "timestamp": str(row_dict.get("window_end", "")),
            })

            category = row_dict.get("category", "Unknown")
            css_class = row_dict.get("css_class", "good")
            traffic = row_dict.get("traffic", "Unknown")
            source = row_dict.get("source", "")
            is_real = bool(row_dict.get("is_real", False))

            # Compose enriched payload
            insight = _generate_insight(aqi, trend)
            alert = _generate_alert(aqi)
            message = _compose_message(aqi, trend, prediction, traffic, category)
            priority = _compute_priority(aqi, trend, prediction)

            # Prediction category
            pred_cat = "Unknown"
            pred_css = "good"
            for cat in AQI_CATEGORIES:
                if prediction <= cat["max"]:
                    pred_cat = cat["label"]
                    pred_css = cat["css"]
                    break

            enriched = {
                "city":            city,
                "timestamp":       str(row_dict.get("window_end", "")),
                "aqi":             aqi,
                "pm25":            round(float(row_dict.get("latest_pm25", 0)), 2),
                "pm10":            round(float(row_dict.get("latest_pm10", 0)), 2),
                "no2":             round(float(row_dict.get("latest_no2", 0)), 2),
                "o3":              round(float(row_dict.get("latest_o3", 0)), 2),
                "co":              round(float(row_dict.get("latest_co", 0)), 3),
                "so2":             round(float(row_dict.get("latest_so2", 0)), 2),
                "category":        category,
                "css_class":       css_class,
                "health_advisory": row_dict.get("health_advisory", ""),
                "trend":           trend,
                "insight":         insight,
                "alert":           alert,
                "traffic":         traffic,
                "source":          source,
                "is_real":         is_real,
                "next_hour_aqi":   prediction,
                "next_hour_label": pred_cat,
                "next_hour_css":   pred_css,
                "history_aqi":     _city_aqi_history[city][-20:],
                "message":         message,
                "priority":        priority,
                # Window stats
                "avg_aqi":         round(float(row_dict.get("avg_aqi", 0)), 1),
                "max_aqi":         round(float(row_dict.get("max_aqi", 0)), 1),
                "min_aqi":         round(float(row_dict.get("min_aqi", 0)), 1),
                "reading_count":   int(row_dict.get("reading_count", 0)),
            }
            enriched_records.append(enriched)

        # Write enriched records to Kafka processed topic
        if enriched_records:
            from pyspark.sql.types import StringType as ST
            output_df = spark.createDataFrame(
                [(json.dumps(r),) for r in enriched_records],
                ["value"]
            )
            (
                output_df.write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER)
                .option("topic", KAFKA_TOPIC_PROCESSED)
                .save()
            )
            print(f"[Spark] Epoch {epoch_id}: published {len(enriched_records)} enriched records")

    query = (
        windowed_df
        .writeStream
        .outputMode("update")
        .foreachBatch(process_and_publish)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[Spark] Streaming query running. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
