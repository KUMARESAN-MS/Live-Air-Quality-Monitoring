"""
spark/aqi_stream_processor.py
──────────────────────────────
Full Apache Spark Structured Streaming job.
Requires: docker-compose up (Kafka broker) + spark-submit

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/aqi_stream_processor.py
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
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


def main():
    spark = (
        SparkSession.builder
        .appName("LiveAQIMonitor")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/aqi_checkpoint")
        .config("spark.sql.shuffle.partitions", "4")    # small cluster friendly
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

    # ── 3. Compute instantaneous AQI & label ──────────────────────────────────
    enriched_df = (
        parsed_df
        .withColumn("aqi",      pm25_to_aqi_udf(F.col("pm25")))
        .withColumn("category", aqi_label_udf(F.col("aqi")))
    )

    # ── 4. 5-minute tumbling window  →  rolling avg per city ──────────────────
    window_duration = f"{WINDOW_DURATION_SEC} seconds"
    windowed_df = (
        enriched_df
        .groupBy(
            F.window(F.col("event_time"), window_duration),
            F.col("city"),
        )
        .agg(
            F.avg("aqi").alias("avg_aqi"),
            F.avg("pm25").alias("avg_pm25"),
            F.avg("no2").alias("avg_no2"),
            F.max("aqi").alias("max_aqi"),
            F.min("aqi").alias("min_aqi"),
            F.count("*").alias("reading_count"),
            F.last("category").alias("category"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("avg_aqi", F.round("avg_aqi", 1))
    )

    # ── 5. Write processed results to Kafka output topic ──────────────────────
    def write_to_kafka(batch_df, epoch_id):
        output = (
            batch_df
            .withColumn(
                "value",
                F.to_json(F.struct(
                    "city", "avg_aqi", "avg_pm25", "avg_no2",
                    "max_aqi", "min_aqi", "category",
                    "window_start", "window_end", "reading_count",
                ))
            )
            .select("value")
        )
        (
            output.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("topic", KAFKA_TOPIC_PROCESSED)
            .save()
        )
        batch_df.show(truncate=False)

    query = (
        windowed_df
        .writeStream
        .outputMode("update")
        .foreachBatch(write_to_kafka)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[Spark] Streaming query running. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
