# 🛠️ Fixed: LIVE_AQI Pipeline

This document summarizes the issues found in the air quality monitoring system and the technical solutions implemented to get the full Kafka pipeline running on Windows.

## 🔴 What Was Wrong

1. **Environment Mismatch**
   - **Issue**: The dashboard and producer were failing with `ModuleNotFoundError` (e.g., `eventlet`, `kafka`).
   -  **Cause**: Commands were being run against the global Python installation instead of the project's virtual environment (`venv`) where dependencies were installed.

2. **Docker Spark Incompatibility**
   - **Issue**: The `aqi_processor` container (`apache/spark:3.5.1`) exited immediately with `ModuleNotFoundError: No module named 'pyspark'`.
   - **Cause**: The base Spark image is designed for JAR submissions or contains PySpark internally but doesn't expose it to the default `python3` command as a module without further configuration/installation.

3. **Windows `spark-submit` Failures**
   - **Issue**: Running the Spark job locally via `spark-submit` failed with `winutils.exe` errors and Java 23 security manager exceptions.
   - **Cause**: Spark has a dependency on Hadoop binaries (`winutils.exe`) which are missing on standard Windows setups. Additionally, Spark 3.5.x is currently incompatible with the `SecurityManager` changes in Java 23.

4. **Eventlet/Kafka Socket Conflict**
   - **Issue**: The Kafka consumer in the dashboard connected successfully but never received messages, even when the producer was active.
   - **Cause**: `eventlet.monkey_patch()` in `app.py` patches Python's `socket` module. The `kafka-python` library uses blocking selectors that hang when monkey-patched if not run within an eventlet-friendly greenlet.

5. **"Flat Line" Visualization & Empty Charts**
   - **Issue**: Real-world AQI changes too slowly to be visually "exciting," and the graph started empty on every reboot.
   - **Cause**: The system didn't retain history on restart, and OpenWeather Map data has very low variance over short timeframes.

---

## 🟢 What Was Fixed

1. **Native Python Kafka Consumer**
   - **Change**: Rewrote `spark/kafka_consumer.py` as a robust, pure-Python streaming processor.
   - **Result**: It now performs the same logic (AQI calculation, trend detection, and ML inference) originally intended for Spark, bypassing Hadoop/Java 23 issues on Windows.

2. **Eventlet-Compatible Spawning**
   - **Change**: Updated the consumer startup to use `eventlet.spawn()`.
   - **Result**: The consumer now runs as a greenlet, working seamlessly with monkey-patched sockets.

3. **Instant History Pre-Filling**
   - **Change**: Added an initialization loop to processors to pre-load 20 minutes of historical data (using fast simulation) on startup.
   - **Result**: The dashboard graph starts **completely full** immediately, and the ML model can provide smart predictions from the first second.

4. **UI Demo Mode Toggle**
   - **Change**: Added a "Demo Mode" button to the header and a backend controller to toggle high-variance simulation.
   - **Result**: Solves the "Flat Line" problem by allowing users to switch between accurate real-time data and "dramatic" test data for demonstrations.

---

## 🚀 How to Run

### Option A: Simulation (Easiest - 1 Terminal)
1. **Config**: Set `SIMULATION_MODE = True` in `config/settings.py`.
2. **Run**: `.\venv\Scripts\python.exe dashboard\app.py`
3. **Access**: [http://localhost:5001](http://localhost:5001)

### Option B: Full Pipeline (Production - 3 Terminals)
1. **Infrastucture**: `docker-compose up -d`
2. **Producer**: `.\venv\Scripts\python.exe producer\aqi_producer.py`
3. **Dashboard**: `.\venv\Scripts\python.exe dashboard\app.py`
4. **Access**: [http://localhost:5001](http://localhost:5001)

---

## 🛡️ Phase 2: Production-Grade Stability (Latest)

1. **Prediction Anomaly & Synthetic Contamination**
   - **Issue**: New cities showed high AQI (~150) even when current air was good.
   - **Cause**: Synthetic "pre-fill" data used a default high-pollution profile that biased the ML model before real data arrived.
   - **Fix**: Implemented **"Cold Start Mode"**. Synthetic pre-fills were removed. New cities now start with an empty history for pure accuracy.

2. **ML Data Validation Layer**
   - **Issue**: The ML model couldn't distinguish between real API data and simulation fallback.
   - **Fix**: Added `is_real: True/False` tagging. The ML predictor now explicitly **filters out** synthetic data from its calculations.

3. **Warm-Up Thresholds & Hybrid Prediction**
   - **Issue**: ML models are unstable with only 1 or 2 data points.
   - **Fix**: Added a **12-reading (1 minute) Warm-Up Threshold**. During this period, the system uses a **Linear Trend Fallback** (Moving Average) before switching to full ML inference.

4. **Dynamic City Syncing**
   - **Issue**: Adding a city in the dashboard didn't always update the background producer/consumer instantly.
   - **Fix**: Centralized city management in `config/active_cities.json`. All background processes now reload the JSON every 10 seconds to stay in perfect sync.

5. **API Rate Limit & "Spike" Logic**
   - **Issue**: Constant "Hazardous 500.0" spikes and OpenWeather connection errors.
   - **Fix**:
      - Increased polling interval from **5s to 10s** (staying under the 60 req/min limit).
      - Closed mathematical **gaps** in the EPA breakpoint table that were causing fallback "noise."
