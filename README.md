# 🌫️ Live Air Quality Monitoring System

A **Real-Time Air Quality Monitoring & Prediction System** that ingests live pollution data from the OpenWeatherMap API, streams it through Apache Kafka, processes it with Apache Spark Structured Streaming (or a pure-Python fallback), and displays it on a live web dashboard via WebSockets. Includes ML-powered next-hour AQI predictions using GradientBoostingRegressor.

---

## 🏛️ Architecture & Data Flow

```
  ┌─────────────────────────┐
  │  OpenWeatherMap API     │
  │  (Real pollution data)  │
  └───────────┬─────────────┘
              │
              ▼
  ┌─────────────────────────┐
  │  Kafka Producer         │   producer/aqi_producer.py
  │  Publishes to: raw-aqi  │   Every 10 seconds
  └───────────┬─────────────┘
              │
              ▼
  ┌─────────────────────────┐
  │  Apache Kafka           │   Docker (confluentinc/cp-kafka:7.6.0)
  │  Topic: "raw-aqi"       │
  └───────────┬─────────────┘
              │
              ▼
  ┌──────────────────────────────────────────┐
  │  Stream Processor                        │
  │                                          │
  │  Option A: Spark Structured Streaming    │  spark/aqi_stream_processor.py
  │            (requires Python 3.11/3.12)   │  (requires Java 11+)
  │                                          │
  │  Option B: Pure-Python Processor         │  spark/python_processor.py
  │            (no Java/Spark needed)         │  (works with any Python 3.9+)
  │                                          │
  │  Both perform:                           │
  │   • AQI calculation (EPA PM2.5 formula)  │
  │   • Category & health advisory           │
  │   • Trend detection (↑ ↓ →)              │
  │   • ML prediction (next hour)            │
  │   • Insight & alert generation           │
  │   • Priority scoring                     │
  │                                          │
  │  Publishes to: "processed-aqi"           │
  └───────────┬──────────────────────────────┘
              │
              ▼
  ┌─────────────────────────┐
  │  Kafka Consumer         │   spark/kafka_consumer.py
  │  → In-memory state      │   (runs inside dashboard process)
  │  → SQLite persistence   │
  └───────────┬─────────────┘
              │
              ▼
  ┌─────────────────────────┐
  │  Flask + Socket.IO      │   dashboard/app.py
  │  Dashboard              │   http://localhost:5001
  │  (WebSocket push 3s)    │
  └─────────────────────────┘
```

---

## ✨ Key Features

- **🌍 Live API Data**: Real pollution data from OpenWeatherMap for up to 8 cities worldwide
- **📡 Kafka Streaming**: All data flows through Kafka topics (raw-aqi → processed-aqi)
- **⚡ Spark Processing**: Structured Streaming with 5-min tumbling windows and watermarking
- **🧠 ML Predictions**: GradientBoostingRegressor predicts AQI 1 hour ahead
- **🎯 Decision Engine**: Priority-based AI insights and threshold-based alerts
- **📱 Live Dashboard**: Card-based UI with real-time WebSocket updates every 3 seconds
- **📈 Trend Detection**: Automatic rising/falling/stable trend analysis
- **🏥 Health Advisories**: EPA-aligned health guidance per AQI category
- **🌐 City Picker**: Choose from ~50 world cities or add custom lat/lon coordinates
- **💾 Persistent Storage**: SQLite for historical data queries and warm-start on restart
- **🧪 Fully Tested**: 30+ unit tests covering AQI formulas, decision engine, and storage

---

## 📦 Prerequisites

| Software | Version | Required For |
|----------|---------|-------------|
| **Python** | 3.11 or 3.12 (for Spark) / 3.9+ (for fallback) | All components |
| **Java JDK** | 11, 17, or 23 | Spark processor only |
| **Docker Desktop** | Any recent version | Kafka infrastructure |
| **OpenWeatherMap API Key** | Free tier | Real pollution data |

> ⚠️ **PySpark 3.5.1 does NOT support Python 3.13**. If you have Python 3.13, either install Python 3.11 alongside it (they coexist fine), or use the pure-Python processor fallback.

---

## 🚀 Setup (One-Time)

### 1. Clone and create virtual environment

```powershell
cd c:\Users\kumar\Downloads\LIVE_AQI

# If using Spark: use Python 3.11
py -3.11 -m venv venv311
.\venv311\Scripts\Activate

# If using fallback processor: Python 3.13 is fine
python -m venv venv
.\venv\Scripts\Activate
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
```

### 3. Create `.env` file with your API key

```powershell
echo OPENWEATHER_API_KEY=your_key_here > .env
```

Get a free key at: https://openweathermap.org/api/air-pollution

### 4. Download Hadoop binaries (Windows, one-time)

```powershell
python scripts\setup_windows.py
```

### 5. Pre-train the ML model (optional — auto-trains on first run)

```powershell
python ml\aqi_predictor.py
```

### 6. Run tests to verify setup

```powershell
python -m pytest tests/ -v
```

---

## 🏗️ Running the System (4 Terminals)

### Terminal 1 — Start Kafka Infrastructure

```powershell
docker-compose up -d
```

Wait 30 seconds, then verify: `docker ps` (all containers should show `healthy`).

Kafka UI available at: http://localhost:8080

### Terminal 2 — Start Stream Processor

**Option A: Spark (recommended)**
```powershell
.\venv311\Scripts\Activate
python spark\aqi_stream_processor.py
```
Wait for: `[Spark] Streaming query running. Press Ctrl+C to stop.`

**Option B: Pure-Python Fallback (if Spark doesn't work)**
```powershell
.\venv\Scripts\Activate
python spark\python_processor.py
```
Wait for: `Python AQI Processor started (Spark-free fallback)`

### Terminal 3 — Start Data Producer

```powershell
.\venv\Scripts\Activate
python producer\aqi_producer.py
```

> ⚠️ Start the producer **after** the stream processor is ready.

### Terminal 4 — Start Dashboard

```powershell
.\venv\Scripts\Activate
python dashboard\app.py
```

### Open Dashboard

🌐 **http://localhost:5001**

---

## 📂 Project Structure

```
LIVE_AQI/
├── config/
│   ├── settings.py                ← Central config (cities, Kafka, AQI thresholds)
│   ├── active_cities.json         ← Currently monitored cities (runtime)
│   └── mode.json                  ← Demo mode toggle state
│
├── producer/
│   └── aqi_producer.py            ← Kafka producer (OpenWeatherMap API → raw-aqi)
│
├── spark/
│   ├── aqi_stream_processor.py    ← Spark Structured Streaming (PRIMARY)
│   ├── python_processor.py        ← Pure-Python fallback (no Spark needed)
│   ├── kafka_consumer.py          ← Kafka consumer for dashboard
│   └── aqi_stream_processor_simple.py ← AQI utility functions
│
├── ml/
│   ├── aqi_predictor.py           ← Model training (GradientBoosting)
│   ├── feature_engineering.py     ← Feature extraction (25 dimensions)
│   ├── predict.py                 ← Thread-safe prediction API
│   └── models/
│       └── aqi_model.pkl          ← Trained model (auto-generated)
│
├── utils/
│   ├── decision_engine.py         ← AI insights & alert generation
│   └── message_composer.py        ← Structured message composition
│
├── dashboard/
│   ├── app.py                     ← Flask + SocketIO server
│   ├── templates/index.html       ← Dashboard HTML (2-tab layout)
│   └── static/
│       ├── style.css              ← Dark theme CSS
│       └── dashboard.js           ← Socket.IO + Chart.js frontend
│
├── storage/
│   └── storage.py                 ← SQLite persistence layer
│
├── tests/                         ← 30+ unit tests (pytest)
├── scripts/
│   └── setup_windows.py           ← Downloads Hadoop binaries for Windows
├── hadoop/bin/                    ← winutils.exe + hadoop.dll (Windows)
│
├── docker-compose.yml             ← Kafka, Zookeeper, Kafka UI
├── requirements.txt               ← Python dependencies
├── .env                           ← API key (not committed)
└── .env.example                   ← Template for .env
```

---

## 🧪 Testing

```powershell
python -m pytest tests/ -v
```

| Test File | Covers |
|-----------|--------|
| `test_aqi_calculator.py` | EPA AQI formula, category mapping, trend detection |
| `test_decision_engine.py` | Insight generation, alert thresholds, spike detection |
| `test_message_composer.py` | Message structure, priority scoring, prediction notes |
| `test_storage.py` | SQLite insert/query, persistence, multi-city queries |

---

## 📊 AQI Reference Scale (US EPA)

| AQI | Category | Health Advisory |
|-----|----------|-----------------|
| 0–50 | 🟢 Good | Enjoy outdoor activities |
| 51–100 | 🟡 Moderate | Sensitive groups reduce exertion |
| 101–150 | 🟠 Unhealthy for Sensitive | Children/respiratory patients limit outdoors |
| 151–200 | 🔴 Unhealthy | Everyone reduce heavy exertion, wear mask |
| 201–300 | 🟣 Very Unhealthy | Avoid all outdoor activities |
| 300+ | ⚫ Hazardous | Stay indoors, use air purifiers |

---

## 🔒 Important Notes

- **API Key Safety**: `.env` is gitignored and never committed. Use `.env.example` as a template.
- **Max 8 Cities**: Free OpenWeather API allows 60 req/min. 8 cities × 10s interval = 48 req/min.
- **Python Version**: PySpark 3.5.1 supports Python 3.8–3.12 only. Python 3.13+ needs the fallback processor.
- **Data Persistence**: All readings are stored in SQLite (`data/aqi_readings.db`). Dashboard warm-starts from storage on restart.

---

## 🛑 Stopping the System

To stop the system correctly, press **Ctrl+C** in each terminal in the following order:

1. **Terminal 4 (Dashboard)**: `Ctrl+C`
2. **Terminal 3 (Producer)**: `Ctrl+C`
3. **Terminal 2 (Processor)**: `Ctrl+C` (Type `Y` if prompted to terminate batch job)
4. **Terminal 1 (Infrastructure)**:
   ```powershell
   docker-compose down
   ```

---

## 🧹 Clean Start (Resetting Data)

If you see old data from previous sessions or want to reset the active city list to only those currently being produced:

1. Stop the dashboard (`Ctrl+C`).
2. Delete the historical database file:
   ```powershell
   # Windows PowerShell
   Remove-Item data\aqi_readings.db -ErrorAction SilentlyContinue
   ```
3. Restart the dashboard. The system will start fresh with only the currently active cities.

---

## 👤 Tech Stack

| Component | Technology |
|-----------|-----------|
| Message Broker | Apache Kafka 7.6.0 (Docker) |
| Stream Processing | Apache Spark 3.5.1 / Pure-Python fallback |
| ML Model | GradientBoostingRegressor (scikit-learn) |
| Web Server | Flask 3.1.3 + Flask-SocketIO |
| Real-time Push | Socket.IO (WebSocket) |
| Charts | Chart.js |
| Storage | SQLite |
| Container Runtime | Docker + Docker Compose |
