# 🌦️ Weather Streaming Checkpoint Store (`infra/checkpoints/weather/`)

![Status](https://img.shields.io/badge/Status-Active%20%26%20Synchronized-brightgreen)
![Query](https://img.shields.io/badge/Query-weather__query-blue)
![Topic](https://img.shields.io/badge/Source-weather--topic-red)

This directory stores the Write-Ahead Logs (WAL) and partition state for the `weather_query` Structured Streaming job in `services/streaming/streaming_job.py`.

---

## 🏛️ Directory Structure
- **`offsets/`**: Transaction logs tracking Kafka offset progression on `weather-topic`.
- **`commits/`**: Atomic commit records confirming window aggregations written to `data/feature_table/weather/`.
- **`metadata`**: Query metadata and checkpoint version descriptors.
- **`sources/0/`**: Kafka source tracking records for weather telemetry.

---

## 🔄 Fault-Tolerant Mechanics
Guarantees that temperature, humidity, and wind-speed calculations resume from the exact message offset upon stream restarts, maintaining accurate meteorological feature states.
