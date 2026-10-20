# 🌫️ AQI Streaming Checkpoint Store (`infra/checkpoints/aqi/`)

![Status](https://img.shields.io/badge/Status-Active%20%26%20Synchronized-brightgreen)
![Query](https://img.shields.io/badge/Query-aqi__query-blue)
![Topic](https://img.shields.io/badge/Source-aqi--topic-red)

This directory stores the Write-Ahead Logs (WAL) and partition state for the `aqi_query` Structured Streaming job in `services/streaming/streaming_job.py`.

---

## 🏛️ Directory Structure
- **`offsets/`**: Sequential offset logs tracking consumed records from `aqi-topic`.
- **`commits/`**: Atomic commit logs verifying micro-batch persistence to `data/feature_table/aqi/`.
- **`metadata`**: Structured streaming query identifier and checkpoint format schema.
- **`sources/0/`**: Kafka source metadata for the air quality topic.

---

## 🔒 Reliability Guarantees
Ensures that ambient $PM_{2.5}$ readings are committed with exactly-once semantics into the environmental risk feature store, preventing corrupted rolling averages even across sudden JVM terminates.
