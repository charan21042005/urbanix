# 🚗 Traffic Streaming Checkpoint Store (`infra/checkpoints/traffic/`)

![Status](https://img.shields.io/badge/Status-Active%20%26%20Synchronized-brightgreen)
![Query](https://img.shields.io/badge/Query-traffic__query-blue)
![Topic](https://img.shields.io/badge/Source-traffic--topic-red)

This directory maintains the persistent Write-Ahead Logs (WAL) and partition state for the `traffic_query` Structured Streaming job in `services/streaming/streaming_job.py`.

---

## 🏛️ Directory Contents & Internal Role
- **`offsets/`**: Transaction log tracking starting and ending Kafka topic offsets consumed per micro-batch from `traffic-topic`.
- **`commits/`**: Atomic transaction commit log confirming that 5-minute tumbling window aggregations were successfully flushed to `data/feature_table/traffic/`.
- **`metadata`**: Spark streaming query metadata, including query ID and format version.
- **`sources/0/`**: Kafka source-specific metadata for the input topic.

---

## 🔄 Recovery Workflow
Upon a node reboot or query restart, Spark initializes this path:
```powershell
# Streaming query definition in services/streaming/streaming_job.py
query = (
    traffic_windowed.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/feature_table/traffic")
    .option("checkpointLocation", "infra/checkpoints/traffic")
    .start()
)
```
Spark inspects the highest commit log index, reads the corresponding offset mark, and resumes ingestion without dropping or duplicating a single traffic sensor record.
