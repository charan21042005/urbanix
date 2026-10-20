# 💾 Spark Structured Streaming Checkpoint Store (`infra/checkpoints/`)

![Status](https://img.shields.io/badge/Status-Active%20%26%20Fault--Tolerant-brightgreen)
![Semantics](https://img.shields.io/badge/Delivery-Exactly--Once_Processing-blue)
![Format](https://img.shields.io/badge/Store-Write--Ahead_Logs_(WAL)-orange)
![Engine](https://img.shields.io/badge/Engine-Apache_Spark_3.5.1-purple)

This directory serves as the centralized **State Store and Write-Ahead Log (WAL) Checkpoint Directory** for all streaming queries in the Urbanix platform. It provides end-to-end fault tolerance, idempotent restart recovery, and exactly-once processing guarantees across worker crashes and node restarts.

---

## 🏛️ 1. Architecture of Spark Streaming Checkpoints

When Apache Spark Structured Streaming executes an append query reading from Kafka and sinking to Parquet, it maintains transactional state inside this directory across three dedicated sub-components:

```
checkpoints/<query_name>/
├── offsets/            # Write-Ahead Log of starting and ending Kafka topic partition offsets per micro-batch
├── commits/            # Commit log recording micro-batches that successfully finished sinking to Parquet
├── metadata/           # Immutable query UUID, schema fingerprints, and engine version identifiers
└── state/              # RocksDB / HDFS state store storing in-flight watermarking & window aggregations
```

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka Broker
    participant S as Spark Worker Executor
    participant C as Checkpoint Store (WAL)
    participant P as Parquet Feature Sink

    Note over S: 1. Determine Micro-Batch Boundaries
    S->>K: Query latest available offsets
    K-->>S: Return partition offsets (e.g., 0 to 500)
    S->>C: Write offset log entry: batch_id=0 (offsets/0)
    
    Note over S: 2. Process Transformations & Window Aggregations
    S->>P: Write atomic Snappy Parquet part-files
    
    Note over S: 3. Commit Transaction
    S->>C: Write commit log entry: batch_id=0 (commits/0)
    
    Note over S: CRASH OCCURS! (Simulated Node Failure)
    S->>C: Read latest offsets and commits on reboot
    C-->>S: Batch 0 committed; Resume exactly from offset 500!
```

---

## 📁 2. Active Query Checkpoint Subdirectories

| Query Subfolder | Streaming Source Topic | Target Sink Directory | Recovery Guarantee |
| :--- | :--- | :--- | :---: |
| [`traffic/`](traffic/README.md) | `traffic-topic` | `data/feature_table/traffic/` | Exactly-Once Append |
| [`aqi/`](aqi/README.md) | `aqi-topic` | `data/feature_table/aqi/` | Exactly-Once Append |
| [`weather/`](weather/README.md) | `weather-topic` | `data/feature_table/weather/` | Exactly-Once Append |

---

## 🔒 3. Exactly-Once Processing Mechanics
- **Idempotent Writes:** Parquet part-files are stamped with unique micro-batch identifiers.
- **Atomic Commits:** If a Spark executor crashes while writing data, the corresponding file in `commits/` will not be present. On reboot, Spark automatically ignores partial, uncommitted output files and re-executes the exact micro-batch from the offset recorded in `offsets/`, ensuring zero data loss and zero duplicate records.
- **Git Hygiene:** Transient checkpoint binary logs are excluded from Git version control via `.gitignore`, while directory structure is maintained.
