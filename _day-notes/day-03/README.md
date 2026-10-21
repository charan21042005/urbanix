# Urbanix — Day 3: Spark Structured Streaming Engine & Parquet Feature Tables

Welcome to the **Day 3 Module Documentation** for Project Urbanix.

## Milestone Summary
- **Module:** Weekend 2 — Distributed Analytics & Graph Engine (Day 3 of 12)
- **Primary Language:** Python (PySpark)
- **Core Engine:** Apache Spark Structured Streaming 3.5.0
- **Streaming Primitives:** 2-minute event-time watermarks, 5-minute tumbling windows, static broadcast joins (`grid_zones.csv`), partitioned Snappy Parquet sinks
- **Status:** **VERIFIED & FORMALLY CLOSED** (All Acceptance Gates Passed)
- **Total Tests:** 14 Passing Automated Tests in `services/streaming/tests/test_streaming.py`

## Directory Contents
- [DAY_03_STREAMING_REPORT.md](DAY_03_STREAMING_REPORT.md) — Comprehensive technical report detailing Catalyst streaming execution plans, stateful watermark eviction, multi-stream tumbling window semantics, Parquet partition layouts, and empirical verification logs.
- [../../services/streaming/streaming_job.py](../../services/streaming/streaming_job.py) — Core Structured Streaming ingestion pipeline.
- [../../services/streaming/tests/test_streaming.py](../../services/streaming/tests/test_streaming.py) — 14-test automated streaming verification suite.
- [../../data/feature_table/](../../data/feature_table/) — Partitioned Parquet feature tables for traffic (480 rows), AQI, and weather.

## Quick Execution Commands
```powershell
# Navigate to streaming directory
cd services/streaming

# 1. Run automated streaming verification suite (14 tests)
python -m pytest tests/test_streaming.py -v

# 2. Launch production streaming ingestion job
python streaming_job.py
```
