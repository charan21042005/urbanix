# 🌊 Urbanix — Day 3 Stream Processing Layer Deep-Dive
> **Real-Time IoT Telemetry Processing with PySpark Structured Streaming, Event-Time Watermarks, and Parquet Feature Sinks**

> [!NOTE]
> **DEVELOPMENT NOTEBOOK & VIVA COMPENDIUM**
> This document records the complete architecture, engineering decisions, environment troubleshooting, and verified test evidence for **Day 3** of the Urbanix platform. It serves as the authoritative source of truth for viva defense, academic review, and Day 4 feature-construction handover.

---

## 🎯 1. Day 3 Objective & Scope

In **Day 1**, we provisioned the distributed container infrastructure (Kafka broker, Zookeeper, Spark Standalone Master and Worker). In **Day 2**, we implemented and verified the Python simulation layer, replaying historical loop detector speeds, EPA PM2.5 readings, and OpenWeather meteorological observations into Kafka topics.

**Day 3 Objective:** Implement and verify the **Real-Time Stream Processing Subsystem** using **Apache Spark Structured Streaming**. The streaming engine ingests raw JSON telemetry from Kafka, validates and quarantines unphysical sensor spikes, parses event timestamps, applies an event-time watermark and 5-minute tumbling windows, enriches metrics with static municipal zone metadata via a relational join, and commits feature datasets to independent Parquet tables with checkpointed fault tolerance.

```
+---------------------------------------------------------------------------------------------------------+
|                                        URBANIX DAY 3 PIPELINE                                           |
|                                                                                                         |
| [traffic-topic] --> Validated JSON --> 2-min Watermark + 5-min Window --> grid_zones.csv --> Parquet    |
|                     (Speed km/h)      [Append Mode, Checkpoint 1]                                       |
|                                                                                                         |
| [aqi-topic]     --> Validated JSON --> 2-min Watermark + 5-min Window --> grid_zones.csv --> Parquet    |
|                     (PM2.5 ug/m3)     [Append Mode, Checkpoint 2]                                       |
|                                                                                                         |
| [weather-topic] --> Validated JSON --> 2-min Watermark + 5-min Window --> grid_zones.csv --> Parquet    |
|                     (Temp C, Hum %)   [Append Mode, Checkpoint 3]                                       |
+---------------------------------------------------------------------------------------------------------+
```

### 🚫 Scope Boundaries (What Was Deliberately Excluded):
- MLlib predictive model training or inference (Scheduled for Day 4)
- Chronological train/test dataset splitting (Scheduled for Day 4)
- GraphX routing and shortest-path calculation (Scheduled for Day 5)
- REST APIs and React dashboards (Scheduled for Day 6+)
- Artificial distortion of timestamps or fabrication of vehicle counts / CO₂

---

## 🏛️ 2. Architectural Decisions & Temporal Alignment

### The Disjoint Historical Calendar Challenge
The preflight audit verified the authentic time horizons of the underlying public datasets:
- **METR-LA Traffic:** March 1, 2012 (00:00 to 19:50) at 5-minute intervals.
- **U.S. EPA AirData PM2.5:** March 1, 2012 to June 30, 2012 at daily resolution.
- **OpenWeather Meteorological Data:** October 1, 2012 to October 8, 2012 at hourly resolution.

Because the weather dataset originates from October 2012 while traffic and AQI originate from March 2012, **no synchronous temporal overlap exists across all three streams in the calendar year 2012**.

### Evaluation of Candidate Architectures

| Candidate | Strategy | Trade-offs & Risks | Decision |
|---|---|---|---|
| **Option A: Mandatory Stream-Stream Inner Join** | Perform a 3-way inner join on `zone_id` and window in Spark streaming state. | **Catastrophic Failure.** Because event-time timestamps differ by 7 months, the inner join produces 0 rows while buffering state indefinitely in memory until Out-Of-Memory (OOM) crash. | **REJECTED** |
| **Option B: Three Independent Stateful Streaming Queries (Agreed Solution)** | Run three independent streaming queries writing to separate Parquet feature directories (`data/feature_table/{traffic,aqi,weather}`). Each maintains its own checkpoint and watermark. | **Zero Data Loss & Maximum Resilience.** Each sensor pipeline functions continuously. Day 4 can combine features using defensible spatial-temporal alignment strategies without fabricating timestamps. | **ADOPTED** |
| **Option C: Synthetic Time Shift** | Rewrite original timestamps of the weather dataset from October to March 2012. | **Academic Dishonesty & Fragility.** Violates dataset provenance and breaks reproducibility against original OpenWeather sources. | **REJECTED** |

---

## 🧩 3. Component Implementation Details

### A. Schemas & Measurement Semantics (`services/streaming/schemas.py`)
Explicit PySpark `StructType` definitions adhere strictly to Day 2 producer payloads without synthetic columns:

1. **Traffic (`TRAFFIC_SCHEMA`):**
   - Fields: `zone_id` (String), `sensor_id` (String), `timestamp` (String), `speed_kmph` (Double), `speed_mph` (Double), `unit` (String), `source` (String).
   - *Physical Bounds:* $0.0 < \text{speed\_kmph} \le 250.0$.
   - *Absence of Fabricated Fields:* Loop detectors measure time-mean velocity; no vehicle count is invented.
2. **Air Quality (`AQI_SCHEMA`):**
   - Fields: `zone_id` (String), `station_id` (String), `station_name` (String), `city` (String), `timestamp` (String), `pm25` (Double), `aqi` (Integer), `unit` (String), `source` (String).
   - *Physical Bounds:* $0.0 \le \text{pm25} \le 1000.0$.
   - *Absence of Fabricated Fields:* Ground monitoring stations measure fine particulate matter; no synthetic CO₂ is added.
3. **Weather (`WEATHER_SCHEMA`):**
   - Fields: `zone_id` (String), `city` (String), `timestamp` (String), `temperature_c` (Double), `temperature_k` (Double), `humidity_pct` (Double), `temperature_unit` (String), `humidity_unit` (String), `source` (String).
   - *Physical Bounds:* $-50.0 \le \text{temperature\_c} \le 60.0$, $0.0 \le \text{humidity\_pct} \le 100.0$.

### B. Static Zone Metadata Table (`data/grid_zones.csv`)
Provides reference metadata for the 5 Urbanix platform zones:
- `zone_1`: Downtown Core (EPA Site 1103, PeMS partition 1)
- `zone_2`: San Fernando Valley (EPA Sites 1002/1201, PeMS partition 2)
- `zone_3`: San Gabriel Foothills (EPA Sites 2005/0002, PeMS partition 3)
- `zone_4`: South Central Corridor (EPA Sites 1302/1602, PeMS partition 4)
- `zone_5`: Harbor & Outer Ring (EPA Sites 4002/4004/9033, PeMS partition 5)

*Honest Spatial Semantics:* Because METR-LA traffic sensors are mapped to zones via a deterministic modulo partition for multi-zone load balancing, no fabricated polygon bounding boxes or GPS coordinates are claimed. Lat/Lon bounds are preserved as empty/null.

### C. Spark Streaming Engine (`services/streaming/streaming_job.py`)
- **Event-Time Watermark:** `withWatermark("event_time", "2 minutes")` enables state eviction and window finalization.
- **Tumbling Windows:** `groupBy(window(col("event_time"), "5 minutes"), col("zone_id"))` computes non-overlapping 300-second aggregates.
- **Output Mode:** `append` mode guarantees that only finalized, immutable window aggregates are committed to Parquet.
- **Independent Checkpoints:** Separate state stores in `infra/checkpoints/{traffic, aqi, weather}` ensure fault recovery without inter-query interference.
- **Execution Modes:** Supports continuous streaming (`processingTime="10 seconds"`) or bounded micro-batch processing (`--available-now`).

### D. Automated Verification Engine (`services/streaming/verify_features.py`)
A standalone verification utility that validates:
1. Parquet directory existence and non-emptiness.
2. Exact StructType schema conformance.
3. Tumbling window duration (asserts `window_end - window_start == 300` seconds).
4. Uniqueness of feature keys (`window_start`, `zone_id`).
5. Absence of fabricated fields (`vehicle_count`, `co2`).
6. 100% successful static zone metadata join match.

---

## ⚙️ 4. Environment & Compatibility Findings

During implementation and live testing, three critical distributed runtime hurdles were identified and resolved:

### 1. Scala Binary Incompatibility (`NoSuchMethodError`)
- **Symptom:** When loading Kafka streams, Spark threw `java.lang.NoSuchMethodError: scala.Predef$.wrapRefArray`.
- **Root Cause:** The host Spark installation (`C:\Spark`, version 3.5.9) was compiled against **Scala 2.13.8**, whereas the default maven coordinate specified was Scala 2.12 (`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`).
- **Resolution:** Updated dependency coordinates to `org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1` with automatic fallback.

### 2. Python Worker Version Mismatch (`[PYTHON_VERSION_MISMATCH]`)
- **Symptom:** Spark tasks failed with `Python in worker has different version (3, 14) than that in driver 3.10`.
- **Root Cause:** The driver was executed inside the virtual environment (`Python 3.10.11`), but Spark child worker processes inherited the system `PATH`, which pointed to Python 3.14.
- **Resolution:** Explicitly injected `PYSPARK_PYTHON = sys.executable` and `PYSPARK_DRIVER_PYTHON = sys.executable` into the execution environment.

### 3. Py4J Timezone Deserialization Offset (+05:30)
- **Symptom:** Unit tests asserting `str(row["event_time"]) == "2012-03-01 12:00:00"` returned `'2012-03-01 17:30:00'`.
- **Root Cause:** While Spark processed timestamps in UTC (`spark.sql.session.timeZone = "UTC"`), Py4J deserialized Java timestamp objects to Python `datetime` using the local Windows OS timezone (Indian Standard Time, UTC+5:30).
- **Resolution:** Standardized assertions to validate Spark SQL UTC formatting (`date_format(col("event_time"), "yyyy-MM-dd HH:mm:ss")`), ensuring tests pass deterministically on any host machine globally.

---

## 🧪 5. Verification Evidence & Test Execution

### A. Deterministic Unit Tests (`services/streaming/tests/test_streaming.py`)
Command:
```powershell
.\services\streaming\venv\Scripts\pytest services\streaming\tests\test_streaming.py -v
```
Output:
```
============================= test session starts =============================
platform win32 -- Python 3.10.11, pytest-9.1.1, pluggy-1.7.0
collected 14 items

services/streaming/tests/test_streaming.py::test_traffic_valid_json_parsing PASSED [  7%]
services/streaming/tests/test_streaming.py::test_aqi_valid_json_parsing PASSED [ 14%]
services/streaming/tests/test_streaming.py::test_weather_valid_json_parsing PASSED [ 21%]
services/streaming/tests/test_streaming.py::test_malformed_json_handling PASSED [ 28%]
services/streaming/tests/test_streaming.py::test_missing_required_fields PASSED [ 35%]
services/streaming/tests/test_streaming.py::test_invalid_timestamps PASSED [ 42%]
services/streaming/tests/test_streaming.py::test_physical_boundary_filtering PASSED [ 50%]
services/streaming/tests/test_streaming.py::test_valid_zero_measurements_where_applicable PASSED [ 57%]
services/streaming/tests/test_streaming.py::test_five_minute_window_boundaries PASSED [ 64%]
services/streaming/tests/test_streaming.py::test_aggregation_calculations PASSED [ 71%]
services/streaming/tests/test_streaming.py::test_zone_metadata_uniqueness_and_join PASSED [ 78%]
services/streaming/tests/test_streaming.py::test_absence_of_fabricated_fields PASSED [ 85%]
services/streaming/tests/test_streaming.py::test_independent_stream_processing PASSED [ 92%]
services/streaming/tests/test_streaming.py::test_parquet_schema_and_roundtrip PASSED [100%]

============================= 14 passed in 45.81s =============================
```

### B. Day 2 Producer Regression Suite
Command:
```powershell
.\services\producers\venv\Scripts\pytest services/producers/tests/test_producers.py -v
```
Output:
```
============================= 19 passed in 0.41s ==============================
```

### C. Live End-to-End Streaming & Feature Verification
Command:
```powershell
.\services\streaming\venv\Scripts\python services/streaming/verify_features.py
```
Output:
```
======================================================================
VERIFYING FEATURE DATASET: TRAFFIC (data/feature_table/traffic)
======================================================================
[*] Total rows recorded: 5
[*] Distinct zones represented (5): ['zone_1', 'zone_2', 'zone_3', 'zone_4', 'zone_5']
[*] 5-minute tumbling window duration check: PASS (All windows = 300s)
[*] Unique feature key check (window_start, zone_id): PASS (Zero duplicates)
[*] Static zone metadata join check: PASS (100% matched zone_name)
[PASS] All verification assertions PASSED for TRAFFIC!

======================================================================
VERIFYING FEATURE DATASET: AQI (data/feature_table/aqi)
======================================================================
[*] Total rows recorded: 64
[*] Distinct zones represented (2): ['zone_2', 'zone_3']
[*] 5-minute tumbling window duration check: PASS (All windows = 300s)
[*] Unique feature key check (window_start, zone_id): PASS (Zero duplicates)
[*] Static zone metadata join check: PASS (100% matched zone_name)
[PASS] All verification assertions PASSED for AQI!

======================================================================
VERIFYING FEATURE DATASET: WEATHER (data/feature_table/weather)
======================================================================
[*] Total rows recorded: 45
[*] Distinct zones represented (5): ['zone_1', 'zone_2', 'zone_3', 'zone_4', 'zone_5']
[*] 5-minute tumbling window duration check: PASS (All windows = 300s)
[*] Unique feature key check (window_start, zone_id): PASS (Zero duplicates)
[*] Static zone metadata join check: PASS (100% matched zone_name)
[PASS] All verification assertions PASSED for WEATHER!

======================================================================
FINAL VERIFICATION SUMMARY
======================================================================
  TRAFFIC   : [PASS]
  AQI       : [PASS]
  WEATHER   : [PASS]

All feature tables verified successfully.
```

---

## 🎓 6. Viva-Ready Defense Questions & Answers

### Q1: What is the fundamental difference between Event Time and Processing Time?
> **Answer:** **Event Time** is the timestamp recorded at the physical sensor source when the measurement occurred in the real world (e.g., `2012-03-01T00:00:00` on loop detector 773869). **Processing Time** is the system clock time of the Spark executor machine when the record is being processed. In distributed IoT applications, network latency and disconnections make processing time non-deterministic. Only event-time processing guarantees reproducible, accurate window aggregations regardless of arrival order.

### Q2: Why did you use Tumbling Windows instead of Sliding Windows for feature generation?
> **Answer:** A **tumbling window** partitions time into fixed, non-overlapping intervals (e.g. $[00:00, 00:05), [00:05, 00:10)$). Each event belongs to exactly one window, ensuring that observation counts and feature aggregates are mutually exclusive and additive. A **sliding window** overlaps (e.g. 15-minute window sliding every 5 minutes), which multiplies observation records across windows. For canonical feature table generation in Day 3, non-overlapping 5-minute tumbling windows provide clean, un-duplicated time-series records for machine learning training in Day 4.

### Q3: How does an Event-Time Watermark work in Spark Structured Streaming?
> **Answer:** A **watermark** is a threshold defined by `withWatermark(timeCol, delayThreshold)`. It instructs Spark: *"Assume that no event will arrive with an event timestamp older than $(\text{MaxEventTimeSeen} - \text{delayThreshold})$."* Once the watermark advances past the end of a window, Spark finalizes that window, emits the result in `Append` mode, and drops the window from the state store to free JVM memory.

### Q4: Why can't you simply perform an inner join between the three live streams?
> **Answer:** In Spark Structured Streaming, an inner stream-stream join requires events from all participating streams to match within a defined time bound. Because our verified historical datasets have disjoint calendar spans (Traffic and AQI in March–June 2012; Weather in October 2012), an inner join would evaluate to zero matches while buffering millions of unmatched states in memory, eventually exhausting heap memory. Our decoupled Option B architecture preserves all valid sensor readings independently.

### Q5: What information is stored in a Spark Structured Streaming checkpoint?
> **Answer:** A checkpoint directory stores four critical sub-structures:
> 1. `offsets/`: Kafka partition offsets processed in each micro-batch.
> 2. `commits/`: Confirmations that a micro-batch completed successfully.
> 3. `sources/`: Metadata about the data source configuration.
> 4. `state/`: RocksDB or HDFSBackedStateStore binary delta snapshots of in-flight window aggregations.
> Checkpointing guarantees **at-least-once** source reading and **idempotent exactly-once** write semantics when paired with atomic file sinks.

---

## ✅ 7. Definition-of-Done Checklist

| Item | Requirement | Status | Evidence |
|---|---|---|---|
| 1 | Explicit Spark StructType schemas for 3 streams | **PASS** | `services/streaming/schemas.py` |
| 2 | Static zone metadata table without fake coordinates | **PASS** | `data/grid_zones.csv` |
| 3 | PySpark Structured Streaming job with 3 independent queries | **PASS** | `services/streaming/streaming_job.py` |
| 4 | Event-time watermarking (2 min) and tumbling windows (5 min) | **PASS** | Verified in `streaming_job.py` and `test_streaming.py` |
| 5 | Relational zone metadata join | **PASS** | 100% matched zone names in feature tables |
| 6 | Independent Parquet feature sinks | **PASS** | `data/feature_table/{traffic, aqi, weather}` |
| 7 | Independent checkpoint directories | **PASS** | `infra/checkpoints/{traffic, aqi, weather}` |
| 8 | Checkpoint recovery verified | **PASS** | `HDFSBackedStateStoreProvider` version 2 delta reload |
| 9 | Standalone feature verification script | **PASS** | `services/streaming/verify_features.py` exits 0 |
| 10 | 14/14 automated streaming tests pass | **PASS** | `pytest services/streaming/tests/` (100%) |
| 11 | Day 2 producer tests pass without regression | **PASS** | 19/19 passed in `test_producers.py` |
| 12 | Clean Git status (no raw data or checkpoints tracked) | **PASS** | Checked via `git status --short` |

---

## 🚀 8. Handover to Day 4

With Day 3 verified, the platform now has persistent, reproducible 5-minute windowed feature tables for Los Angeles:
- `data/feature_table/traffic/`: Aggregated velocity, min/max speed, reading counts, and zone metadata.
- `data/feature_table/aqi/`: Aggregated PM2.5 concentrations, AQI indices, and station metadata.
- `data/feature_table/weather/`: Ambient temperature and humidity metrics across zones.

**Day 4 Objectives:**
1. Ingest the three Parquet feature tables.
2. Develop feature engineering transformations (lagged traffic speeds, rolling moving averages, environmental hazard indicators).
3. Implement chronological train/test splitting (strictly avoiding future-data leakage).
4. Train baseline regression and classification models using Spark MLlib.
