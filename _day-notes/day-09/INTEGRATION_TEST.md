# 🧪 Urbanix Day 9 — End-to-End Forecast-to-Reroute Integration Test Runbook

![Status](https://img.shields.io/badge/Status-Procedure%20Verified%20%7C%20Manual%20Results%20Pending-yellow)
![Platform](https://img.shields.io/badge/OS-Windows%20PowerShell-blue)
![Stack](https://img.shields.io/badge/Stack-PySpark%20%2B%20Scala%20GraphX-orange)

---

## 🎯 1. Purpose and Scope

This runbook provides an authoritative, step-by-step engineering verification procedure for testing the **closed-loop integration** between the Python / PySpark machine learning prediction pipeline (Day 6) and the Scala Spark GraphX shortest-path routing engine (Day 8 & 9).

### What We Are Proving
1. **Shared Storage Decoupling:** The Scala routing application (`urbanix.graphx.RouteRecommender`) autonomously reads live velocity predictions from the shared Parquet feature store (`data/feature_table/traffic/`) with zero direct Python-Scala runtime interop.
2. **Dynamic Weight Translation:** Zone-level predicted speeds are mapped to directed road edges using the **destination intersection's zone**.
3. **Pregel Dynamic Shortest Path Convergence:** GraphX recomputes shortest travel-time routes from Origin Intersection 1 (`Civic Center`) to all 20 network vertices.
4. **Controlled Congestion-Spike Response:** Injecting severe congestion (e.g. speed dropping from 50+ km/h to 5–10 km/h) into an active transit zone dynamically:
   - Significantly increases the travel time along the affected corridor, and/or
   - Dynamically diverts the optimal route sequence to an alternative free-flowing arterial, matching theoretical graph oracle predictions.
5. **Dashboard-Ready Overwrite Contract:** The final route snapshot is published to `data/recommended_routes/` in `overwrite` mode for consumption by the Day 10 geospatial dashboard.

---

## 📋 2. Prerequisites & Environment Check

Before executing the manual verification procedure, ensure your Windows PowerShell terminal environment satisfies the capstone baseline:

```powershell
# 1. Verify Java 8 / 11 runtime (Oracle JDK 1.8.0_202 or compatible)
java -version

# 2. Verify Scala compiler and sbt runner
cd services/graphx-reroute
.\sbt.bat --version
cd ../..

# 3. Verify Python 3.10+ virtual environments
.\services\producers\venv\Scripts\python --version
.\services\streaming\venv\Scripts\python --version
```

---

## 🔍 3. Stage 1: Inspect the Baseline Feature Table

Confirm that the shared Parquet feature table exists and contains valid windowed predictions produced by the Day 6 MLlib inference pipeline:

```powershell
.\services\streaming\venv\Scripts\python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').getOrCreate()
df = spark.read.parquet('data/feature_table/traffic')
print('=== FEATURE TABLE PROFILE ===')
print('Total Rows:', df.count())
print('Columns:', df.columns)
df.select('zone_id', 'window_start', 'avg_speed_kmph', 'predicted_speed_kmph').show(5, truncate=False)
"
```

### Expected Output
- **Total Rows:** $\ge 480$ rows.
- **Verified Columns:** `zone_id`, `window_start`, `window_end`, `avg_speed_kmph`, `predicted_speed_kmph`, `predicted_speed`, `prediction_model`.
- **Velocity Sanity:** All speeds are positive finite values ($0 < v \le 250\text{ km/h}$).

---

## 🚀 4. Stage 2: Execute Baseline Route Recommendation

Run the Scala Route Recommender in single-pass mode (`--once`) to generate the initial baseline routing recommendations from Origin 1:

```powershell
cd services/graphx-reroute
.\sbt.bat --batch "runMain urbanix.graphx.RouteRecommender --once"
cd ../..
```

Inspect the generated route snapshot in `data/recommended_routes/`:

```powershell
.\services\streaming\venv\Scripts\python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').getOrCreate()
df = spark.read.parquet('data/recommended_routes')
print('=== BASELINE RECOMMENDED ROUTES (ORIGIN 1) ===')
df.select('destination_id', 'total_travel_time_minutes', 'path_formatted', 'is_reachable') \
  .sort('destination_id') \
  .show(20, truncate=False)
"
```

### Record Your Baseline
Identify your target destination (e.g., Destination 8, 15, 17, or 20) and record:
- **Target Destination:** e.g., `Destination 8` (Sherman Way) or `Destination 20` (San Pedro)
- **Baseline Travel Time (minutes):** e.g., `20.58 min`
- **Baseline Path Sequence:** e.g., `1 -> 2 -> 3 -> 5 -> 7 -> 8`
- **Computed At:** Wall-clock timestamp

---

## ⚡ 5. Stage 3: Controlled Congestion-Spike Injection

To prove that the routing engine genuinely responds to live predictions, we introduce a controlled congestion spike. You can execute this either via **Method A (Zero-Risk Isolated Test Table — Recommended)**, **Method B (Safe In-Place Injection with Mandatory Backup)**, or **Method C (Full Streaming Stack with Live Kafka Producers)**.

> [!CAUTION]
> **Data Safety Gate:** Before modifying the shared feature table in-place, always create a pristine filesystem backup. Never execute in-place overwrites without a verified backup.

```powershell
# Mandatory Pre-Test Safety Backup (PowerShell)
Copy-Item -Path "data\feature_table\traffic" -Destination "data\feature_table\traffic_backup" -Recurse -Force
Write-Host "[+] Pristine backup established at data/feature_table/traffic_backup" -ForegroundColor Green
```

### Method A: Zero-Risk Isolated Test Table (Highest Safety & Recommended)
Because `RouteRecommender` natively supports the `--feature-table` and `--output` CLI flags, we generate an isolated fixture directly from the production store. PySpark reads `data/feature_table/traffic` (read-only), applies the congestion spike strictly to the latest prediction window (`window_start == max(window_start)`) for `zone_2` and `zone_4`, and writes directly to `data/feature_table_spike`. This avoids copying and eliminates any Spark read-and-overwrite collision on the same directory:

```powershell
.\services\streaming\venv\Scripts\python -c "
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, max as spark_max

spark = SparkSession.builder.master('local[1]').getOrCreate()
src_df = spark.read.parquet('data/feature_table/traffic')
max_ts = src_df.agg(spark_max('window_start')).collect()[0][0]
print('[+] Identified latest prediction window:', max_ts)

# Throttle zone_2 (SF Valley) and zone_4 (South LA) to 5.0 km/h for the latest window
spiked_df = src_df.withColumn(
    'avg_speed_kmph',
    when((col('window_start') == max_ts) & col('zone_id').isin('zone_2', 'zone_4'), lit(5.0))
    .otherwise(col('avg_speed_kmph'))
)

# Write to isolated staging directory (leaves production feature store completely untouched)
spiked_df.write.mode('overwrite').parquet('data/feature_table_spike')
print('[+] Successfully materialized isolated test fixture at data/feature_table_spike')
"
```

### Method B: In-Place Synthetic Injection (With Active Backup)
This method appends an isolated synthetic row into `data/feature_table/traffic/` with a current timestamp:

```powershell
.\services\streaming\venv\Scripts\python -c "
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
spark = SparkSession.builder.master('local[1]').getOrCreate()

# Create a synthetic congestion event for zone_2 with 5.0 km/h speed
spike_data = [('zone_2', 5.0, 5.0, 5.0, 5.0, 100)]
columns = ['zone_id', 'avg_speed_kmph', 'min_speed_kmph', 'max_speed_kmph', 'predicted_speed_kmph', 'traffic_record_count']
spike_df = spark.createDataFrame(spike_data, columns) \
    .withColumn('window_start', current_timestamp()) \
    .withColumn('window_end', current_timestamp()) \
    .withColumn('predicted_speed', lit(5.0)) \
    .withColumn('prediction_model', lit('Synthetic_Congestion_Spike'))

spike_df.write.mode('append').parquet('data/feature_table/traffic')
print('[+] Synthetic congestion spike successfully injected for zone_2 (5.0 km/h).')
"
```

### Method C: Live Streaming Pipeline Injection
If running the entire distributed pipeline simultaneously:
1. **Terminal 1 (Infrastructure):** `cd infra; docker compose up -d`
2. **Terminal 2 (Producers):** `cd services/producers; .\venv\Scripts\python traffic_producer.py`
3. **Terminal 3 (Streaming Inference):** `cd services/streaming; spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming_job.py`
4. Temporarily edit `traffic_producer.py` or trigger a localized sensor drop on sensors in `zone_2`.
5. Wait for the 5-minute tumbling window micro-batch to commit into `data/feature_table/traffic`.

---

## 🔄 6. Stage 4: Re-Run Route Recommendation & Observe Adaptation

Execute `RouteRecommender` in single-pass mode. If using **Method A**, pass `--feature-table data/feature_table_spike --output data/recommended_routes_spike`:

```powershell
# For Method A (Isolated Fixture):
cd services/graphx-reroute
.\sbt.bat --batch "runMain urbanix.graphx.RouteRecommender --once --feature-table data/feature_table_spike --output data/recommended_routes_spike"
cd ../..

# For Method B or C (In-Place Table):
cd services/graphx-reroute
.\sbt.bat --batch "runMain urbanix.graphx.RouteRecommender --once"
cd ../..
```

Inspect the updated route snapshot:

```powershell
# For Method A:
.\services\streaming\venv\Scripts\python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').getOrCreate()
df = spark.read.parquet('data/recommended_routes_spike')
print('=== POST-CONGESTION RECOMMENDED ROUTES (ISOLATED RUN) ===')
df.filter(df.destination_id.isin(5, 6, 7, 8, 15)) \
  .select('destination_id', 'total_travel_time_minutes', 'path_formatted', 'computed_at') \
  .sort('destination_id') \
  .show(truncate=False)
"

# For Method B or C:
.\services\streaming\venv\Scripts\python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').getOrCreate()
df = spark.read.parquet('data/recommended_routes')
print('=== POST-CONGESTION RECOMMENDED ROUTES (IN-PLACE RUN) ===')
df.filter(df.destination_id.isin(5, 6, 7, 8, 15)) \
  .select('destination_id', 'total_travel_time_minutes', 'path_formatted', 'computed_at') \
  .sort('destination_id') \
  .show(truncate=False)
"
```

### What to Compare:
1. **Travel Time Increase (Corridor Bottleneck):** Compare Destination 8 (`1 -> 2 -> 3 -> 5 -> 7 -> 8`). Because `zone_2` dropped from ~81 km/h to 5 km/h, the travel time across the Hollywood Fwy corridor surges from ~20.58 minutes to ~308.8 minutes.
2. **Route Divergence (Rerouting Detour):** If throttling `zone_4` to 5.0 km/h, Destination 15 dynamically diverts from `1 -> 2 -> 14 -> 16 -> 15` (which traverses three congested `zone_4` edges) to `1 -> 9 -> 11 -> 15` (taking the free-flowing `zone_3` Arroyo / Rosemead corridor).

---

## 🧹 7. Stage 5: Restoration and Cleanup

To maintain pristine repository cleanliness after completing verification:

```powershell
# If Method A was used: Simply delete the temporary test directories
Remove-Item -Path "data\feature_table_spike" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "data\recommended_routes_spike" -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "[+] Isolated test directories cleanly removed. Baseline remains untouched!" -ForegroundColor Green

# If Method B was used: Restore from the verified pre-test backup
Remove-Item -Path "data\feature_table\traffic" -Recurse -Force
Copy-Item -Path "data\feature_table\traffic_backup" -Destination "data\feature_table\traffic" -Recurse -Force
Remove-Item -Path "data\feature_table\traffic_backup" -Recurse -Force
Write-Host "[+] Restored data/feature_table/traffic from verified backup." -ForegroundColor Green

# Re-run recommender to restore baseline routes snapshot
cd services/graphx-reroute
.\sbt.bat --batch "runMain urbanix.graphx.RouteRecommender --once"
cd ../..
```

---

## 🛠️ 8. Troubleshooting Guide

| Issue | Likely Cause | Resolution |
| :--- | :--- | :--- |
| **`FileNotFoundException: data/intersections.csv`** | Working directory mismatch | Run from repo root or verify paths in `services/graphx-reroute/` (the application automatically searches parent paths). |
| **`No prediction found; fallback used`** | Feature table empty or path invalid | Run `python services/streaming/verify_inference.py` to populate/verify `data/feature_table/traffic`. |
| **Route does not change after congestion spike** | Congested route is still the only topological path, or alternative is slower | Check if travel time increased; in sparse topologies without redundant cycles, travel time increases rather than path diversion. |
| **Parquet Write Error (`FileAlreadyExistsException`)** | Target permissions or locked file | Ensure `--once` mode finishes and no zombie Spark process holds a file handle on Windows. |

---

## 📊 9. Engineering Test Results & Sign-off

### Automated Regression Suites

| Test Suite | Scope | Command | Passed / Total | Verdict |
| :--- | :--- | :--- | :---: | :---: |
| **Python Producers** | Sensor schema & synthetic generators | `.\services\producers\venv\Scripts\python -m pytest services/producers/tests` | **19 / 19** | **PASS** |
| **MLlib Models** | Model deserialization & parity audit | `.\services\streaming\venv\Scripts\python services/streaming/verify_models.py` | **10 / 10** | **PASS** |
| **Streaming Inference** | Feature table bounds & schema parity | `.\services\streaming\venv\Scripts\python services/streaming/verify_inference.py` | **7 / 7** | **PASS** |
| **Streaming PyTest** | End-to-end streaming & training tests | `.\services\streaming\venv\Scripts\python -m pytest services/streaming/tests` | **44 / 44** | **PASS** |
| **Scala GraphX** | Pregel SSSP, property graph, & recommender | `cd services/graphx-reroute; .\sbt.bat --batch test` | **43 / 43** | **PASS** |

---

### Manual End-to-End Congestion-Spike Verification Table

> **Note for Academic Audit:** Per capstone engineering safety standards, the manual end-to-end verification table below is explicitly marked **PENDING** until personally executed and observed on your local workstation. Do not falsify manual verification rows.

| Run ID | Execution Timestamp (UTC) | Tested Zone | Injected Speed (km/h) | Target Destination | Baseline Route & ETA | Congested Route & ETA | Observed Delta | Verification Verdict |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **RUN-01** | *[PENDING]* | `zone_2` | `5.0 km/h` | `Dest 8` | `1->2->3->5->7->8` (20.58m) | *[PENDING]* | *[PENDING]* | **PENDING MANUAL RUN** |
| **RUN-02** | *[PENDING]* | `zone_1` | `10.0 km/h` | `Dest 4` | `1->4` (0.62m) | *[PENDING]* | *[PENDING]* | **PENDING MANUAL RUN** |
| **RUN-03** | *[PENDING]* | `zone_4` | `8.0 km/h` | `Dest 15` | `1->2->14->16->15` (19.02m) | *[PENDING]* | *[PENDING]* | **PENDING MANUAL RUN** |

---

## 🎓 10. Student Viva Quick-Reference

1. **Why does Python not call Scala directly?**  
   Shared storage via Apache Parquet provides complete polyglot decoupling. Both languages agree on the Parquet schema without relying on unstable inter-process calls, Py4J bridges, or native sockets.
2. **Why is the destination intersection's zone used?**  
   In urban traffic physics, congestion on a directed street segment $(u \to v)$ is governed by queuing, back-ups, and intersection delays at the downstream destination intersection $v$.
3. **Why overwrite mode for recommended routes?**  
   The Day 10 dashboard only needs the single current optimal route snapshot right now. Historical routes are irrelevant for real-time dispatch and would require costly filtering.
