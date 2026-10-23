# 📋 URBANIX — SOFTWARE REQUIREMENTS SPECIFICATION (REVISED & FINALIZED)
## Smart City IoT Traffic & Environmental Risk Forecasting Platform

**Document Identifier:** URBANIX-SRS-V1.0-FINAL  
**Course:** INT315 — Cluster Computing Capstone  
**Release Date:** October 23, 2026 (Day 12 Final Capstone Closure)  
**Status:** **FINAL AUDITED & DEFENSE READY**  
**Repository:** `https://github.com/charan21042005/urbanix`  
**Authors / Maintainers:** Urbanix Engineering Team  

---

## 1. Executive Summary

Urbanix is a distributed smart-city analytics platform designed to ingest high-frequency traffic and environmental telemetry, aggregate sensor observations into windowed features, forecast short-term traffic velocities, dynamically recompute congestion-weighted road routes, and visualize predictions through an interactive geospatial dashboard.

Built across a 12-day modular engineering plan, Urbanix integrates Apache Kafka, Apache Spark Structured Streaming, Spark SQL, Spark MLlib, and Scala Spark GraphX, bridged by an asynchronous FastAPI gateway and Leaflet/Folium web dashboard.

### 1.1 Key Differentiators & Proven Capabilities
- **Genuine Scala Spark GraphX Rerouting:** Unlike typical capstone projects relying on Python NetworkX or GraphFrames, Urbanix implements a dedicated Scala Spark GraphX module using Google's Bulk-Synchronous Parallel (BSP) Pregel framework to compute Single-Source Shortest Paths (SSSP) on a directed property multigraph of 20 physical intersections and 40 road segments.
- **Dynamic Travel-Time Optimization:** Road routing costs are modeled as travel time in hours ($\text{cost} = \text{distance} / \text{predicted\_speed}$) rather than static Euclidean distance, with defensive gridlock floor clamping ($v \ge 1.0\text{ km/h}$) to prevent division-by-zero.
- **Forecasting Over Description:** PySpark MLlib models predict traffic velocities in the *subsequent* 5-minute interval ($t+1$), framed via chronological time-series splitting to eliminate temporal lookahead bias.
- **Verified Evaluation Metrics:** Every syllabus-named metric ($R^2$, RMSE, MAE) is computed on a held-out chronological test split. The production Linear Regression model achieves verified $R^2 = 0.7757$, $\text{RMSE} = 2.9280\text{ km/h}$, and $\text{MAE} = 2.4733\text{ km/h}$.
- **Decoupled Polyglot Contract:** Python/PySpark and Scala/GraphX are decoupled via Snappy-compressed columnar Parquet files (`data/feature_table/traffic/` and `data/recommended_routes/`), avoiding fragile JVM-Python socket interop.
- **Epistemic Freshness & Reliability:** The geospatial dashboard scans Parquet snapshots fresh per request (zero in-memory caching), evaluates timestamp freshness against operational thresholds (180s for routes, 900s for predictions), and gracefully degrades on missing, empty, or malformed data.

---

## 2. Problem Statement & Course Alignment

### 2.1 Problem Statement
Modern municipal traffic monitoring systems are predominantly *reactive*: they report congestion only after queues have formed. Furthermore, municipal data sources (inductive loop detectors, environmental air quality stations, meteorological sensors) operate in isolated silos with differing spatial bounds, update frequencies, and schema formats.

Urbanix solves this by:
1. Ingesting disparate sensor streams into decoupled, partitioned message queues.
2. Unifying telemetry into 5-minute spatial-temporal tumbling windows bounded by watermarks.
3. Training machine learning estimators to forecast corridor velocities before bottlenecks materialize.
4. Using forecasts to dynamically re-weight directed road network graphs and route emergency/municipal vehicles around impending gridlock.

### 2.2 Course Outcome (CO) Alignment Matrix

| Course Outcome | Syllabus Topic & Academic Unit | Concrete Urbanix Implementation Artifact |
| :--- | :--- | :--- |
| **CO1: Distributed Processing Fundamentals** | Unit I & II: MapReduce paradigms, cluster architecture, storage overhead | Dockerized Apache Spark Master-Worker cluster (`infra/docker-compose.yml`); memory-efficient RDD caching vs. repeated disk writes. |
| **CO2: Ingestion & Stream Processing** | Unit IV: Apache Kafka, producer-consumer architectures, micro-batching | Three independent Python Kafka producers (`services/producers/`); Kafka broker partition assignment; At-least-once delivery callbacks. |
| **CO3: Structured Streaming & Watermarking** | Unit IV: Event-time semantics, state stores, windowed aggregations | Spark Structured Streaming job (`services/streaming/streaming_job.py`); 5-min tumbling windows; 2-min event-time watermarking; Parquet sink. |
| **CO4: Distributed Feature Engineering** | Unit III & IV: Relational transformations, VectorAssembler, joins | Chronological train/test split (`prepare_training_data.py`); spatial broadcast join with `data/grid_zones.csv`; lag feature assembly. |
| **CO5: Scalable Machine Learning (MLlib)** | Unit VI: Spark MLlib regression pipelines, hyperparameter tuning, evaluation | PySpark MLlib Linear Regression and Decision Tree models (`services/streaming/train_evaluate.py`); RegressionEvaluator ($R^2$, RMSE, MAE). |
| **CO6: Distributed Graph Processing** | Unit V: GraphX, property graphs, Pregel BSP iteration, shortest paths | Scala Spark GraphX module (`services/graphx-reroute/`); 20-vertex, 40-edge directed road network; Pregel SSSP algorithm (`ShortestPathPregel.scala`). |

---

## 3. System Architecture & Component Lineage

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              URBANIX DISTRIBUTED TOPOLOGY                              │
└────────────────────────────────────────────────────────────────────────────────────────┘

 [1. Ingestion Sources]
     ├── Caltrans PeMS METR-LA Highway Speed Loop Detectors (Historical Replay)
     ├── EPA AirData Ambient PM2.5 Monitors
     └── OpenWeather Atmospheric Telemetry
             │
             ▼
 [2. Message Queuing: Apache Kafka 7.5.0]
     ├── Topic: `traffic-topic`  (Port 9092, partition 0)
     ├── Topic: `aqi-topic`      (Port 9092, partition 0)
     └── Topic: `weather-topic`  (Port 9092, partition 0)
             │
             ▼
 [3. Streaming Core: Apache Spark Structured Streaming 3.5.1]
     ├── Ingestion & Deserialization: JSON payload parsing & timestamp validation
     ├── Watermarking: .withWatermark("timestamp", "2 minutes")
     ├── Temporal Windows: window(col("timestamp"), "5 minutes")
     ├── Spatial Join: Broadcast join with `data/grid_zones.csv` on `zone_id`
     └── State Management: Write-Ahead Logs in `infra/checkpoints/traffic/`
             │
             ▼
 [4. Streaming Feature Store: Apache Parquet]
     └── Columnar partition: `data/feature_table/traffic/` (480 5-min window records)
             │
             ▼
 [5. Predictive Modeling: PySpark MLlib]
     ├── Training: `prepare_training_data.py` (365 train rows, 100 test rows)
     ├── Linear Regression (OLS): R² = 0.7757, RMSE = 2.93 km/h, MAE = 2.47 km/h
     └── Target: Next 5-minute velocity per municipal zone (t+1)
             │
             ▼
 [6. Routing Engine: Scala Spark GraphX 3.5.1 / Scala 2.12.18]
     ├── Topology: 20 Intersections (Vertices), 40 Directed Roads (Edges)
     ├── Dynamic Edge Weighting: Travel Time = Distance (km) / Forecast Speed (km/h)
     ├── Gridlock Clamping: Speed clamped to minimum 1.0 km/h floor
     └── Pregel SSSP: Iterative Bulk-Synchronous Parallel message passing
             │
             ▼
 [7. Operational Recommendation Store: Apache Parquet]
     └── `data/recommended_routes/` (20 rows, OVERWRITE mode, O(1) point query)
             │
             ▼
 [8. Web Serving Gateway: FastAPI 0.110+ / Python 3.10.11]
     ├── Scans Parquet tables fresh per HTTP request (Zero In-Memory Caching)
     ├── Freshness Evaluator: 180s route stale threshold, 900s pred stale threshold
     └── Endpoints: `/`, `/api/predictions`, `/api/route`, `/api/map`
             │
             ▼
 [9. Geospatial Console: Folium / Leaflet / Vanilla CSS]
     └── Interactive Map: Zone centroids, intersections, and GraphX PolyLine route
```

---

## 4. Functional Requirements (FR-1 through FR-10)

- **FR-1 (Ingestion Decoupling):** The system shall ingest simulated/replayed sensor observations from at least three distinct domains (traffic velocity, air quality, weather) into independent, durable Apache Kafka topics.
- **FR-2 (Unit Normalization):** Sensor velocities reported in miles per hour (mph) shall be deterministically converted to kilometers per hour ($\text{km/h} = \text{mph} \times 1.60934$) prior to message emission.
- **FR-3 (Windowed Spatial Aggregation):** The stream processing tier shall aggregate traffic telemetry into non-overlapping 5-minute tumbling event-time windows, computing average, minimum, and maximum speeds per spatial zone.
- **FR-4 (Bounded Lateness via Watermarks):** The streaming engine shall implement a 2-minute event-time watermark. Telemetry records arriving with event timestamps older than the current watermark threshold shall be dropped to bound state store size.
- **FR-5 (Supervised Model Evaluation):** The machine learning tier shall train regression models on historical windowed features using a chronological train/test split. The system shall compute and report $R^2$, RMSE, and MAE on held-out test data.
- **FR-6 (Graph Network Construction):** The routing module shall construct a directed property graph representing the municipal road network using Scala Spark GraphX, comprising at least 20 intersection vertices and 40 directed road edges.
- **FR-7 (Congestion-Weighted Edge Costs):** Road edge traversal costs shall be modeled as travel time in hours ($\text{distance\_km} / \text{speed\_kmph}$). In the event of zero or invalid speed readings, edge speeds shall be defensively clamped to a minimum floor of $1.0\text{ km/h}$.
- **FR-8 (Pregel Shortest Path Computation):** The system shall compute Single-Source Shortest Paths (SSSP) from a designated origin intersection to all reachable intersections using GraphX's Bulk-Synchronous Parallel (BSP) Pregel framework, returning both cumulative travel time and the ordered sequence of traversed vertices.
- **FR-9 (Decoupled Route Snapshot Persistence):** The GraphX routing engine shall persist optimal routing recommendations to `data/recommended_routes/` in `OVERWRITE` mode, ensuring downstream consumers receive the current point-in-time routing decision.
- **FR-10 (Freshness-Aware Geospatial Presentation):** The web dashboard shall render an interactive map displaying zone congestion heatmaps, physical intersection nodes, and the recommended route PolyLine. The dashboard shall evaluate data age against operational thresholds (180s for routes, 900s for predictions) and display prominent visual staleness indicators.

---

## 5. Machine Learning Evaluation & Results (Audited & Verified)

### 5.1 Dataset Lineage & Sample Accounting
- **Source Dataset:** Caltrans Performance Measurement System (PeMS) METR-LA loop detector telemetry (March 1, 2012).
- **Raw Window Rows:** 480 5-minute windows across 5 municipal zones.
- **Chronological Split:**
  - Training Partition (`data/train/`): 365 rows (80% split; targets $\le$ 06:20 UTC).
  - Held-out Morning Rush-Hour Partition (`data/test/`): 100 rows (features $\ge$ 06:25 UTC).
  - Boundary Windows Excluded: 15 rows dropped (gap continuity, boundary crossing, terminal row without $t+1$ label).

### 5.2 Model Performance Comparison Table

| Model Class | Architecture / Hyperparameters | Evaluated $R^2$ | RMSE ($\text{km/h}$) | MAE ($\text{km/h}$) | Acceptance Gate | Operational Role |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- |
| **Linear Regression (OLS)** | `regParam=0.0`, `fitIntercept=True`, Quasi-Newton solver | **+0.7757** | **2.9280** | **2.4733** | **PASSED ($R^2 > 0.75$)** | Production Forecasting Engine |
| **Decision Tree Regressor** | `maxDepth=8`, `minInstancesPerNode=1`, 307 leaf nodes | **-0.2064** | **6.7904** | **5.1781** | **FAILED** | Comparative Baseline |

> [!NOTE]
> **Academic Note on Decision Tree Failure:**  
> The Decision Tree suffered from severe overfitting ($R^2 = -0.2064$) due to tree depth (307 leaves on 365 samples) failing to generalize to unseen rush-hour variance. Disclosing this negative result demonstrates rigorous out-of-sample testing without lookahead leakage.

---

## 6. GraphX Routing Architecture & Edge Cost Modeling

### 6.1 Road Network Dimensions
- **Vertices ($|V|$):** 20 physical intersections (`data/intersections.csv`).
- **Edges ($|E|$):** 40 directed road segments (`data/roads.csv`).
- **Origin Node:** Intersection 1 (Civic Center / Downtown Core: $34.0549^\circ\text{ N}, -118.2498^\circ\text{ W}$).

### 6.2 Travel-Time Cost Equation
$$\text{Cost}_{\text{hours}} = \frac{\text{Distance}_{\text{km}}}{\max(\text{EffectiveSpeed}_{\text{km/h}}, 1.0)}$$
$$\text{ETA}_{\text{minutes}} = \text{Cost}_{\text{hours}} \times 60.0$$

### 6.3 Pregel SSSP Execution & Convergence
GraphX's built-in `ShortestPaths` library was rejected because it only calculates unweighted hop counts. Urbanix implements custom Pregel message passing:
1. **`vprog`:** Updates vertex state if incoming proposed travel time is strictly less than current distance.
2. **`sendMsg`:** Emits messages along outgoing directed edges (`EdgeDirection.Out`) proposing $\text{src.distance} + \text{edge.weight}$.
3. **`mergeMsg`:** Reduces competing messages at destination vertices by selecting $\min(\text{msg}_1, \text{msg}_2)$.
4. **Convergence:** Guaranteed in $\le |V| - 1 = 19$ supersteps per the Bellman-Ford theorem.

### 6.4 Verified Baseline Route for Destination 15
- **Destination:** Intersection 15 (Long Beach Blvd & Pacific Coast Highway).
- **Optimal Route Path:** `[1, 2, 14, 16, 15]`
- **Human-Readable Sequence:** `Grand Ave & 1st St -> Broadway & 7th St -> Long Beach Blvd & Imperial Hwy -> Pacific Ave & Willow St -> Long Beach Blvd & PCH`
- **Total Travel Time (ETA):** **19.0178 minutes** (0.3170 hours).
- **Dynamic Congestion Reroute (Benchmark):** Injected congestion on corridor (speed drop to $10\text{ km/h}$) causes path to dynamically divert to arterial corridor `[1, 9, 11, 15]`.

---

## 7. Reliability, Freshness & Graceful Degradation Patterns

### 7.1 Freshness Evaluation Logic
- **Predictions Update Cadence:** 300.0 seconds (5 minutes). Stale threshold multiplier: $3.0\times$. Stale threshold: **900.0 seconds**.
- **Route Snapshot Cadence:** 60.0 seconds (1 minute). Stale threshold multiplier: $3.0\times$. Stale threshold: **180.0 seconds**.
- **Clock Skew Safety:** If source timestamp is $> 5.0\text{ seconds}$ in the future, flagged as `is_stale = true` with status `FUTURE_CLOCK_SKEW`.
- **Historical Replay Handling:** Data from 2012 replay is honestly flagged as `STALE`, displaying amber badges on the dashboard.

### 7.2 Structured Error Taxonomy
All API errors return sanitized JSON envelopes without internal stack traces:
- `HTTP 503 (DATA_SOURCE_UNAVAILABLE)`: Feature table or route snapshot directory missing.
- `HTTP 500 (DATA_PARSING_ERROR)`: Corrupted Parquet part-files detected.
- `HTTP 500 (INVALID_DATA_SCHEMA)`: Parquet schema missing required partition or routing keys.
- `HTTP 404 (DESTINATION_NOT_FOUND)`: Destination ID not present in 1–20 graph registry.
- `HTTP 422 (UNPROCESSABLE_ENTITY)`: Malformed query parameter (non-integer ID).

---

## 8. Summary of Revisions & Non-Implemented Scope

1. **Replaced Linear Regression $R^2 = 0.81$ placeholder** with verified observed result: **$R^2 = 0.7757$** ($\text{RMSE} = 2.93\text{ km/h}$, $\text{MAE} = 2.47\text{ km/h}$).
2. **Formally documented Decision Tree Regressor results** ($R^2 = -0.2064$) and explained overfitting dynamics.
3. **Clarified that AQI PM2.5 forecasting is currently deferred/unimplemented**, returning `null` in the API payload while traffic velocity forecasting is fully operational.
4. **Clarified that Pregel BSP message passing replaces built-in `ShortestPaths`** to support continuous travel-time edge weighting.
5. **Preserved historical data replay distinction**, confirming that while Kafka replayed real telemetry, live physical municipal hardware was not connected.
