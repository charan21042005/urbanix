# 🎓 URBANIX — MASTER VIVA & TECHNICAL DEFENSE QUESTION BANK
## Comprehensive Capstone Examination Preparation Across All 6 Course Units & 12 Implementation Days

**Project:** Urbanix — Smart City IoT Traffic & Environmental Risk Forecasting Platform  
**Course:** INT315 — Cluster Computing Capstone  
**Target Audience:** B.Tech Capstone Examiners, Distributed Systems Evaluators, Viva Committees  
**Pedagogical Method:** Beginner Explanation → Short Viva Answer → Deep Technical Architecture → Likely Follow-up → Anti-Fabrication Boundary  

---

## 📑 Thematic Group Index
1. [Architecture & Distributed Systems (CO1)](#group-1--architecture--distributed-systems-co1)
2. [Kafka Ingestion, Producers & Topics (CO2)](#group-2--kafka-ingestion-producers--topics-co2)
3. [Spark Structured Streaming, Windows & Watermarks (CO3)](#group-3--spark-structured-streaming-windows--watermarks-co3)
4. [Feature Engineering & Chronological Train/Test Split (CO4)](#group-4--feature-engineering--chronological-traintest-split-co4)
5. [Spark MLlib Forecasting Models & Evaluation Metrics (CO5)](#group-5--spark-mllib-forecasting-models--evaluation-metrics-co5)
6. [Scala Spark GraphX, Pregel SSSP & Routing Optimization (CO6)](#group-6--scala-spark-graphx-pregel-sssp--routing-optimization-co6)
7. [Polyglot Storage Integration & Parquet Contracts](#group-7--polyglot-storage-integration--parquet-contracts)
8. [FastAPI Gateway, Geospatial Map & Freshness Evaluation](#group-8--fastapi-gateway-geospatial-map--freshness-evaluation)
9. [Five Adversarial Examiner Questions (Audit Weaknesses & Hard Probes)](#group-9--five-adversarial-examiner-questions-audit-weaknesses--hard-probes)

---

## Group 1 — Architecture & Distributed Systems (CO1)

### Q1.1: Why use Spark Structured Streaming instead of a traditional batch MapReduce job?
- **Plain-English Explanation:** Imagine sorting mail: traditional Hadoop MapReduce waits for the post truck to deliver all letters at the end of the day, writes them to heavy boxes on disk, and sorts them once. Spark Structured Streaming is like a postal worker sorting letters immediately as each envelope drops through the mail slot, keeping the tally in memory.
- **Short Viva Answer:** Spark Structured Streaming provides sub-second micro-batch latency and natively supports event-time windowing with stateful watermarks. Hadoop MapReduce requires persisting intermediate results to HDFS disks between every Map and Reduce stage, introducing tens of seconds of disk I/O latency that violates real-time traffic dispatch SLAs.
- **Deeper Technical Answer:** Structured Streaming executes queries as incremental computations on a continuous DataFrame stream backed by the Catalyst Optimizer and Tungsten execution engine. Aggregations use in-memory state stores with Write-Ahead Logging (WAL) to checkpoints. MapReduce lacks continuous query execution, requires external schedulers (Oozie/Cron) to poll data, and cannot maintain incremental windowed state across batches without full table re-scans.
- **Likely Follow-up:** *"How does Spark's RDD caching help when retraining ML models?"*
  - *Response:* MLlib iterative optimization algorithms (like Quasi-Newton L-BFGS or Gradient Descent) pass over the training data dozens of times. Caching the feature RDD/DataFrame in memory (`persist(StorageLevel.MEMORY_AND_DISK)`) avoids re-reading from disk on every iteration, delivering a 10x–100x speedup over MapReduce.
- **Evidence in Repo:** `infra/docker-compose.yml`, `services/streaming/streaming_job.py`.
- **What NOT to Claim:** Do not claim Spark has "zero disk writes." Spark writes checkpoint offsets and state deltas to disk on every micro-batch commit.

---

## Group 2 — Kafka Ingestion, Producers & Topics (CO2)

### Q2.1: Why insert Apache Kafka between your sensor producers and Apache Spark? Why not connect producers directly to Spark?
- **Plain-English Explanation:** If 1,000 traffic cameras phoned an office worker directly all day, the worker would get overwhelmed and drop calls during rush hour. Kafka acts like a sturdy answering machine (or inbox) that records every message reliably in order, allowing the worker (Spark) to process them at their own pace without dropping anything.
- **Short Viva Answer:** Kafka decouples data ingestion from stream processing, acting as an enterprise shock-absorber (buffer). If Spark crashes or pauses for garbage collection, Kafka retains incoming sensor messages durably on disk across its partitioned commit log without losing telemetry.
- **Deeper Technical Answer:** Direct producer-to-Spark connections create tight temporal and network coupling: if Spark undergoes backpressure or worker failure, producers fail or exhaust local client memory. Kafka provides distributed log persistence, linear partition scalability, consumer offset tracking, and multi-consumer fanout (e.g., both Spark Streaming and an archival logger can read the same stream independently).
- **Likely Follow-up:** *"How many partitions do your topics have and why?"*
  - *Response:* In our development configuration, each topic (`traffic-topic`, `aqi-topic`, `weather-topic`) operates with 1 partition to guarantee total ordering per sensor type during local single-node cluster simulation. In a distributed multi-node deployment, topics are partitioned by `zone_id` to distribute load evenly across Spark executors.
- **Evidence in Repo:** `services/producers/traffic_producer.py`, `services/producers/tests/test_producers.py`.
- **What NOT to Claim:** Do not claim you have a 50-broker Kafka cluster; our cluster runs locally via Docker Compose on port 9092.

---

## Group 3 — Spark Structured Streaming, Windows & Watermarks (CO3)

### Q3.1: What is an event-time watermark, and what happens to sensor readings that arrive past the watermark?
- **Plain-English Explanation:** If you run an event starting at 5:00 PM and tell everyone, "I'll wait 2 minutes for latecomers," anyone arriving by 5:02 PM is admitted. Anyone arriving at 5:05 PM is turned away because the room has moved on and the doors are locked. The watermark is that 2-minute cutoff.
- **Short Viva Answer:** A watermark bounds how long Spark maintains memory state for late-arriving events. In Urbanix, we set a 2-minute watermark on sensor event timestamps. Records that arrive with an event timestamp older than (Current Event Time − 2 minutes) are dropped and not included in already-closed window aggregates.
- **Deeper Technical Answer:** Structured Streaming tracks `max(event_time)` observed across the stream. The watermark is calculated as $\text{Watermark} = \max(\text{event\_time}) - \text{delay}$. When the watermark passes a window's `window_end` timestamp, Spark materializes the final aggregate into the Parquet sink in `Append` mode and purges that window's state from the executor's state store memory. This guarantees that state store memory does not grow unbounded over time.
- **Likely Follow-up:** *"Why use 5-minute tumbling windows instead of sliding windows?"*
  - *Response:* Tumbling windows are non-overlapping and contiguous, which aligns with municipal reporting cadences and prevents double-counting observations in downstream machine learning feature stores.
- **Evidence in Repo:** `services/streaming/streaming_job.py` lines 14–15, 56.
- **What NOT to Claim:** Do not claim that late events are "written to a dead-letter queue" unless you built one; in standard Structured Streaming watermarking, late events are dropped from aggregation.

---

## Group 4 — Feature Engineering & Chronological Train/Test Split (CO4)

### Q4.1: Why did you split the training and testing datasets chronologically instead of randomly?
- **Plain-English Explanation:** If you shuffle a deck of cards containing daily weather reports and randomly test on cards from Monday and Wednesday using models trained on Tuesday and Thursday, the model cheats by peeking into the future. A chronological split ensures the model only trains on the past and tests on the future.
- **Short Viva Answer:** Traffic velocity is an autocorrelated time series. A random train/test split causes **temporal lookahead leakage**, where future patterns leak into the training partition, yielding artificially inflated accuracy. We split strictly by time: the first 80% (365 rows) for training, and the final 20% (100 rows) for testing.
- **Deeper Technical Answer:** When predicting $v_{t+1}$ from $v_t$, adjacent 5-minute windows share strong spatial-temporal autocorrelation. Random sampling places $t$ in train and $t+1$ in test, allowing the model to memorize smooth local transitions rather than learning generalizable dynamics. Our split uses a hard cutoff at `2012-03-01 06:24:00 UTC`. Furthermore, we excluded boundary-crossing windows at the split boundary to ensure zero target contamination.
- **Likely Follow-up:** *"How was your forecasting target variable constructed?"*
  - *Response:* We used the Spark SQL window function `lead(col("avg_speed_kmph"), 1).over(Window.partitionBy("zone_id").orderBy("window_start"))` to create `target_next_avg_speed_kmph`, dropping terminal windows that lack a $t+1$ label.
- **Evidence in Repo:** `services/streaming/prepare_training_data.py` lines 42–50, `data/feature_table/traffic/README.md`.
- **What NOT to Claim:** Do not claim you performed K-Fold Cross Validation; standard random K-Fold is statistically invalid for non-stationary time series without walk-forward expanding window splits.

---

## Group 5 — Spark MLlib Forecasting Models & Evaluation Metrics (CO5)

### Q5.1: Why are your Linear Regression metrics ($R^2 = 0.7757$, $\text{RMSE} = 2.93\text{ km/h}$) trustworthy, and why did Decision Tree fail ($R^2 = -0.2064$)?
- **Plain-English Explanation:** Linear Regression fits a smooth, sensible trend line that generalizes well to new situations. Decision Tree grew 307 tiny branches trying to memorize 365 specific numbers; when rush-hour traffic acted slightly differently, the tree made wild errors, performing worse than simply guessing the average.
- **Short Viva Answer:** Our Linear Regression model achieves a genuine $R^2$ of 0.7757 and RMSE of 2.93 km/h because it was evaluated on unseen held-out morning rush-hour traffic (`data/test/`). The Decision Tree overfit severely with 307 leaf nodes on 365 training samples, yielding a negative $R^2$ on test data.
- **Deeper Technical Answer:** $R^2$ is defined as $1 - (SS_{\text{res}} / SS_{\text{tot}})$. When a model's test predictions have residual variance exceeding the variance of the true label around its mean, $SS_{\text{res}} > SS_{\text{tot}}$, resulting in a mathematically negative $R^2$. Linear Regression regularized with Quasi-Newton optimization avoided this overfitting and captured linear morning deceleration effectively.
- **Likely Follow-up:** *"What units are RMSE and MAE in, and why report both?"*
  - *Response:* Both are in $\text{km/h}$. MAE (2.47 km/h) measures average linear magnitude of error; RMSE (2.93 km/h) squares the errors before averaging, penalizing occasional large prediction spikes more heavily. Reporting both proves that large prediction blunders are rare.
- **Evidence in Repo:** `models/README.md`, `services/streaming/train_evaluate.py`, `services/streaming/verify_models.py`.
- **What NOT to Claim:** Do not claim Decision Tree succeeded; disclose its negative $R^2$ as proof of honest, non-fabricated evaluation.

---

## Group 6 — Scala Spark GraphX, Pregel SSSP & Routing Optimization (CO6)

### Q6.1: Why build the routing engine in Scala Spark GraphX rather than a Python library like NetworkX?
- **Plain-English Explanation:** NetworkX is like a single person calculating routes on a desktop notepad; it cannot handle massive road networks distributed across multiple computers. Scala GraphX runs natively inside the Spark cluster engine on the JVM, allowing graph computations to scale to millions of intersections across a cluster.
- **Short Viva Answer:** Spark GraphX is exclusively a JVM/Scala API with no native PySpark wrapper. Building it in Scala demonstrates true cluster computing mastery. Furthermore, GraphX executes graph algorithms across distributed partitions using memory-efficient Bulk-Synchronous Parallel (BSP) Pregel iterations.
- **Deeper Technical Answer:** In PySpark, graph processing requires GraphFrames (which converts graph operations into relational DataFrame joins, adding execution overhead) or pulling graph data into single-node Python memory via NetworkX. Scala GraphX uses native vertex and edge RDDs partitioned via 2D Edge Routing, minimizing network shuffles during iterative vertex message passing.
- **Likely Follow-up:** *"Why didn't you use GraphX's built-in `ShortestPaths` library?"*
  - *Response:* GraphX's `org.apache.spark.graphx.lib.ShortestPaths` only computes **unweighted hop counts** (number of turns). It completely ignores edge distance, speed, and travel time. Urbanix required travel-time minimization, so we authored a custom Pregel SSSP implementation (`ShortestPathPregel.scala`).
- **Evidence in Repo:** `services/graphx-reroute/src/main/scala/ShortestPathPregel.scala`, `services/graphx-reroute/src/main/scala/CongestionWeight.scala`.
- **What NOT to Claim:** Do not claim GraphX can be called natively from Python via `import pyspark.graphx`; that library does not exist.

---

## Group 7 — Polyglot Storage Integration & Parquet Contracts

### Q7.1: How do Python and Scala communicate in Urbanix if they run in separate runtimes?
- **Plain-English Explanation:** Instead of trying to connect a Python phone call to a Scala speaker (which is fragile and crashes easily), Python writes its predictions into a clean, shared filing cabinet on disk (Parquet). When Scala is ready, it opens the filing cabinet, reads the latest numbers, and computes the routes.
- **Short Viva Answer:** Python and Scala integrate via a **Shared Storage Contract** using columnar Apache Parquet. Python appends predictions to `data/feature_table/traffic/`, and Scala reads them periodically. Scala writes recommended routes to `data/recommended_routes/` in `OVERWRITE` mode for the FastAPI dashboard to consume.
- **Deeper Technical Answer:** Shared storage integration enforces clean architectural boundaries: neither language runtime depends on the process lifecycle, memory space, or threading model of the other. Apache Parquet provides cross-language binary schema enforcement, dictionary encoding, and Snappy compression.
- **Likely Follow-up:** *"Why use OVERWRITE mode for routes but APPEND mode for the streaming feature table?"*
  - *Response:* The feature table represents permanent historical time-series facts, so appending preserves temporal history. Route recommendations represent a **current operational snapshot**: commuters only need the fastest route right now. Overwriting guarantees an $O(1)$ point query without requiring downstream clients to run expensive window deduplication.
- **Evidence in Repo:** `data/recommended_routes/README.md`, `services/graphx-reroute/src/main/scala/RouteRecommender.scala` lines 31–40.
- **What NOT to Claim:** Do not claim you used Py4J or JNI for live runtime function calls between Python and Scala in production.

---

## Group 8 — FastAPI Gateway, Geospatial Map & Freshness Evaluation

### Q8.1: Does your 15-second browser polling interval mean new predictions arrive every 15 seconds?
- **Plain-English Explanation:** No. Just because a wall clock ticks every second doesn't mean the mail arrives every second. The browser checks every 15 seconds so that whenever a 5-minute batch finishes, the user sees it without manually hitting the refresh button.
- **Short Viva Answer:** No. 15 seconds is simply the frontend client-side polling interval for UI responsiveness. New predictions are committed once every 5-minute tumbling window (300 seconds), and routes are recomputed on a 60-second cadence.
- **Deeper Technical Answer:** The 15-second polling interval guarantees that the browser visualizes updates within at most 15 seconds of a streaming micro-batch commit. The dashboard computes data age dynamically via `diff_seconds = now_utc - source_timestamp`. If `diff_seconds >= 900.0s` (3x window duration), the API honestly flags the payload with `"is_stale": true` and status `"STALE"`.
- **Likely Follow-up:** *"Why does FastAPI read Parquet fresh on every request instead of caching in memory?"*
  - *Response:* If FastAPI cached DataFrames in a global variable at startup, the dashboard would display frozen data even while Spark and GraphX commit fresh Parquet part-files to disk. Reading fresh ensures zero-lag visibility of filesystem state.
- **Evidence in Repo:** `dashboard/main.py` lines 22–28, 80–93, 400–500; `dashboard/static/index.html`.
- **What NOT to Claim:** Do not claim that browser polling generates new Kafka events.

---

## Group 9 — Five Adversarial Examiner Questions (Audit Weaknesses & Hard Probes)

### Hard Probe 1: *"Your dashboard map shows historical data from 2012. How can you claim this is a real-time smart city platform?"*
> **Honest Viva Answer:**  
> "We explicitly distinguish between the **data source provenance** and the **processing architecture**. Our data source is Caltrans PeMS METR-LA, a recognized academic benchmark recorded on March 1, 2012.  
> However, our **streaming and inference pipeline operates in genuine real time**: sensor producers stream these records through Kafka, Spark aggregates them in 5-minute event-time micro-batches, and GraphX calculates routes on the fly.  
> Furthermore, our dashboard honestly detects that the telemetry timestamps are historical and displays prominent amber `STALE` badges rather than falsifying timestamps to deceive evaluators."

### Hard Probe 2: *"Why is `predicted_pm25` null in your API response? Does your platform actually forecast environmental risk?"*
> **Honest Viva Answer:**  
> "In our initial Day 1–3 roadmap, we successfully implemented Kafka ingestion and 5-minute windowed feature engineering for EPA PM2.5 air quality data in `aqi-topic`.  
> However, when prioritizing our capstone scope during Weekends 2 and 3, we chose to focus our machine learning and distributed routing integration on traffic velocity to complete the Scala GraphX Pregel pipeline.  
> Rather than fabricating fake PM2.5 predictions or hardcoding synthetic numbers, our API honestly returns `predicted_pm25: null`. The schema and ingestion pipeline are production-ready for an AQI regression estimator as future work."

### Hard Probe 3: *"Why did you use a synthetic/curated road network of 20 intersections instead of OpenStreetMap for the entire city of Los Angeles?"*
> **Honest Viva Answer:**  
> "A 20-intersection, 40-directed-edge property multigraph was a deliberate pedagogical and verification decision.  
> In distributed graph processing, an unverified algorithm running on a 500,000-node graph produces output that cannot be verified by hand. With our 20-node graph, we hand-verified the Pregel algorithm against a known Dijkstra oracle on a 4-node benchmark and mathematically proved dynamic route diversion under congestion.  
> The GraphX code is written generically using distributed Vertex and Edge RDDs; scaling it to 100,000 nodes simply requires ingesting an OpenStreetMap edge list without changing the Pregel routing logic."

### Hard Probe 4: *"Your audit notes state that Spark Master and Worker were stopped during the final audit. Is the live pipeline functional right now?"*
> **Honest Viva Answer:**  
> "During our final audit, the Docker containers were stopped to protect our persisted feature store and model checkpoints from state contamination.  
> Every component has been verified at Level 1 and Level 2: 47 dashboard tests passed, 19 producer tests passed, 9 MLlib model tests passed, and GraphX routing was verified via sbt test suites.  
> We have documented the exact step-by-step Level 3 cold-start procedure in `FULL_SYSTEM_TEST.md`. The pipeline components are fully verified independently, while the live streaming cluster was paused for audit safety."

### Hard Probe 5: *"What happens if a sensor reports 0 km/h or negative speed? Does your travel time formula divide by zero?"*
> **Honest Viva Answer:**  
> "Our routing engine defends against division-by-zero through two layers of protection in `CongestionWeight.scala`:  
> First, input validation rejects negative speeds or non-finite values (NaN / Infinity) with an `IllegalArgumentException`.  
> Second, to model severe gridlock where speed approaches zero, speeds below $1.0\text{ km/h}$ are defensively clamped to a minimum floor of $1.0\text{ km/h}$. At $1.0\text{ km/h}$, a 5 km road costs 5.0 hours of travel time. This heavily penalizes the congested corridor in Pregel without producing `Double.PositiveInfinity`, preventing arithmetic crashes during message relaxation."
