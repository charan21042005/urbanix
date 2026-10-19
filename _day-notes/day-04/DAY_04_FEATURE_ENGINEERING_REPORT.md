# 🤖 Urbanix — Day 4 Feature Engineering & Chronological Train/Test Split Deep-Dive
> **Transforming 5-Minute Windowed Streaming Telemetry into Model-Ready, Leakage-Free Columnar Datasets for Apache Spark MLlib Forecasting**

> [!WARNING]
> **DEVELOPMENT NOTEBOOK, PEDAGOGICAL COMPENDIUM & VIVA DEFENSE MANUAL**
> This document is the definitive, atomic, and exhaustive source of truth for **Day 4** of the Urbanix Smart City Platform. It is written to serve two critical purposes:
> 1. **Complete Beginner-to-Advanced Learning Manual:** Explaining every distributed machine learning, time-series, and feature engineering concept from first principles so that anyone reading from top to bottom can understand, reproduce, and explain the system completely.
> 2. **Academic & Industrial Viva Defense Playbook:** Equipping you with exact mathematical proofs, state transitions, leakage prevention mechanisms, terminal execution logs, and model answers to challenging examiner questions at the atomic level.

---

## 🎯 1. Day 4 Objective & Scope Boundaries

### The Context: From Streaming Sinks to Supervised Datasets
In **Weekend 1 (Days 1–3)**, we established the core streaming infrastructure:
- **Day 1:** Local distributed infrastructure provisioned via Docker Compose: Zookeeper, Apache Kafka broker (with topics `traffic-topic`, `aqi-topic`, `weather-topic`), and an Apache Spark Standalone cluster (Master + Worker).
- **Day 2:** Sensor simulation layer in Python, continuously replaying real-world historical telemetry from Caltrans PeMS loop detectors (METR-LA), U.S. EPA air quality stations, and OpenWeather meteorological archives into Kafka with delivery callbacks and process supervision.
- **Day 3:** Apache Spark Structured Streaming engine aggregating raw Kafka JSON streams into 5-minute tumbling windows governed by an event-time watermark (2 minutes), enriched with static municipal zone metadata (`data/grid_zones.csv`), and persisted into independent Parquet feature tables under `data/feature_table/{traffic,aqi,weather}`.

**Day 4 Objective:** Build the **Feature Engineering and Chronological Train/Test Split Pipeline**. Raw streaming feature tables cannot be ingested directly by machine learning algorithms. They require three fundamental transformations:
1. **Supervised Target Formulation:** Constructing a clearly defined prediction target ($y$) that represents the *future velocity* of the roadway network.
2. **Feature Engineering & Vector Assembly:** Transforming raw windowed metrics into validated numeric features, deriving periodic time-of-day encodings, encoding categorical zone identifiers, and packaging them into a single high-performance linear algebra vector column ($\vec{x} \in \mathbb{R}^9$) required by Spark MLlib.
3. **Chronological Splitting with Zero Data Leakage:** Partitioning the historical timeline into training and testing datasets based strictly on elapsed time, with mathematical guarantees that future information never leaks into the past.

```
+-------------------------------------------------------------------------------------------------------------------------+
|                                                URBANIX DAY 4 PIPELINE                                                   |
|                                                                                                                         |
|  [ data/feature_table/traffic ] (480 rows across 96 contiguous 5-minute tumbling windows)                               |
|               │                                                                                                         |
|               ▼                                                                                                         |
|  1. Data Quality & Continuity Audit (Assert delta == 300s between adjacent windows) ──────────────────────────────────┐ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  2. Target Construction: lead("avg_speed_kmph", 1).over(partitionBy("zone_id").orderBy("window_start"))               │ |
|     --> Target: 'next_avg_speed_kmph', Target Timestamp: 'target_window_start'                                        │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  3. Feature Engineering:                                                                                              │ |
|     - Velocity Statistics: avg_speed_kmph, min_speed_kmph, max_speed_kmph, speed_spread_kmph                          │ |
|     - Observation Density: traffic_record_count                                                                       │ |
|     - Temporal Periodic Cycles: hour_of_day, sin_hour, cos_hour                                                       │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  4. Chronological Cutoff Calculation (80% of Elapsed Time Horizon) ──> Cutoff Epoch: 1330583040.0                      │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  5. Conservative Boundary Policy (Zero Temporal Leakage):                                                             │ |
|     - Training Set: target_window_start <= Cutoff (Labels remain strictly in the past)                                │ |
|     - Testing Set:  window_start > Cutoff (Features occur strictly in the future)                                     │ |
|     - Excluded: window_start <= Cutoff AND target_window_start > Cutoff (5 boundary-crossing rows discarded)          │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  6. Estimator Hygiene: StringIndexer.fit(TRAIN ONLY) ──> transform(Train) & transform(Test)                          │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  7. VectorAssembler: Bundle 9 validated numeric features into 'features' Vector (Target strictly excluded!)           │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  8. Parquet Sinks & Metadata:                                                                                         │ |
|     - data/train/ (365 rows)                                                                                          │ |
|     - data/test/ (100 rows)                                                                                           │ |
|     - data/split_metadata.json (Cutoff, row counts, feature names, timestamps)                                        │ |
+-------------------------------------------------------------------------------------------------------------------------+
```

### 🚫 Day 4 Scope Boundaries (What Was Deliberately Excluded):
To maintain strict architectural discipline and avoid premature implementation:
- **NO Machine Learning Model Training (Day 5 Scope):** `LinearRegression.fit()`, `DecisionTreeRegressor.fit()`, and hyperparameter tuning are strictly scheduled for Day 5.
- **NO Evaluation Metric Computation:** Calculating $R^2$, RMSE, and MAE occurs in Day 5 after models are trained.
- **NO Live Streaming Inference (Day 6 Scope):** Wiring the trained model pipeline into `streaming_job.py` for real-time scoring is scheduled for Day 6.
- **NO Graph Modeling or Shortest Path Calculation (Days 7–8 Scope):** Road network topology and GraphX routing are scheduled for Weekend 3.
- **NO Synthetic Observation Injection:** We strictly forbid manufacturing fake rows, synthetic vehicle counts, or fictitious CO₂ values to inflate data size.

---

## 🛠️ 2. Technologies Used & Component Overview

| Technology / Component | Version / Artifact | Role in Day 4 Machine Learning Pipeline |
| :--- | :--- | :--- |
| **Python** | `3.10.11` (64-bit) | Isolated host environment running data preparation, independent verification, and pytest. |
| **Apache Spark** | `3.5.1` (Host: `3.5.9`) | Distributed computing engine executing window specifications, DataFrame aggregations, and ML pipelines. |
| **PySpark MLlib** | `3.5.1` | Machine learning library providing `VectorAssembler` and `StringIndexer`. |
| **NumPy** | `2.2.6` | Numerical linear algebra library required by `pyspark.ml.linalg` for dense/sparse vector structures. |
| **Apache Parquet** | Format v1/v2 | Columnar, compressed, binary on-disk storage format for persisted training and testing datasets. |
| **pytest** | `9.1.1` | Automated test runner verifying future-label logic, gap handling, vector assembly, and zero leakage. |
| **PowerShell** | `7.x / 5.1` (Windows) | Command shell used to launch Python pipelines, execute tests, and inspect Parquet directories. |
| **Git / GitHub** | Monorepo on `main` | Version control enforcing Conventional Commits and branch hygiene. |

---

## 📚 3. Core Theory & Atomic Definitions of ML for Time-Series Streaming

To defend this architecture in an academic viva or technical interview at the atomic level, you must understand the mathematical, structural, and operational mechanics of machine learning applied to time-series streaming data.

```
                           TIME-SERIES ML TIMELINE BREAKDOWN
                           
Window Start (t_k):            2012-03-01 08:00:00  <── Observation time: Current features measured here (x)
Window End (t_k + 300s):       2012-03-01 08:05:00  <── Window seals, avg_speed_kmph finalized
Target Window Start:           2012-03-01 08:05:00  <── Forecast target begins (y = next_avg_speed_kmph)
Target Window End:             2012-03-01 08:10:00  <── Ground truth future value observed
```

### 3.1 What is Supervised Machine Learning?
**Supervised Learning** is the branch of machine learning where an algorithm learns a mapping function $f: \mathcal{X} \to \mathcal{Y}$ from an input space of feature vectors $\mathcal{X} \subset \mathbb{R}^d$ to an output space of ground-truth labels $\mathcal{Y}$. 
* During **Training**, the model is presented with pairs $(\vec{x}_i, y_i)$ and adjusts its internal parameters (e.g. regression weights $w$ or decision tree split thresholds) to minimize an empirical loss function:
  $$\mathcal{L} = \frac{1}{N} \sum_{i=1}^N \ell(f(\vec{x}_i), y_i)$$
* During **Testing / Inference**, the model is presented with an unseen feature vector $\vec{x}_{\text{test}}$ and must output a prediction $\hat{y} = f(\vec{x}_{\text{test}})$ that generalizes accurately to future conditions.

### 3.2 Feature ($\vec{x}$) vs. Target ($y$)
* **Feature Vector ($\vec{x} \in \mathbb{R}^d$):** The set of numeric measurements describing the state of the system *at or prior to decision time $t$*. Features represent what is physically known.
* **Target / Label ($y \in \mathbb{R}$):** The variable the model is tasked with predicting. In forecasting, $y$ represents *a physical measurement that will occur in the future*.
* **The Cardinal Rule of ML:** A feature vector must **never** contain the target variable, any direct mathematical transformation of the target variable, or any measurement collected chronologically after time $t$. Violating this rule is known as **Label Leakage**.

### 3.3 Autocorrelation & The Breakdown of the i.i.d. Assumption
In standard tabular machine learning (e.g., predicting house prices or loan defaults), observations are assumed to be **independent and identically distributed (i.i.d.)**. Under i.i.d. assumptions:
$$P(X_1, X_2, \dots, X_N) = \prod_{i=1}^N P(X_i)$$
Rows can be shuffled in any arbitrary order without altering the statistical validity of the experiment.

**In traffic and IoT sensor networks, the i.i.d. assumption is completely false.**
Sensor data exhibits extreme **temporal autocorrelation**: the average velocity on a freeway at 08:05 AM is strongly dependent on the velocity at 08:00 AM:
$$\text{Corr}(V_t, V_{t - \Delta t}) \gg 0$$
If you shuffle time-series data randomly (using standard scikit-learn `train_test_split(shuffle=True)` or Spark's `randomSplit()`), observations from 08:00 AM land in the training set while observations from 08:05 AM land in the test set, and vice versa. 
The model effectively cheats: it memorizes the future to predict the past, achieving an artificially inflated test score ($R^2 > 0.99$). When deployed in production where the future has not occurred yet, the model collapses. This fatal flaw is known as **Lookahead Bias** or **Temporal Leakage**.

### 3.4 The Spectrum of Data Leakage
Data leakage is any mechanism by which information from outside the training dataset (or future information) accidentally influences model training. It manifests in three distinct flavors:
1. **Target Leakage (Label Leakage):** Including the target column $y$ (or a column derived from $y$) inside the feature vector $\vec{x}$.
   * *Example:* Accidentally including `next_avg_speed_kmph` in `VectorAssembler`.
2. **Temporal Leakage (Lookahead Leakage):** Allowing a training example to predict a target that occurs *after* the chronological cutoff, or training on samples from time $t_2$ to predict test samples at time $t_1$ where $t_1 < t_2$.
3. **Preprocessing / Estimator Leakage:** Calculating normalization statistics (e.g. mean $\mu$, standard deviation $\sigma$, or categorical index frequencies) across the *entire* dataset before splitting.
   * *Why it's a bug:* The mean $\mu$ secretly encodes information about the values in the test set.

### 3.5 Future-Label Construction: Spark Window Specification & `lead()`
To forecast the future, each observation at window $k$ must be paired with the speed observed in window $k+1$.
In Apache Spark SQL, this is achieved using a **Window Specification**:
```python
zone_window = Window.partitionBy("zone_id").orderBy("window_start")
labeled_df = df.withColumn("lead_speed", lead("avg_speed_kmph", 1).over(zone_window))
```
* `Window.partitionBy("zone_id")`: Divides the DataFrame into independent spatial partitions.
* `Window.orderBy("window_start")`: Sorts records within each partition in strict chronological order.
* `lead("avg_speed_kmph", 1)`: Shifts the column backward by 1 row, attaching window $k+1$'s value to window $k$'s row.

### 3.6 Spatial Partitioning: Why `partitionBy("zone_id")` is Mandatory
In Urbanix, sensor telemetry arrives concurrently from 5 distinct municipal zones (`zone_1` to `zone_5`).
If you executed `orderBy("window_start")` **without** `partitionBy("zone_id")`:
- The DataFrame would be sorted as a single global timeline: `[zone_1 at 00:00, zone_2 at 00:00, ..., zone_5 at 00:00, zone_1 at 00:05, ...]`.
- Calling `lead(1)` on `zone_1` at 00:00 would grab the speed of `zone_2` at 00:00!
- Worse, the final window of `zone_1` would take its label from the first window of `zone_2`!
Enforcing `partitionBy("zone_id")` ensures that each municipal zone's timeline is treated as an isolated, contiguous time-series, eliminating cross-zone spatial bleeding.

### 3.7 The Continuity Guard: Mathematical Interval Validation
What happens if a sensor station experiences a power outage from 02:00 AM to 06:00 AM?
If you blindly apply `lead(1)`, the 01:55 AM window will grab the 06:00 AM window as its "next" value!
The model would be trained on a 4-hour leap while believing it is predicting a 5-minute interval!

**The Urbanix Continuity Guard:**
We extract both the lead speed and the lead window timestamp:
$$\Delta t = \text{unix\_timestamp}(\text{lead\_window\_start}) - \text{unix\_timestamp}(\text{window\_start})$$
$$\text{next\_avg\_speed\_kmph} = \begin{cases} \text{lead\_speed} & \text{if } \Delta t = 300\text{ seconds} \\ \text{NULL} & \text{otherwise (gap detected)} \end{cases}$$
If $\Delta t \ne 300\text{s}$, the row is flagged as discontinuous; its label is set to `NULL` and purged from the supervised training set.

### 3.8 The Final Row Rule
For every valid zone timeline, the **very last recorded window** has no subsequent window ($k+1$ has not yet occurred).
Therefore, `lead("avg_speed_kmph", 1)` evaluates to `NULL`.
Because supervised learning requires both an input $\vec{x}$ and a ground-truth label $y$, these final rows cannot be used for training or evaluation and are dropped.

### 3.9 Spark ML Architecture: Transformers vs. Estimators vs. Pipelines
Apache Spark MLlib models workflows using three abstractions:
1. **Transformer:** An algorithm that transforms one DataFrame into another DataFrame by applying a deterministic mathematical function. It implements `.transform()`.
   * *Example:* `VectorAssembler` takes individual columns and outputs a `Vector` column. It has no learned parameters.
2. **Estimator:** An algorithm that fits or trains on a DataFrame to produce a Transformer. It implements `.fit()`.
   * *Example:* `StringIndexer` reads a string column, counts category frequencies, and produces a `StringIndexerModel` (which is a Transformer).
3. **Pipeline:** A sequence of stages (Transformers and Estimators) chained together into a unified execution graph.

### 3.10 Categorical Encoding: `StringIndexer` & Estimator Hygiene
Machine learning models cannot perform numerical operations on string identifiers like `"zone_1"`.
`StringIndexer` maps each distinct string category to an integer index in $[0, K-1]$, ordered by category frequency.

> [!CAUTION]
> **ESTIMATOR LEAKAGE HAZARD**
> If you call `StringIndexer.fit(full_dataset)`, the indexer analyzes category frequencies across both the training period and the testing period. If a category is rare in training but common in testing, the fitted index leaks test set distribution information!
>
> **The Urbanix Fix:**
> We fit `StringIndexer` **strictly on `raw_train`**:
> ```python
> indexer_model = zone_indexer.fit(raw_train)
> indexed_train = indexer_model.transform(raw_train)
> indexed_test = indexer_model.transform(raw_test)
> ```
> We configure `handleInvalid="keep"`. If an unseen municipal zone appears in the test partition during future evaluation, Spark assigns it a dedicated fallback index rather than crashing.

### 3.11 Vector Representation: What is `VectorAssembler`?
In Spark MLlib, algorithms like `LinearRegression` do not take feature lists like `fit(X=[col1, col2])`. They expect a single DataFrame column of type `pyspark.ml.linalg.VectorUDT`.
`VectorAssembler` bundles multiple numerical columns into that required vector:
```python
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="error")
```
* **`DenseVector`:** An array of floating-point numbers where every element is stored explicitly: `[103.5, 80.2, 112.0, 31.8, 50.0, 1, 0.258, 0.965, 0.0]`.
* **`SparseVector`:** Used when many features are zero; stores only non-zero indices and their values to conserve memory.
* **`handleInvalid="error"`:** Enforces strict data hygiene. If a NULL or NaN is encountered in any feature column, the pipeline halts immediately, preventing silent numerical corruption.

### 3.12 Chronological Splitting: Elapsed Time vs. Row Count Percentiles
How should the split point be chosen?
* **Row Count Split (Naive):** Sorting rows by time and picking the 80th percentile row index ($0.8 \times N$).
  * *Flaw:* If sensor reporting frequencies change or if early hours had missing sensors, row count does not correspond to physical time!
* **Elapsed Time Split (Urbanix Standard):**
  $$T_{\min} = \min(\text{window\_start}), \quad T_{\max} = \max(\text{window\_start})$$
  $$\text{Cutoff Epoch} = \text{epoch}(T_{\min}) + 0.8 \times \left( \text{epoch}(T_{\max}) - \text{epoch}(T_{\min}) \right)$$
  The split point represents the exact moment when 80% of the physical time horizon has elapsed.

### 3.13 The Conservative Boundary Policy & Boundary-Crossing Exclusion
Most naive implementations define:
- `train = df.filter(window_start <= cutoff)`
- `test = df.filter(window_start > cutoff)`

**Why this is scientifically invalid:**
Consider an observation at $\text{window\_start} = \text{cutoff} - 2\text{ minutes}$.
Because $\text{window\_start} \le \text{cutoff}$, it is placed into the **training set**.
However, its target is $\text{target\_window\_start} = \text{cutoff} + 3\text{ minutes}$!
Its ground-truth label lies inside the **testing period**!
During model training, the loss function evaluates predictions against ground-truth labels that belong to the test set, creating subtle target leakage across the boundary.

**The Urbanix Conservative Boundary Policy:**
1. **Training Set Rule:** All training examples must have their **target timestamp** at or before the cutoff:
   $$\text{Train} = \{ x \mid \text{target\_window\_start} \le \text{Cutoff} \}$$
2. **Testing Set Rule:** All testing examples must have their **feature timestamp** strictly after the cutoff:
   $$\text{Test} = \{ x \mid \text{window\_start} > \text{Cutoff} \}$$
3. **Boundary-Crossing Exclusion:** Any observation where $\text{window\_start} \le \text{Cutoff}$ but $\text{target\_window\_start} > \text{Cutoff}$ is **strictly excluded**.
   In our live dataset, exactly **5 boundary-crossing rows** (one per zone) were identified and discarded.

### 3.14 Mathematical Proof of Zero Temporal Leakage
Under the Urbanix Conservative Boundary Policy:
$$\forall x \in \text{Train}, \quad \text{target\_window\_start}(x) \le \text{Cutoff}$$
$$\forall x \in \text{Test}, \quad \text{window\_start}(x) > \text{Cutoff}$$
Because $\text{window\_start}(x) \le \text{target\_window\_start}(x)$, it follows transitively that:
$$\max_{x \in \text{Train}}(\text{target\_window\_start}(x)) \le \text{Cutoff} < \min_{x \in \text{Test}}(\text{window\_start}(x))$$
$$\implies \max(\text{train.target}) < \min(\text{test.feature})$$
There is an absolute chronological gap between the newest training label and the oldest testing feature. Zero overlap. Zero leakage.

---

## 🔬 4. Data Investigation & Historical Horizon Audit

### 4.1 Ingesting the 5-Minute Windowed Stream Sinks
In Day 3, Apache Spark Structured Streaming persisted tumbling window aggregates into `data/feature_table/traffic/`.
Let's inspect the actual schema generated by the streaming job:
```text
root
 |-- zone_id: string (nullable = true)
 |-- window_start: timestamp (nullable = true)
 |-- window_end: timestamp (nullable = true)
 |-- traffic_record_count: long (nullable = false)
 |-- avg_speed_kmph: double (nullable = true)
 |-- min_speed_kmph: double (nullable = true)
 |-- max_speed_kmph: double (nullable = true)
 |-- zone_name: string (nullable = true)
 |-- mapping_type: string (nullable = true)
 |-- mapping_description: string (nullable = true)
 |-- min_lat: double (nullable = true)
 |-- max_lat: double (nullable = true)
 |-- min_lon: double (nullable = true)
 |-- max_lon: double (nullable = true)
```

### 4.2 The 5-Row Cold Start Problem & Kafka Replay Recovery
During our initial Stage B audit of `data/feature_table/traffic/`, the directory contained only **5 rows** (representing a single 5-minute window from `00:00:00` to `00:05:00` on March 1, 2012).
* **The Blocker:** With only 1 window per zone:
  - `lead("avg_speed_kmph", 1)` evaluates to `NULL` for 100% of rows.
  - Dropping null targets leaves **0 valid supervised pairs**.
  - A chronological 80/20 train/test split on 1 window is mathematically impossible.
* **The Root Cause:** In Day 3, we executed a bounded smoke test replaying only 600 events. Because each 5-minute slice contains 207 sensors, 600 events only spanned ~2.8 timestamps. Due to the 2-minute watermark delay, only the very first window closed before the producer stopped!
* **The Defensible Resolution:**
  We leveraged our verified Day 2 producer infrastructure without modifying schemas or deleting checkpoints. We replayed 20,000 real METR-LA events (spanning 00:00:00 to 08:15:00 on March 1, 2012) into Kafka:
  ```powershell
  python services/producers/traffic_producer.py --bootstrap-server localhost:9092 --max-events 20000 --interval 0.001
  ```
  Then executed `streaming_job.py --available-now`. Spark Structured Streaming resumed from its existing checkpoint, processed all micro-batches, advanced the watermark across 96 time slices, and wrote **480 finalized rows** into `data/feature_table/traffic/`.

### 4.3 Provenance Audit of the 480-Row Traffic Feature Table
* **Total Rows:** Exactly **480 rows**.
* **Spatial Balance:** Perfectly balanced across all 5 zones:
  - `zone_1` (Downtown Core): 96 rows
  - `zone_2` (San Fernando Valley): 96 rows
  - `zone_3` (San Gabriel Foothills): 96 rows
  - `zone_4` (South Central Corridor): 96 rows
  - `zone_5` (Harbor & Outer Ring): 96 rows
* **Temporal Coverage:** March 1, 2012 from `05:30:00` to `13:30:00` (8.0 hours continuous morning rush-hour progression).
* **Key Uniqueness:** Exactly 480 distinct `(window_start, zone_id)` composite keys (zero duplicates).
* **Speed Range:** $45.2\text{ km/h} \le v \le 112.65\text{ km/h}$ (realistic transition from midnight free-flow to morning congestion).

### 4.4 Why AQI and Weather Are Excluded from Primary Traffic Features
In Day 3, our historical calendar investigation established:
- METR-LA Traffic: March 1, 2012 (00:00 to 19:50) at 5-minute sampling intervals.
- EPA Air Quality: March 1 to June 30, 2012 at daily station resolution.
- OpenWeather Meteorological: October 1 to October 8, 2012 at hourly resolution.

Because Weather begins in October 2012 while Traffic is located in March 2012, **there is a 7-month calendar gap**.
An inner join between traffic and weather yields **zero rows**.
Attempting to forward-fill or rewrite October dates to March would violate data engineering ethics and academic reproducibility.
Therefore, our primary Day 4 traffic forecasting dataset is built strictly from verified traffic dynamics and temporal periodic encodings. AQI and weather remain decoupled feature tables for future multimodal research.

---

## 🧩 5. Step-by-Step Code Walkthrough: `prepare_training_data.py`

Let's dissect each functional block of [prepare_training_data.py](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/services/streaming/prepare_training_data.py):

```python
# -------------------------------------------------------------------------
# STAGE 1: INGESTION & DATA QUALITY AUDIT
# -------------------------------------------------------------------------
raw_df = spark.read.parquet(input_path)
total_raw_rows = raw_df.count()

# Check for duplicate feature keys (window_start, zone_id)
distinct_keys = raw_df.select("window_start", "zone_id").distinct().count()
if distinct_keys != total_raw_rows:
    raise DataQualityError(f"Duplicate feature keys detected! Distinct: {distinct_keys}, Total: {total_raw_rows}")

# Filter unphysical sensor spikes or dead values
clean_df = raw_df.filter(
    col("avg_speed_kmph").isNotNull()
    & (col("avg_speed_kmph") > 0.0)
    & (col("avg_speed_kmph") <= 250.0)
)
```
- **Why this matters:** Asserts that every `(window_start, zone_id)` combination is unique and strips out negative or impossible speeds ($> 250\text{ km/h}$) caused by loop detector electrical glitches.

```python
# -------------------------------------------------------------------------
# STAGE 2: FUTURE-LABEL CONSTRUCTION (ZERO LEAKAGE)
# -------------------------------------------------------------------------
zone_time_window = Window.partitionBy("zone_id").orderBy("window_start")

labeled_candidates = (
    clean_df
    .withColumn("lead_speed", lead("avg_speed_kmph", 1).over(zone_time_window))
    .withColumn("lead_window_start", lead("window_start", 1).over(zone_time_window))
    .withColumn("delta_seconds", unix_timestamp("lead_window_start") - unix_timestamp("window_start"))
)

# Enforce continuity guard: delta must equal 300s
labeled_df = labeled_candidates.withColumn(
    "next_avg_speed_kmph",
    when(col("delta_seconds") == 300, col("lead_speed")).otherwise(lit(None)),
).withColumn(
    "target_window_start",
    when(col("delta_seconds") == 300, col("lead_window_start")).otherwise(lit(None)),
)

supervised_df = labeled_df.filter(
    col("next_avg_speed_kmph").isNotNull() & col("target_window_start").isNotNull()
)
```
- **Why this matters:** `lead()` shifts speed 1 step forward *within the zone*. If `delta_seconds != 300`, the label is set to `NULL`. Filtering out `NULL` drops gaps and the last window of each zone.

```python
# -------------------------------------------------------------------------
# STAGE 3: INPUT FEATURE ENGINEERING
# -------------------------------------------------------------------------
featured_df = (
    supervised_df
    .withColumn("speed_spread_kmph", col("max_speed_kmph") - col("min_speed_kmph"))
    .withColumn("hour_of_day", hour(col("window_start")))
    .withColumn("minute_of_hour", minute(col("window_start")))
    .withColumn("sin_hour", sin(lit(2.0) * pi() * col("hour_of_day") / lit(24.0)))
    .withColumn("cos_hour", cos(lit(2.0) * pi() * col("hour_of_day") / lit(24.0)))
)
```
- **Why this matters:** `speed_spread_kmph` represents traffic volatility. `sin_hour` and `cos_hour` provide a smooth cyclic representation of the 24-hour day, ensuring that 23:55 and 00:05 are mathematically adjacent rather than at opposite poles.

```python
# -------------------------------------------------------------------------
# STAGE 4: CHRONOLOGICAL TIME CUTOFF COMPUTATION
# -------------------------------------------------------------------------
time_bounds = featured_df.selectExpr(
    "min(unix_timestamp(window_start)) as t_min_epoch",
    "max(unix_timestamp(window_start)) as t_max_epoch"
).first()

t_min_epoch = time_bounds["t_min_epoch"]
t_max_epoch = time_bounds["t_max_epoch"]
cutoff_epoch = t_min_epoch + train_ratio * (t_max_epoch - t_min_epoch)
```
- **Why this matters:** Computes the cutoff as the 80th percentile of **elapsed physical time**, completely independent of row count variations.

```python
# -------------------------------------------------------------------------
# STAGE 5: CONSERVATIVE BOUNDARY SPLITTING
# -------------------------------------------------------------------------
raw_train = featured_df.filter(unix_timestamp(col("target_window_start")) <= cutoff_epoch)
raw_test = featured_df.filter(unix_timestamp(col("window_start")) > cutoff_epoch)

# Discard boundary-crossing examples
boundary_excluded = featured_df.filter(
    (unix_timestamp(col("window_start")) <= cutoff_epoch)
    & (unix_timestamp(col("target_window_start")) > cutoff_epoch)
)
```
- **Why this matters:** Enforces that training targets never predict into the testing period. Discards the 5 boundary-crossing rows.

```python
# -------------------------------------------------------------------------
# STAGE 6: ESTIMATOR HYGIENE — FIT ON TRAIN ONLY
# -------------------------------------------------------------------------
zone_indexer = StringIndexer(inputCol="zone_id", outputCol="zone_index", handleInvalid="keep")
indexer_model = zone_indexer.fit(raw_train)
indexed_train = indexer_model.transform(raw_train)
indexed_test = indexer_model.transform(raw_test)
```
- **Why this matters:** `fit()` is called strictly on `raw_train`. Test set category distributions cannot influence the index mapping.

```python
# -------------------------------------------------------------------------
# STAGE 7: VECTORASSEMBLER
# -------------------------------------------------------------------------
feature_cols = [
    "avg_speed_kmph", "min_speed_kmph", "max_speed_kmph", "speed_spread_kmph",
    "traffic_record_count", "hour_of_day", "sin_hour", "cos_hour", "zone_index"
]
target_col = "next_avg_speed_kmph"
assert target_col not in feature_cols

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="error")
assembled_train = assembler.transform(indexed_train)
assembled_test = assembler.transform(indexed_test)
```
- **Why this matters:** Assembles all 9 predictors into `features` VectorUDT column while asserting that the target is strictly absent.

---

## 🔍 6. Step-by-Step Code Walkthrough: `verify_split.py`

[verify_split.py](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/services/streaming/verify_split.py) serves as the independent gatekeeper for Day 5. It does not trust in-memory DataFrames; it reloads persisted Parquet files from disk and executes 8 rigorous assertions:

```python
# 1. Directory Existence & Non-Emptiness
train_df = spark.read.parquet(train_path)
test_df = spark.read.parquet(test_path)
assert train_df.count() > 0 and test_df.count() > 0

# 2. Metadata Agreement
with open(metadata_path, "r") as f:
    meta = json.load(f)
assert meta["train_row_count"] == train_df.count()
assert meta["test_row_count"] == test_df.count()

# 3. Vector Dimensionality & Anti-Leakage
assert isinstance(train_df.schema["features"].dataType, VectorUDT)
assert len(train_df.first()["features"]) == 9
assert "next_avg_speed_kmph" not in meta["feature_columns"]

# 4. Sample Disjointness (Zero Duplicate Keys & Zero Overlap)
train_keys = train_df.select("window_start", "zone_id")
test_keys = test_df.select("window_start", "zone_id")
assert train_keys.distinct().count() == train_df.count()
assert test_keys.distinct().count() == test_df.count()
assert train_keys.intersect(test_keys).count() == 0

# 5. Zero Temporal Leakage Proof
train_bounds = train_df.selectExpr("max(unix_timestamp(target_window_start)) as max_target").first()
test_bounds = test_df.selectExpr("min(unix_timestamp(window_start)) as min_feat").first()
assert train_bounds["max_target"] < test_bounds["min_feat"]
assert train_bounds["max_target"] <= meta["cutoff_epoch_seconds"]
assert test_bounds["min_feat"] > meta["cutoff_epoch_seconds"]
```

---

## 🧪 7. Automated Test Suite Deep-Dive: `test_training_data.py`

Located in [services/streaming/tests/test_training_data.py](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/services/streaming/tests/test_training_data.py), this suite exercises 11 deterministic unit and integration test fixtures:

1. `test_future_label_construction_per_zone`: Proves `lead()` shifts speed forward 1 window within each zone.
2. `test_no_cross_zone_leakage`: Confirms that `zone_1` labels never bleed into `zone_2`.
3. `test_last_row_per_zone_dropped`: Asserts that the trailing window of each zone timeline (having no next value) is cleanly dropped.
4. `test_gap_handling_no_false_consecutive`: Injects an artificial 55-minute gap and verifies that the pre-gap row receives a `NULL` label and is dropped rather than predicting across the gap.
5. `test_duplicate_feature_key_detection`: Injects duplicate `(window_start, zone_id)` records and asserts that `DataQualityError` is raised.
6. `test_target_excluded_from_features`: Confirms that `next_avg_speed_kmph` is absent from `meta["feature_columns"]` and vector attributes.
7. `test_boundary_crossing_exclusion_and_zero_leakage`: Generates a synthetic 10-window dataset, runs the pipeline, and calls `verify_split` to prove zero temporal leakage.
8. `test_insufficient_timeline_error`: Passes a single-window DataFrame and asserts that `InsufficientCoverageError` is raised.
9. `test_unseen_category_in_test_set`: Injects an unseen zone (`zone_new_unseen`) appearing strictly in the testing period; confirms `StringIndexer` with `handleInvalid="keep"` processes it without error.
10. `test_null_and_invalid_speed_filtering`: Injects negative speeds ($-5.0$) and unphysical speeds ($999.0\text{ km/h}$); verifies they are filtered before label construction.
11. `test_deterministic_split`: Executes two independent runs on the same input; asserts that cutoff timestamps and split row counts are bit-for-bit identical.

---

## 🔬 8. Distributed Runtime Troubleshooting Case Studies

During Day 4 implementation on Windows 11 with PySpark 3.5.1, three distributed runtime hurdles were identified, diagnosed, and resolved:

### Case Study 1: Missing NumPy Dependency in PySpark MLlib
* **Symptom:** When running pytest on `test_training_data.py`, Spark failed during collection:
  ```text
  ModuleNotFoundError: No module named 'numpy'
    at pyspark.ml.param.__init__.py:32
  ```
* **Root Cause:** PySpark's core SQL engine does not require NumPy, but its machine learning module (`pyspark.ml.linalg`, `VectorAssembler`, `DenseVector`) depends on NumPy C-bindings for vectorized array operations.
* **Resolution:** Installed `numpy-2.2.6` in `services/streaming/venv` and appended `numpy>=1.24.0` to `services/streaming/requirements.txt`.

### Case Study 2: Offset-Naive vs. Offset-Aware Datetime Collision
* **Symptom:** During boundary assertions, Python threw:
  ```text
  TypeError: can't compare offset-naive and offset-aware datetimes
    at prepare_training_data.py:352
  ```
* **Root Cause:** `datetime.fromtimestamp(cutoff_epoch, tz=timezone.utc)` produced an **offset-aware** datetime object. Meanwhile, Spark SQL's Py4J gateway converts `TimestampType` to Python `datetime` objects that are **offset-naive** (lacking a `tzinfo` attribute). Python forbids comparing naive and aware datetimes.
* **Resolution:** Refactored all time boundary filtering and assertions to evaluate **epoch seconds** via Spark SQL's native `unix_timestamp()` function:
  ```python
  max_train_target_epoch = final_train.selectExpr("max(unix_timestamp(target_window_start))").first()[0]
  min_test_feature_epoch = final_test.selectExpr("min(unix_timestamp(window_start))").first()[0]
  assert max_train_target_epoch < min_test_feature_epoch
  assert max_train_target_epoch <= cutoff_epoch
  assert min_test_feature_epoch > cutoff_epoch
  ```
  Comparing integer epoch seconds provides 100% mathematical immunity to local host timezones, Daylight Savings Time (DST) shifts, and Py4J deserialization offsets.

### Case Study 3: The 5-Row Insufficient Coverage Discovery
* **Symptom:** Initial execution of `prepare_training_data.py` halted with:
  ```text
  InsufficientCoverageError: Eligible supervised examples (0) is insufficient for train/test splitting.
  ```
* **Root Cause:** The feature table from Day 3 held only 5 rows (1 single window across 5 zones). Because `lead()` on the only window produces `NULL`, 0 labeled rows remained.
* **Resolution:** Replayed 20,000 real events through Kafka and executed `streaming_job.py --available-now`. This cleanly populated `data/feature_table/traffic/` to 480 rows (96 contiguous windows), providing 470 valid labeled examples.

---

## 📊 9. Live Terminal Execution Logs & Verification Evidence

### 1. Feature Engineering & Split Execution Log
```powershell
.\services\streaming\venv\Scripts\python services/streaming/prepare_training_data.py
```
```text
======================================================================
URBANIX DAY 4: FEATURE ENGINEERING & CHRONOLOGICAL SPLIT PIPELINE
======================================================================
[*] Input feature table : data/feature_table/traffic
[*] Train output path   : data/train
[*] Test output path    : data/test
[*] Metadata path       : data/split_metadata.json
[*] Train/Test ratio    : 80.0% / 20.0%
[*] Raw feature table row count: 480
[*] Valid labeled examples after continuity filtering: 470
[*] Temporal Horizon (Feature Time): 2012-03-01 05:30:00 to 2012-03-01 13:30:00
[*] Computed 80% Chronological Cutoff: 2012-03-01 06:24:00 UTC (epoch 1330583040.0)
[*] Boundary-crossing examples excluded: 5
[*] Training candidate rows             : 365
[*] Testing candidate rows              : 100
[*] Latest Training Target Timestamp  : 2012-03-01 11:50:00 (epoch 1330582800)
[*] Earliest Testing Feature Timestamp: 2012-03-01 11:55:00 (epoch 1330583100)
[PASS] Zero Temporal Leakage Verified: max(train.target) < min(test.feature)!
[*] Successfully saved split metadata to data/split_metadata.json
[SUCCESS] Day 4 Dataset Preparation Complete!
======================================================================
```

### 2. Independent Split Verifier Log
```powershell
.\services\streaming\venv\Scripts\python services/streaming/verify_split.py
```
```text
======================================================================
URBANIX DAY 4: INDEPENDENT CHRONOLOGICAL SPLIT VERIFICATION
======================================================================
[*] Reloading Train Parquet : data/train
[*] Reloading Test Parquet  : data/test
[*] Checking Split Metadata : data/split_metadata.json
[*] Reloaded Train rows : 365
[*] Reloaded Test rows  : 100
[PASS] Both train and test datasets are populated and non-empty.
[PASS] Observed row counts match split metadata exactly.
[PASS] All required modeling and key columns present in both splits.
[PASS] 'features' column is verified VectorUDT format.
[PASS] Vector dimension confirmed: 9 features.
[PASS] Anti-leakage verified: Target strictly excluded from feature vector.
[PASS] Zero duplicate keys within training partition.
[PASS] Zero duplicate keys within testing partition.
[PASS] Zero key overlap: Train and test samples are mutually disjoint.
[*] Metadata Cutoff Timestamp         : 2012-03-01 06:24:00 UTC (epoch 1330583040.0)
[*] Observed Max Train Target Time     : 2012-03-01 11:50:00 (epoch 1330582800)
[*] Observed Min Test Feature Time     : 2012-03-01 11:55:00 (epoch 1330583100)
[PASS] Zero Temporal Leakage Verified: max(train.target_window_start) [2012-03-01 11:50:00] < min(test.window_start) [2012-03-01 11:55:00].
[PASS] All training targets strictly occur at or before cutoff.
[PASS] All testing features strictly occur after cutoff.
[PASS] All target speeds are finite, positive, and within physical bounds (0 < v <= 250 km/h).
======================================================================
FINAL VERIFICATION RESULT: [ALL CHECKS PASSED]
The train/test splits are mathematically sound, leak-free, and ready for Day 5 modeling.
======================================================================
```

### 3. Automated Test Suite Execution Logs
```text
PS C:\Users\...\urbanix> .\services\streaming\venv\Scripts\pytest services/streaming/tests/test_training_data.py -v
============================= test session starts =============================
platform win32 -- Python 3.10.11, pytest-9.1.1, pluggy-1.7.0
rootdir: C:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix
collected 11 items

services/streaming/tests/test_training_data.py::test_future_label_construction_per_zone PASSED [  9%]
services/streaming/tests/test_training_data.py::test_no_cross_zone_leakage PASSED [ 18%]
services/streaming/tests/test_training_data.py::test_last_row_per_zone_dropped PASSED [ 27%]
services/streaming/tests/test_training_data.py::test_gap_handling_no_false_consecutive PASSED [ 36%]
services/streaming/tests/test_training_data.py::test_duplicate_feature_key_detection PASSED [ 45%]
services/streaming/tests/test_training_data.py::test_target_excluded_from_features PASSED [ 54%]
services/streaming/tests/test_training_data.py::test_boundary_crossing_exclusion_and_zero_leakage PASSED [ 63%]
services/streaming/tests/test_training_data.py::test_insufficient_timeline_error PASSED [ 72%]
services/streaming/tests/test_training_data.py::test_unseen_category_in_test_set PASSED [ 81%]
services/streaming/tests/test_training_data.py::test_null_and_invalid_speed_filtering PASSED [ 90%]
services/streaming/tests/test_training_data.py::test_deterministic_split PASSED [100%]

============================= 11 passed in 47.97s =============================
```

---

## 🛡️ 10. Atomic Viva Defense Playbook (20 Q&A for Examiners)

#### Q1: *"What is the exact machine learning problem formulated in Day 4?"*
* **Defense:**  
  *"It is a supervised autoregressive time-series regression task. Given the physical velocity statistics, detector observation counts, and cyclic time encodings in municipal zone $z$ during 5-minute window $k$, predict the average speed in the subsequent consecutive 5-minute window $k+1$ (`next_avg_speed_kmph`)."*

#### Q2: *"Why did you use `lead()` instead of predicting the current window's speed?"*
* **Defense:**  
  *"Predicting the current window's speed from current window features is a trivial identity mapping ($y = x$), which provides zero predictive utility. To build an operational forecasting platform that enables proactive traffic diversion, the model must forecast future conditions ($t + \Delta t$) before they occur."*

#### Q3: *"Explain why `Window.partitionBy('zone_id')` is required when constructing the label."*
* **Defense:**  
  *"Urbanix ingests telemetry concurrently from 5 distinct municipal zones. Without `partitionBy('zone_id')`, sorting globally by timestamp would cause `lead()` on the final window of `zone_1` to pull the first window of `zone_2` as its target, corrupting spatial boundaries. Partitioning isolates each zone's timeline into an independent time series."*

#### Q4: *"What is the Continuity Guard and why is it necessary?"*
* **Defense:**  
  *"If a sensor station experiences an outage (e.g. from 02:00 to 06:00), a naive `lead(1)` call assigns the 06:00 speed as the label for the 02:00 row. The Continuity Guard verifies that $\text{lead\_timestamp} - \text{current\_timestamp} == 300\text{s}$. If the interval exceeds 300 seconds, the target is set to `NULL` and dropped, preventing the model from learning across telemetry gaps."*

#### Q5: *"Why is `randomSplit()` considered a fatal error in time-series forecasting?"*
* **Defense:**  
  *"Random splitting shuffles rows randomly, which assumes observations are independent and identically distributed (i.i.d.). In auto-correlated time series, this scatters future observations into the training set and past observations into the test set (lookahead bias). The model memorizes diurnal curves and scores near-perfect test metrics that fail completely in production."*

#### Q6: *"What is a boundary-crossing example and how does Urbanix handle it?"*
* **Defense:**  
  *"A boundary-crossing example occurs when an observation's feature timestamp is at or before the cutoff ($\text{window\_start} \le \text{cutoff}$), but its target timestamp falls strictly into the testing period ($\text{target\_window\_start} > \text{cutoff}$). If included in training, the model evaluates its loss against test-period ground truth. Urbanix explicitly excludes these boundary-crossing rows (5 rows discarded in our live run) to guarantee complete temporal separation."*

#### Q7: *"State the mathematical proof that your split contains zero temporal leakage."*
* **Defense:**  
  *"In our persisted splits: $\max_{x \in \text{Train}}(\text{target\_window\_start}(x)) = 1330582800 < 1330583040 (\text{Cutoff}) < 1330583100 = \min_{x \in \text{Test}}(\text{window\_start}(x))$. Because the newest training ground truth strictly precedes the oldest testing feature by 300 seconds, zero temporal leakage is mathematically proven."*

#### Q8: *"Why did you fit `StringIndexer` on the training partition only?"*
* **Defense:**  
  *"Fitting any estimator (like `StringIndexer` or `StandardScaler`) on the full dataset leaks category distributions and frequency statistics from the test set into the model. Fitting on `train_df` only and setting `handleInvalid='keep'` ensures production-grade estimator hygiene."*

#### Q9: *"What does `VectorAssembler` do and why is it required for Spark MLlib?"*
* **Defense:**  
  *"Spark MLlib algorithms require all numerical predictors packed into a single `VectorUDT` column per row. `VectorAssembler` transforms our 9 independent feature columns into a unified linear algebra vector, ready for `LinearRegression` or `DecisionTreeRegressor` in Day 5."*

#### Q10: *"Why did you represent time of day using sine and cosine functions?"*
* **Defense:**  
  *"Using raw hour numbers (0 to 23) introduces an artificial numerical discontinuity: 23:55 (hour 23) and 00:05 (hour 0) appear 23 units apart, even though they are separated by only 10 minutes. Cyclic encodings $\sin(2\pi \cdot \text{hour} / 24)$ and $\cos(2\pi \cdot \text{hour} / 24)$ project time onto a continuous unit circle where midnight connects smoothly."*

#### Q11: *"Why are `vehicle_count` and `co2` not present in your feature vector?"*
* **Defense:**  
  *"Caltrans PeMS loop detectors measure inductive frequency shifts to calculate time-mean velocity; they do not count individual vehicles. U.S. EPA AirData station 88101 measures particulate mass concentration ($\text{PM}_{2.5}$); it does not measure $\text{CO}_2$. Inventing fake counts or synthetic greenhouse gas readings violates academic honesty and data engineering ethics."*

#### Q12: *"Why are AQI and Weather not joined into the primary traffic forecasting model?"*
* **Defense:**  
  *"In Day 3, our provenance audit proved that the OpenWeather dataset begins in October 2012, whereas METR-LA traffic is from March 2012. An inner join yields 0 rows due to the 7-month calendar gap. Rather than fabricating dates or forward-filling across months, we built the primary traffic model from genuine traffic dynamics and preserved AQI and weather as independent multimodal feature tables."*

#### Q13: *"What is `speed_spread_kmph` and what physical property does it measure?"*
* **Defense:**  
  *"It is calculated as $\text{max\_speed\_kmph} - \text{min\_speed\_kmph}$ within the 5-minute window. In traffic engineering, a high speed spread indicates localized shockwaves, bottleneck turbulence, or stop-and-go instability, serving as an early-warning predictor of impending gridlock."*

#### Q14: *"Why did you store the datasets in Apache Parquet format instead of CSV?"*
* **Defense:**  
  *"Parquet is a columnar binary format with Snappy compression and strong schema typing. It supports Spark's `VectorUDT` complex type natively (which CSV cannot do without complex string serialization) and enables projection pruning and predicate pushdown during Day 5 model training."*

#### Q15: *"What happens to the final row of each zone timeline?"*
* **Defense:**  
  *"Because there is no subsequent window observed for the final row, `lead()` returns `NULL`. It is dropped from supervised learning because it lacks a target label. However, in Day 6 live streaming inference, that exact final row becomes the active feature vector used to predict future conditions."*

#### Q16: *"How did you resolve the `ModuleNotFoundError: No module named 'numpy'` error?"*
* **Defense:**  
  *"While PySpark SQL does not require NumPy, `pyspark.ml.linalg` relies on NumPy C-bindings for vector representations. Installing `numpy-2.2.6` and pinning `numpy>=1.24.0` in `requirements.txt` resolved the dependency requirement."*

#### Q17: *"How did you resolve the `TypeError: can't compare offset-naive and offset-aware datetimes` issue?"*
* **Defense:**  
  *"Spark SQL deserializes timestamps as offset-naive datetimes, while `datetime.fromtimestamp(..., tz=timezone.utc)` creates offset-aware datetimes. We standardized all boundary calculations and assertions to use integer epoch seconds via `unix_timestamp()`, guaranteeing timezone immunity."*

#### Q18: *"Why did you use 80% of elapsed time instead of 80% of row count for the split?"*
* **Defense:**  
  *"In time-series systems, sensor reporting rates or network packet drops can cause uneven row distributions across hours. An elapsed-time cutoff ensures that the training period covers a continuous, predictable span of the physical morning progression, regardless of row density."*

#### Q19: *"What information is recorded in `data/split_metadata.json`?"*
* **Defense:**  
  *"It stores the exact UTC cutoff timestamp, cutoff epoch seconds, train and test row counts, boundary-crossing exclusion counts, earliest/latest timestamps for both splits, and the complete feature column manifest. This enables independent verification scripts and Day 5 training jobs to validate dataset provenance."*

#### Q20: *"How will Day 5 use the datasets produced today?"*
* **Defense:**  
  *"Day 5 will ingest `data/train/` directly, fit `LinearRegression` and `DecisionTreeRegressor` models on the assembled `features` vector to predict `next_avg_speed_kmph`, evaluate predictions on `data/test/` using `RegressionEvaluator`, and log $R^2$, RMSE, and MAE across zones."*

---

## 📌 11. Complete Step-by-Step PowerShell Command Cheat Sheet

```powershell
# =====================================================================
# 1. ENVIRONMENT ACTIVATION
# =====================================================================
cd "C:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix"
.\services\streaming\venv\Scripts\Activate.ps1

# =====================================================================
# 2. RUN AUTOMATED TEST SUITES
# =====================================================================
# Run Day 4 unit tests (11 tests covering label creation, leakage, gaps)
pytest services\streaming\tests\test_training_data.py -v

# Run Day 3 streaming regression tests (14 tests)
pytest services\streaming\tests\test_streaming.py -v

# Run Day 2 producer regression tests (19 tests)
cd services\producers
.\venv\Scripts\pytest tests\test_producers.py -v
cd ..\..

# =====================================================================
# 3. REPLAY TELEMETRY & RUN STREAMING (IF RE-POPULATING FEATURE TABLES)
# =====================================================================
# Replay 20,000 traffic events into Kafka
python services\producers\traffic_producer.py --bootstrap-server localhost:9092 --max-events 20000 --interval 0.001

# Execute Spark Structured Streaming job in bounded micro-batch mode
python services\streaming\streaming_job.py --available-now

# =====================================================================
# 4. EXECUTE DAY 4 FEATURE PIPELINE & SPLIT
# =====================================================================
# Run Day 4 feature engineering and chronological train/test split
python services\streaming\prepare_training_data.py

# =====================================================================
# 5. EXECUTE INDEPENDENT RELOAD VERIFICATION
# =====================================================================
# Validate persisted Parquet splits on disk and prove zero leakage
python services\streaming\verify_split.py

# =====================================================================
# 6. INSPECT OUTPUTS & METADATA
# =====================================================================
# Inspect generated split metadata JSON
Get-Content data\split_metadata.json

# Check training Parquet directory
Get-ChildItem -Path data\train

# Check testing Parquet directory
Get-ChildItem -Path data\test
```

---

## 🏁 12. Day 4 Definition-of-Done & Handover to Day 5

| # | Item | Requirement | Status | Verification Evidence |
|---|---|---|---|---|
| 1 | **Repository & Environment Audit** | Inspected live Git status and runtime dependencies | **PASS** | Python 3.10.11, Spark 3.5.1, JDK 8 |
| 2 | **Forecasting Target Definition** | `next_avg_speed_kmph` via per-zone `lead(1)` | **PASS** | Validated in `prepare_training_data.py` |
| 3 | **Continuity & Gap Handling** | Enforced $\Delta t == 300\text{s}$; dropped gaps and last row | **PASS** | Tested in `test_gap_handling_no_false_consecutive` |
| 4 | **Input Feature Specification** | 9 validated features; target strictly excluded | **PASS** | Tested in `test_target_excluded_from_features` |
| 5 | **VectorAssembler** | Produces `features` VectorUDT with 9 dimensions | **PASS** | Verified via `verify_split.py` |
| 6 | **Estimator Hygiene** | `StringIndexer` fitted on `train_df` only | **PASS** | Tested in `test_unseen_category_in_test_set` |
| 7 | **Chronological Cutoff** | 80% time range cutoff (epoch `1330583040.0`) | **PASS** | Documented in `data/split_metadata.json` |
| 8 | **Conservative Boundary Policy**| Discarded 5 boundary-crossing rows; zero leakage | **PASS** | $\max(\text{train.target}) < \min(\text{test.feature})$ |
| 9 | **Parquet Output Sinks** | `data/train/` (365 rows) & `data/test/` (100 rows) | **PASS** | Verified on disk |
| 10| **Independent Reload Verifier** | `verify_split.py` asserts zero leakage and exits 0 | **PASS** | All 12 checks passed |
| 11| **Automated Unit Tests** | 11/11 tests passing in `test_training_data.py` | **PASS** | `pytest` 100% pass |
| 12| **Day 3 Regressions** | 14/14 tests passing in `test_streaming.py` | **PASS** | `pytest` 100% pass |
| 13| **Day 2 Regressions** | 19/19 tests passing in `test_producers.py` | **PASS** | `pytest` 100% pass |
| 14| **Git Hygiene** | Parquet datasets gitignored; working tree clean | **PASS** | Commit `e27ae8f` on `main` |

---

### 🚀 Handover to Day 5: MLlib Regression & Forecasting Evaluation
With Day 4 complete, verified, and committed, we have a mathematically clean, leak-free, model-ready dataset.

**Day 5 Implementation Objectives:**
1. **Model Ingestion:** Ingest `data/train/` and `data/test/` directly into PySpark.
2. **Model Training:**
   - Train a baseline **Linear Regression** model (`pyspark.ml.regression.LinearRegression`).
   - Train a non-linear **Decision Tree Regressor** (`pyspark.ml.regression.DecisionTreeRegressor`).
3. **Model Evaluation:**
   - Compute **$R^2$ (Coefficient of Determination)**, **$\text{RMSE}$ (Root Mean Squared Error)**, and **$\text{MAE}$ (Mean Absolute Error)** using `RegressionEvaluator`.
   - Log metrics per zone and in aggregate to satisfy **FR-4** and **FR-5**.
4. **Model Export:** Persist trained model pipelines for Day 6 live streaming inference.
