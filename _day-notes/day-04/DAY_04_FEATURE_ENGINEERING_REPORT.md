# 🤖 Urbanix — Day 4 Feature Engineering & Chronological Train/Test Split Deep-Dive
> **Transforming 5-Minute Windowed Stream Telemetry into Model-Ready, Leakage-Free Parquet Datasets for Spark MLlib Forecasting**

> [!WARNING]
> **DEVELOPMENT NOTEBOOK, PEDAGOGICAL COMPENDIUM & VIVA DEFENSE GUIDE**
> This document is the definitive, atomic, and exhaustive source of truth for **Day 4** of the Urbanix platform. It serves two core functions:
> 1. **Beginner-Friendly Pedagogical Manual:** Explaining every machine learning and time-series engineering concept from absolute first principles (feature vs. target, temporal leakage, boundary-crossing exclusion, estimator hygiene, VectorAssembler).
> 2. **Academic & Industrial Viva Defense Playbook:** Equipping you with exact formulas, state transitions, leakage proofs, and answers to challenging examiner questions at the atomic level.

---

## 🎯 1. Day 4 Objective & Scope Boundaries

### The Context: From Streaming Sinks to Supervised Datasets
In **Weekend 1 (Days 1–3)**, we established the infrastructure and stream processing pipeline:
- **Day 1:** Local Kafka broker, Zookeeper, and Spark standalone cluster in Docker.
- **Day 2:** Sensor simulation layer replaying historical METR-LA loop detectors, EPA air monitoring stations, and OpenWeather meteorological observations.
- **Day 3:** Spark Structured Streaming engine aggregating raw telemetry into 5-minute tumbling windows with a 2-minute event-time watermark, static zone metadata enrichment, and fault-tolerant Parquet sinks (`data/feature_table/{traffic,aqi,weather}`).

**Day 4 Objective:** Build the **Feature Engineering and Chronological Train/Test Split Pipeline**. Machine learning algorithms cannot learn directly from raw streaming tables. They require:
1. A clearly defined supervised learning target ($y$) representing the *future state* of the system.
2. A single numeric feature vector ($\vec{x}$) per observation, bundled into the format expected by Apache Spark MLlib.
3. A strictly chronological (non-random, time-ordered) train/test split that eliminates all forms of data leakage.

```
+-------------------------------------------------------------------------------------------------------------------------+
|                                                URBANIX DAY 4 PIPELINE                                                   |
|                                                                                                                         |
|  [ data/feature_table/traffic ]                                                                                         |
|               │                                                                                                         |
|               ▼                                                                                                         |
|  1. Integrity Audit & Continuity Check (delta_seconds == 300) ────────────────────────────────────────────────────────┐ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  2. Target Construction: lead("avg_speed_kmph", 1).over(partitionBy("zone_id").orderBy("window_start"))               │ |
|     --> Target: 'next_avg_speed_kmph', Target Time: 'target_window_start'                                             │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  3. Feature Engineering:                                                                                              │ |
|     - Speed Metrics: avg_speed_kmph, min_speed_kmph, max_speed_kmph, speed_spread_kmph                                │ |
|     - Sensor Counts: traffic_record_count                                                                             │ |
|     - Temporal Encodings: hour_of_day, sin_hour, cos_hour (cyclic)                                                    │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  4. Chronological Cutoff (80% Elapsed Time Range) ──> Cutoff Epoch: 1330583040 (2012-03-01 06:24:00 UTC)              │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  5. Conservative Boundary Splitting:                                                                                  │ |
|     - Train: target_window_start <= Cutoff                                                                            │ |
|     - Test:  window_start > Cutoff                                                                                    │ |
|     - Excluded: window_start <= Cutoff AND target_window_start > Cutoff (Boundary-crossing examples)                   │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  6. Estimator Hygiene: StringIndexer fit on TRAIN ONLY ──> transform(Train) & transform(Test)                         │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  7. VectorAssembler: Bundle 9 validated input features into 'features' Vector (Target Strictly Excluded!)             │ |
|               │                                                                                                       │ |
|               ▼                                                                                                       │ |
|  8. Parquet Sinks: data/train/ (365 rows) and data/test/ (100 rows) + data/split_metadata.json                        │ |
+-------------------------------------------------------------------------------------------------------------------------+
```

### 🚫 Day 4 Scope Boundaries (What Was Deliberately Excluded):
- **NO Model Training or Fitting (Day 5 Scope):** Linear Regression, Decision Tree Regressor, Gradient-Boosted Trees, and neural networks are strictly deferred to Day 5.
- **NO Metric Calculation ($R^2$, RMSE, MAE):** Evaluation metrics belong to Day 5 after models are trained.
- **NO Live Streaming Inference (Day 6 Scope):** Streaming model deployment occurs in Day 6.
- **NO Synthetic Observation Injection:** We do not manufacture synthetic rows to balance classes or fabricate vehicle counts/CO₂.

---

## 🛠️ 2. Technologies & Environment Specifications

| Component | Version | Role in Day 4 Pipeline |
| :--- | :--- | :--- |
| **Python Runtime** | `3.10.11` (64-bit) | Host runtime executing pipeline, verification, and tests in `services/streaming/venv`. |
| **Apache Spark** | `3.5.1` (Host: `3.5.9`) | Distributed execution engine performing window operations, indexing, and vector assembly. |
| **PySpark MLlib** | `3.5.1` | Machine learning library providing `VectorAssembler` and `StringIndexer`. |
| **NumPy** | `2.2.6` | Numerical backend required by PySpark ML linear algebra routines. |
| **Apache Parquet** | Format v1/v2 | Columnar storage format for persisted `data/train/` and `data/test/` datasets. |
| **pytest** | `9.1.1` | Test framework executing 11 deterministic Day 4 unit and integration tests. |
| **Operating System**| Windows 11 Home | Local development host running PowerShell 7 / Windows Terminal. |

---

## 📚 3. Core Theory & Atomic Definitions of ML for Time-Series Streaming

To defend your machine learning data preparation in an academic viva or technical interview at the atomic level, you must understand these foundational concepts from first principles:

### 3.1 Feature vs. Target
In supervised machine learning:
* **Features ($\vec{x} \in \mathbb{R}^d$):** The vector of input measurements available to the model at decision time $t$. Features represent *what is known right now or in the past* (e.g., current speed, volatility, time of day).
* **Target / Label ($y \in \mathbb{R}$):** The ground-truth value the model is tasked with predicting. In forecasting, $y$ represents *what will happen in the future* (e.g., speed at $t + \Delta t$).
* **The Cardinal Rule:** A feature must **never** contain the target, and a model must never have access to future information when making a prediction.

### 3.2 The Forecasting Problem: Next-Window Average Speed
* **Problem Formulation:** Given traffic conditions in municipal zone $z$ during 5-minute window $k$ (spanning $[t_k, t_k + 300\text{s})$), predict the average speed in the *subsequent consecutive 5-minute window* $k+1$ (spanning $[t_k + 300\text{s}, t_k + 600\text{s})$).
* **Target Name:** `next_avg_speed_kmph`
* **Target Timestamp:** `target_window_start` ($= t_k + 300\text{s}$)
* **Measurement Units:** Kilometres per hour ($\text{km/h}$), converted from Caltrans loop detector miles per hour via factor $1.60934$.

### 3.3 Future-Label Construction: Window Specification & `lead()`
How do we attach tomorrow's value to today's row in Apache Spark?
We define a Spark Window specification partitioned by `zone_id` and ordered chronologically by `window_start`:
```python
zone_window = Window.partitionBy("zone_id").orderBy("window_start")
labeled_df = df.withColumn("lead_speed", lead("avg_speed_kmph", 1).over(zone_window))
```
* **Why Partition by Zone?**
  Traffic on the San Fernando Valley freeway (`zone_2`) does not immediately follow traffic in Downtown LA (`zone_1`). Partitioning by `zone_id` ensures that `lead(1)` looks ahead **strictly within the same spatial zone's timeline**. Without `partitionBy("zone_id")`, the last window of `zone_1` would incorrectly take its "next" label from the first window of `zone_2`, causing severe spatial data corruption!
* **The Final Row Rule:**
  The last recorded window in each zone's timeline has no subsequent observation ($k+1$ has not happened yet). Therefore, `lead()` returns `NULL`. These rows cannot be used for supervised training and are dropped.

### 3.4 Telemetry Gap Detection (Continuity Guard)
In real-world IoT systems, sensors go offline for maintenance, network packets drop, or power flickers.
Suppose a loop detector logs a window at `08:00`, drops offline, and logs its next window at `14:00` (a 6-hour gap).
If you blindly execute `lead("avg_speed_kmph", 1)`, the `08:00` row will receive the `14:00` speed as its label!
The model would be trained to predict traffic 6 hours in the future while believing it is predicting 5 minutes in the future!

**The Continuity Formula:**
$$\Delta t = \text{unix\_timestamp}(\text{lead\_window\_start}) - \text{unix\_timestamp}(\text{window\_start})$$
$$\text{next\_avg\_speed\_kmph} = \begin{cases} \text{lead\_speed} & \text{if } \Delta t = 300\text{ seconds} \\ \text{NULL} & \text{otherwise (gap detected)} \end{cases}$$
Any row with $\Delta t \ne 300\text{s}$ is quarantined and dropped from supervised learning.

### 3.5 What is VectorAssembler?
Most machine learning algorithms in Apache Spark MLlib (e.g. `LinearRegression`, `DecisionTreeRegressor`, `RandomForestRegressor`) do not accept multiple individual DataFrame columns. They require all input features to be bundled into a **single vector column** containing a dense or sparse vector:
```python
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
```
* **Data Science Analogy:** It is the direct equivalent of `numpy.column_stack([f1, f2, f3])` or scikit-learn's `ColumnTransformer`.
* **Fail-Fast Policy:** We configure `handleInvalid="error"`. If an unexpected NaN or NULL enters a feature column, the pipeline halts immediately rather than silently imputing biased numbers.

### 3.6 Estimator Hygiene: Fitting on Training Data Only
Machine learning transformers fall into two categories:
1. **Stateless Transformers:** Apply a deterministic mathematical formula to each row independently without learning any parameters (e.g. `VectorAssembler`, `sin()`, `cos()`).
2. **Stateful Estimators:** Learn parameters from the data before transforming (e.g. `StringIndexer`, `StandardScaler`, `Imputer`).

> [!CAUTION]
> **CRITICAL ESTIMATOR LEAKAGE**
> If you execute `StringIndexer.fit(full_dataset)`, the indexer inspects all category labels across both training and testing periods. In the case of `StandardScaler`, computing $\mu$ and $\sigma$ across the whole dataset means the training features secretly encode the mean and variance of the unseen future test set!
> 
> **The Fix:** We fit `StringIndexer` **strictly on `raw_train`**:
> ```python
> indexer_model = zone_indexer.fit(raw_train)
> indexed_train = indexer_model.transform(raw_train)
> indexed_test = indexer_model.transform(raw_test)
> ```
> We configure `handleInvalid="keep"` so that if an unseen municipal zone appears in the test period, it is mapped to an extra index rather than crashing.

---

## ⏳ 4. Why Time-Series Splits Must Be Chronological

### The Fatal Flaw of `randomSplit()` in Time Series
In standard tabular data (e.g., predicting customer churn or image classification), observations are assumed to be **independent and identically distributed (i.i.d.)**. Shuffling rows randomly using `randomSplit([0.8, 0.2])` is standard practice.

In time series, observations are **autocorrelated**: traffic at `08:05` is strongly correlated with traffic at `08:00`.
If you perform a random split:
- A training row might be from `08:05`.
- A testing row might be from `08:00`.
- The model is trained on future data to predict the past! The model memorizes diurnal patterns and achieves a near-perfect $R^2 \approx 0.99$ in testing.
- When deployed in production, the model fails completely because it cannot look into the future. This is called **Lookahead Bias / Temporal Leakage**.

### The 80% Chronological Cutoff Formula
Instead of splitting by row count, we calculate an explicit timestamp cutoff based on the **elapsed temporal horizon** of the dataset:
$$T_{\min} = \min(\text{window\_start}), \quad T_{\max} = \max(\text{window\_start})$$
$$\text{Cutoff Epoch} = \text{epoch}(T_{\min}) + 0.8 \times \left( \text{epoch}(T_{\max}) - \text{epoch}(T_{\min}) \right)$$
Everything before the cutoff belongs to the past (training); everything after belongs to the future (testing).

### The Conservative Boundary Policy & Boundary-Crossing Exclusion
Most naive implementations define:
- `train = df.filter(window_start <= cutoff)`
- `test = df.filter(window_start > cutoff)`

**Why is this naive definition flawed?**
Consider a row whose feature timestamp is $\text{window\_start} = \text{cutoff} - 2\text{ minutes}$.
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

**Mathematical Proof of Zero Temporal Leakage:**
$$\max(\text{train.target\_window\_start}) < \min(\text{test.window\_start})$$
In our verified live run:
- $\max(\text{train.target\_window\_start}) = \text{2012-03-01 11:50:00 (epoch 1330582800)}$
- $\text{Cutoff} = \text{2012-03-01 06:24:00 UTC (epoch 1330583040)}$
- $\min(\text{test.window\_start}) = \text{2012-03-01 11:55:00 (epoch 1330583100)}$
- **Gap:** Exactly 300 seconds of clean temporal separation. Zero overlap. Zero leakage.

---

## 🔬 5. Input Feature Specification & Justification

| Feature Name | Spark Type | Derivation Formula | Domain Justification |
| :--- | :--- | :--- | :--- |
| `avg_speed_kmph` | `Double` | Current window average speed | Primary autoregressive indicator of immediate roadway velocity. |
| `min_speed_kmph` | `Double` | Current window minimum speed | Captures local bottleneck points or localized slowdowns. |
| `max_speed_kmph` | `Double` | Current window maximum speed | Represents free-flow speed upper bound in the zone. |
| `speed_spread_kmph` | `Double` | `max_speed - min_speed` | Measures traffic turbulence, speed variance, and stop-and-go instability. |
| `traffic_record_count`| `Long` | Count of loop readings | Proxy for detector density and reporting integrity in the window. |
| `hour_of_day` | `Integer` | `hour(window_start)` (0–23) | Captures broad diurnal traffic rhythms (rush hour vs. night free-flow). |
| `sin_hour` | `Double` | $\sin(2\pi \cdot \text{hour} / 24)$ | Smooth cyclic representation of 24-hour periodic cycles. |
| `cos_hour` | `Double` | $\cos(2\pi \cdot \text{hour} / 24)$ | Ensures midnight (`23:55` and `00:05`) are treated as continuous, not distant. |
| `zone_index` | `Double` | `StringIndexer(zone_id)` | Numeric representation of municipal zone for tree and regression models. |

### 🚫 Deliberately Excluded Columns:
1. `next_avg_speed_kmph`: **EXCLUDED.** This is the forecasting target ($y$). Including it in features would constitute catastrophic label leakage.
2. `target_window_start`: **EXCLUDED.** Future timestamp metadata.
3. `vehicle_count` & `co2`: **EXCLUDED.** Do not exist in the Caltrans loop detector or EPA PM2.5 datasets; we refuse to invent synthetic numbers.
4. `min_lat`, `max_lat`, `min_lon`, `max_lon`: **EXCLUDED.** Null placeholders from provisional zone groupings; omitted to keep vectors finite and clean.
5. `AQI` & `Weather`: **EXCLUDED from primary traffic model.** In Day 3, we proved an authentic 7-month calendar mismatch (March 2012 for Traffic vs. October 2012 for Weather). Merging them into the primary traffic model would require unscientific date-shifting or drop 100% of rows. They remain separate feature tables for multimodal modeling.

---

## 📊 6. Real-Data Live Execution & Verification Evidence

### 6.1 Preparation Pipeline Execution (`prepare_training_data.py`)
Command:
```powershell
.\services\streaming\venv\Scripts\python services/streaming/prepare_training_data.py
```
Output:
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

### 6.2 Independent Reload Verification (`verify_split.py`)
Command:
```powershell
.\services\streaming\venv\Scripts\python services/streaming/verify_split.py
```
Output:
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

### 6.3 Split Metadata JSON (`data/split_metadata.json`)
```json
{
  "status": "PASS",
  "generated_at_utc": "2026-10-19T05:04:08.572138+00:00",
  "input_feature_table": "data/feature_table/traffic",
  "train_output_path": "data/train",
  "test_output_path": "data/test",
  "train_ratio": 0.8,
  "cutoff_timestamp_utc": "2012-03-01 06:24:00",
  "cutoff_epoch_seconds": 1330583040.0,
  "train_row_count": 365,
  "test_row_count": 100,
  "boundary_excluded_count": 5,
  "earliest_train_feature_ts": "2012-03-01 05:30:00",
  "latest_train_target_ts": "2012-03-01 11:50:00",
  "earliest_test_feature_ts": "2012-03-01 11:55:00",
  "latest_test_target_ts": "2012-03-01 13:35:00",
  "feature_columns": [
    "avg_speed_kmph",
    "min_speed_kmph",
    "max_speed_kmph",
    "speed_spread_kmph",
    "traffic_record_count",
    "hour_of_day",
    "sin_hour",
    "cos_hour",
    "zone_index"
  ],
  "target_column": "next_avg_speed_kmph",
  "feature_timestamp_column": "window_start",
  "target_timestamp_column": "target_window_start"
}
```

---

## 🧪 7. Automated Test Suite & Regression Execution Logs

### 7.1 Day 4 Unit Test Suite (`test_training_data.py`)
Command:
```powershell
.\services\streaming\venv\Scripts\pytest services/streaming/tests/test_training_data.py -v
```
Output:
```text
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

### 7.2 Day 3 Structured Streaming Regression Suite (`test_streaming.py`)
Command:
```powershell
.\services\streaming\venv\Scripts\pytest services/streaming/tests/test_streaming.py -v
```
Output:
```text
============================= 14 passed in 48.30s =============================
```

### 7.3 Day 2 Sensor Simulation Regression Suite (`test_producers.py`)
Command:
```powershell
.\services\producers\venv\Scripts\pytest services/producers/tests/test_producers.py -v
```
Output:
```text
============================= 19 passed in 0.35s ==============================
```

**Total Automated Test Count:** **44 / 44 tests PASSING (100% pass rate, zero regressions)**.

---

## 🛡️ 8. Atomic Viva Defense Playbook (Q&A for Examiners)

#### Question 1: *"Why did you use `lead()` over a partitioned Window specification instead of just sorting by time?"*
* **Defense:**  
  *"In Urbanix, telemetry flows simultaneously from 5 distinct municipal zones. If you sort strictly by `window_start` without partitioning by `zone_id`, the final 5-minute window of `zone_1` will take its 'next' value from `zone_2`'s first window! By enforcing `Window.partitionBy("zone_id").orderBy("window_start")`, we guarantee that future labels are constructed strictly within the same physical spatial boundary."*

#### Question 2: *"Why can't you use standard `train_test_split(shuffle=True)` or `randomSplit()` for this dataset?"*
* **Defense:**  
  *"Random splitting assumes independent and identically distributed (i.i.d.) observations. Traffic is a contiguous, auto-correlated physical time series. Randomly shuffling rows scatters past and future windows across both splits, allowing the model to train on future observations to predict past test rows (lookahead bias). Chronological splitting places an explicit cutoff in time, proving the model can generalize to unseen future intervals."*

#### Question 3: *"What is a boundary-crossing example and why did you discard it?"*
* **Defense:**  
  *"Consider a sample at time $t$ immediately before the cutoff, but whose target label $t + 300\text{s}$ falls after the cutoff. If placed in the training set, the model trains against ground truth from the testing period, leaking test-set target information into the training loss. If placed in the test set, its features belong to the training period. Discarding these boundary-crossing rows guarantees that $\max(\text{train.target}) < \min(\text{test.feature})$ with mathematical certainty."*

#### Question 4: *"Why did you fit `StringIndexer` on the training partition only?"*
* **Defense:**  
  *"Estimators in Spark ML learn internal mappings from the dataset. Fitting `StringIndexer` (or standard scalers) on the combined dataset leaks category distributions and frequency metadata from the test set into the preprocessing pipeline. Fitting strictly on the training partition and using `handleInvalid='keep'` ensures that preprocessing respects the chronological boundary and can handle unseen categories gracefully in production."*

#### Question 5: *"What does `VectorAssembler` do and why is it necessary for Day 5?"*
* **Defense:**  
  *"Spark MLlib models require all independent feature columns to be formatted into a single `VectorUDT` column (`features`). `VectorAssembler` takes our 9 validated numeric and indexed features and packs them into a single high-performance linear algebra vector per row. In Day 5, calling `LinearRegression().fit(train_df)` directly consumes this assembled `features` column."*

#### Question 6: *"How did you prevent target leakage in the feature vector?"*
* **Defense:**  
  *"We enforce explicit schema filtering and automated unit test assertions: `assert target_col not in feature_cols`. Furthermore, our independent verification script reloads the persisted Parquet metadata and validates that the vector attribute length exactly equals 9 and does not contain `next_avg_speed_kmph`."*

---

## 📌 9. Complete Step-by-Step PowerShell Command Cheat Sheet

```powershell
# 1. Navigate to repository root
cd "C:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix"

# 2. Run Day 4 automated unit tests
.\services\streaming\venv\Scripts\pytest services\streaming\tests\test_training_data.py -v

# 3. Execute Day 4 feature engineering & chronological split pipeline
.\services\streaming\venv\Scripts\python services\streaming\prepare_training_data.py

# 4. Execute independent reload verification
.\services\streaming\venv\Scripts\python services\streaming\verify_split.py

# 5. Run Day 3 streaming regression tests
.\services\streaming\venv\Scripts\pytest services\streaming\tests\test_streaming.py -v

# 6. Run Day 2 producer regression tests
.\services\producers\venv\Scripts\pytest services\producers\tests\test_producers.py -v

# 7. Inspect split metadata
Get-Content data\split_metadata.json
```

---

## 🏁 10. Day 4 Definition-of-Done Checklist

| # | Item | Requirement | Status | Evidence |
|---|---|---|---|---|
| 1 | **Repository & Data Audit** | Inspected live git status and 480 Parquet feature rows | **PASS** | Audit log & schema verified |
| 2 | **Forecasting Target Definition** | Defined `next_avg_speed_kmph` using per-zone `lead()` | **PASS** | `services/streaming/prepare_training_data.py` |
| 3 | **Continuity & Gap Handling** | Enforced $\Delta t == 300\text{s}$; dropped gaps and last row | **PASS** | Tested in `test_gap_handling_no_false_consecutive` |
| 4 | **Input Feature Specification** | 9 validated features; target strictly excluded | **PASS** | Tested in `test_target_excluded_from_features` |
| 5 | **VectorAssembler** | Produces `features` VectorUDT with 9 dimensions | **PASS** | Verified via `verify_split.py` |
| 6 | **Estimator Hygiene** | `StringIndexer` fitted on `train_df` only | **PASS** | Tested in `test_unseen_category_in_test_set` |
| 7 | **Chronological Cutoff** | Computed 80% time range cutoff (epoch 1330583040) | **PASS** | `data/split_metadata.json` |
| 8 | **Conservative Boundary Policy**| Excluded 5 boundary-crossing rows; zero leakage | **PASS** | $\max(\text{train.target}) < \min(\text{test.feature})$ |
| 9 | **Parquet Output Sinks** | `data/train/` (365 rows) & `data/test/` (100 rows) | **PASS** | Verified on disk |
| 10| **Independent Reload Verifier** | `verify_split.py` asserts zero leakage and exits 0 | **PASS** | Full pass on reload |
| 11| **Automated Unit Tests** | 11/11 tests passing in `test_training_data.py` | **PASS** | `pytest` 100% pass |
| 12| **Day 3 Regressions** | 14/14 tests passing in `test_streaming.py` | **PASS** | `pytest` 100% pass |
| 13| **Day 2 Regressions** | 19/19 tests passing in `test_producers.py` | **PASS** | `pytest` 100% pass |
| 14| **Git Hygiene** | Parquet datasets gitignored; working tree clean | **PASS** | `git status --short` verified |

---

## 🌳 11. Final Repository State & Git History

```text
urbanix/
├── _day-notes/
│   ├── day-01/DAY_01_INFRASTRUCTURE_REPORT.md
│   ├── day-02/DAY_02_SENSOR_SIMULATION_REPORT.md
│   ├── day-03/DAY_03_STREAMING_REPORT.md
│   └── day-04/DAY_04_FEATURE_ENGINEERING_REPORT.md
├── data/
│   ├── feature_table/traffic/      # Day 3 windowed feature sink (480 rows)
│   ├── train/                      # Day 4 training partition (365 rows, gitignored)
│   ├── test/                       # Day 4 testing partition (100 rows, gitignored)
│   ├── split_metadata.json         # Split metadata & cutoff documentation
│   ├── grid_zones.csv
│   └── traffic_sample.csv
├── services/
│   ├── streaming/
│   │   ├── prepare_training_data.py# Day 4 ML feature preparation pipeline
│   │   ├── verify_split.py         # Day 4 independent reload verifier
│   │   ├── tests/
│   │   │   ├── test_streaming.py   # 14 Day 3 regression tests
│   │   │   └── test_training_data.py# 11 Day 4 unit tests
│   │   ├── requirements.txt        # Added numpy>=1.24.0 for pyspark.ml
│   │   └── README.md               # Updated with Day 4 documentation
│   └── producers/
└── .gitignore                      # Added data/train/ and data/test/
```

---

## 🚀 12. Handover to Day 5 (MLlib Model Training & Evaluation)

With Day 4 complete, verified, and committed, we have a clean, leak-free, model-ready dataset.

### Day 5 Implementation Objectives:
1. **Model Ingestion:** Ingest `data/train/` and `data/test/` directly into PySpark.
2. **Model Training:**
   - Train a baseline **Linear Regression** model (`pyspark.ml.regression.LinearRegression`).
   - Train a non-linear **Decision Tree Regressor** (`pyspark.ml.regression.DecisionTreeRegressor`).
3. **Model Evaluation:**
   - Compute **$R^2$ (Coefficient of Determination)**, **$\text{RMSE}$ (Root Mean Squared Error)**, and **$\text{MAE}$ (Mean Absolute Error)** using `RegressionEvaluator`.
   - Log metrics per zone and in aggregate to satisfy **FR-4** and **FR-5**.
4. **Model Export:** Persist trained model pipelines for Day 6 live streaming inference.
