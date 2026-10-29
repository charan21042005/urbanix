# 📘 URBANIX — PYSPARK MLLIB MODEL TRAINING CODE GUIDE & ARCHITECTURAL REFERENCE

**Document Purpose:** Academic Companion, Viva Voce Guide & Demonstration Manual  
**Project:** Urbanix — Smart City IoT Traffic and Environmental Risk Forecasting Platform  
**Target Audience:** Capstone Examiners, External Reviewers & Student Presenter  
**Repository Path:** `c:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix`  
**Framework:** Apache Spark MLlib 3.5.1 / PySpark 3.10.11

---

## 1. Executive Summary & Original Implementation Entry Point

In the original Urbanix development cycle, model training and out-of-sample evaluation were unified within a single production script:
* **Historical Master Training Entry Point:** [`services/streaming/train_evaluate.py`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/services/streaming/train_evaluate.py)
* **Feature Engineering & Chronological Split Entry Point:** [`services/streaming/prepare_training_data.py`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/services/streaming/prepare_training_data.py)
* **Master Evaluation Audit File:** [`reports/day-05/metrics.json`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/reports/day-05/metrics.json)

To make the code 100% accessible and understandable for your B.Tech capstone viva presentation, two clean, focused demonstration files were extracted from `train_evaluate.py`:
1. **[`services/streaming/train_linear_regression.py`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/services/streaming/train_linear_regression.py):** Dedicated Linear Regression pipeline ($R^2 = 0.7757$, deployed to production).
2. **[`services/streaming/train_decision_tree.py`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/services/streaming/train_decision_tree.py):** Dedicated Decision Tree Regressor pipeline ($R^2 = -0.2064$, rejected due to rush-hour extrapolation failure).

---

## 2. Dataset Paths, Target Column & 9-Dimensional Feature Vector

### Dataset Partitions (Chronologically Split, Zero Target Leakage)
* **Training Partition:** `data/train` — **365 rows** (timestamps $\le \text{2012-03-01 06:20:00 UTC}$, 80% chronological split).
* **Testing Partition:** `data/test` — **100 rows** (timestamps $\ge \text{2012-03-01 06:25:00 UTC}$, 20% held-out test split).
* **Quarantine Boundary:** Any observation spanning across the 06:20:00 boundary is strictly excluded to prevent boundary-crossing target leakage.

### Target Variable (Label $y$)
* **Column Name:** `next_avg_speed_kmph`
* **Definition:** The ground-truth average velocity in the **NEXT consecutive 5-minute interval** ($t+1$) for the identical municipal zone.
* **Continuity Enforcement:** Constructed using PySpark SQL `lead("avg_speed_kmph", 1)` over `Window.partitionBy("zone_id").orderBy("window_start")`. A strict assertion verifies that `delta_seconds == 300`; telemetry gaps are never treated as consecutive intervals.

### Feature Vector (Inputs $X$, 9 Dimensions)
Bundled via `VectorAssembler` into a `VectorUDT` column named `features`:
1. `avg_speed_kmph`: Current window average velocity ($\text{km/h}$).
2. `speed_spread_kmph`: Volatility proxy ($\text{max\_speed} - \text{min\_speed}$).
3. `min_speed_kmph`: Lowest loop detector speed in the 5-minute window.
4. `max_speed_kmph`: Highest loop detector speed in the 5-minute window.
5. `reading_count`: Total sensor loop observations (density/traffic volume proxy).
6. `sin_hour`: Cyclical diurnal encoding: $\sin(2\pi \cdot \text{hour} / 24)$.
7. `cos_hour`: Cyclical diurnal encoding: $\cos(2\pi \cdot \text{hour} / 24)$ (preserves 23:59 $\to$ 00:00 continuity).
8. `zone_index`: Categorical zone encoding ($0.0 \dots 4.0$) fitted strictly on training data via `StringIndexer`.
9. `speed_lag_1`: Velocity in the *previous* 5-minute window ($t-1$) for autoregressive acceleration capture.

---

## 3. Estimator Configuration & Model Comparison

| Parameter / Metric | Model A: Linear Regression (Production) | Model B: Decision Tree Regressor (Baseline) |
| :--- | :--- | :--- |
| **PySpark Estimator Class** | `pyspark.ml.regression.LinearRegression` | `pyspark.ml.regression.DecisionTreeRegressor` |
| **Key Hyperparameters** | `regParam=0.0`, `elasticNetParam=0.0`, `fitIntercept=True`, `standardization=True` | `maxDepth=8`, `seed=42` |
| **Mathematical Formulation** | $\hat{y} = \beta_0 + \sum_{i=1}^9 \beta_i x_i$ (Parametric Plane) | Piecewise-constant recursive binary variance-reduction tree |
| **Learned Artifacts** | Intercept $\beta_0 = +44.0640$, 9 learned weights | Depth = 8 levels, 307 total nodes, feature importances |
| **$R^2$ Score (Test Set)** | **$+0.7757$** (77.57% variance explained) | **$-0.2064$** (Worse than predicting sample mean) |
| **RMSE (Typical Error)** | **$2.9280\text{ km/h}$** | **$6.7904\text{ km/h}$** |
| **MAE (Mean Error)** | **$2.4733\text{ km/h}$** | **$5.1781\text{ km/h}$** |
| **Roadmap Gate ($R^2 > 0.75$)** | **PASSED [MET]** | **FAILED [OVERFITTING]** |
| **Production Decision** | **Saved to `models/linear_regression` & Deployed to Streaming** | **Saved to `models/decision_tree` for Evaluation Record Only** |

---

## 4. How Training Feeds Live Streaming Inference

1. **Serialization:**
   `train_evaluate.py` saves the trained model using Spark's native serializer:
   `lr_model.write().overwrite().save("models/linear_regression")`
2. **Deserialization at Streaming Startup:**
   In `services/streaming/streaming_job.py` (Line 84) and `streaming_inference.py` (Line 84):
   `forecasting_model = LinearRegressionModel.load("models/linear_regression")`
   The model is loaded **once on the Spark driver** at startup to prevent expensive disk I/O on each micro-batch.
3. **ForeachBatch Scoring:**
   On every finalized 5-minute micro-batch from Kafka, `foreachBatch` passes the DataFrame to `predict_traffic_speed()`, which derives the exact same 9 features and calls `model.transform()`, appending predictions with `predicted_speed_kmph` to `data/feature_table/traffic/`.
4. **GraphX Rerouting:**
   The Scala GraphX engine reads `predicted_speed_kmph` from Parquet to update edge weights and recompute shortest paths.

---

## 5. Exact Terminal Commands for Demonstration

### Command 1: Run Linear Regression Training Demo
* **Working Directory:** `c:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix`
* **Command:**
  ```powershell
  .\services\streaming\venv\Scripts\python.exe services/streaming/train_linear_regression.py
  ```
* **Expected Output:**
  ```
  [*] Training Partition : 365 rows
  [*] Testing Partition  : 100 rows
  [+] Linear Regression training complete.
      - Intercept: +44.0640
      - R²: 0.7757 (Target: > 0.75) -> PASSED
      - RMSE: 2.9280 km/h
      - MAE:  2.4733 km/h
  ```

### Command 2: Run Decision Tree Training Demo (Overfitting Explanation)
* **Working Directory:** `c:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix`
* **Command:**
  ```powershell
  .\services\streaming\venv\Scripts\python.exe services/streaming/train_decision_tree.py
  ```
* **Expected Output:**
  ```
  [+] Decision Tree training complete.
      - Tree Depth: 8 levels (307 nodes)
      - Top Feature: avg_speed_kmph (87.0% importance)
      - R²: -0.2064
      - RMSE: 6.7904 km/h
      - MAE:  5.1781 km/h
  ```

### Command 3: Run the Independent Artifact Parity Verifier
* **Working Directory:** `c:\Users\Tokachichu SriCharan\OneDrive\Documents\GitHub\urbanix`
* **Command:**
  ```powershell
  .\services\streaming\venv\Scripts\python.exe services/streaming/verify_models.py
  ```
* **Expected Output:**
  ```
  [PASS] All verification assertions satisfied.
  - Linear Regression tolerance check: PASSED (< 1e-4)
  - Decision Tree tolerance check: PASSED (< 1e-4)
  ```

---

## 6. Viva Voce Defense Script (Q&A for Capstone Evaluators)

### Q1: What is the mathematical difference between `.fit()` and `.transform()` in PySpark MLlib?
* **Answer:**
  * **`.fit(train_df)`** belongs to an **Estimator** (like `LinearRegression`). It executes distributed optimization (Ordinary Least Squares or L-BFGS) across the training dataset to solve the normal equations and calculate the optimal weights ($\beta$). It returns a **Transformer/Model** instance (`LinearRegressionModel`).
  * **`.transform(test_df)`** belongs to a **Transformer/Model**. It takes a DataFrame containing the `features` column, performs a deterministic matrix-vector multiplication ($\hat{y} = X\beta$), and appends a new column called `prediction` without changing any existing data.

### Q2: Why is the Decision Tree $R^2$ negative ($-0.2064$)? Does that mean there is a bug?
* **Answer:**
  * No, it is not a bug; it is an authentic machine-learning phenomenon caused by **temporal out-of-sample extrapolation failure**.
  * In-sample $R^2$ is bounded in $[0, 1]$. Out-of-sample $R^2$ is defined as:
    $$R^2 = 1 - \frac{SS_{\text{residual}}}{SS_{\text{total}}} = 1 - \frac{\sum (y_i - \hat{y}_i)^2}{\sum (y_i - \bar{y})^2}$$
  * A negative $R^2$ occurs whenever $SS_{\text{residual}} > SS_{\text{total}}$, meaning the model's predictions have larger error than simply predicting the historical average velocity ($\bar{y}$).
  * Decision trees partition data into piecewise-constant leaf regions. During morning rush hour (the test partition), traffic velocities dropped to severe congestion levels not observed during the overnight training data. Because decision trees cannot extrapolate beyond the minimum leaf value seen during training, the tree predicted early-morning free-flow velocities, producing massive errors.
  * Linear Regression succeeded ($R^2 = 0.7757$) because its continuous linear slope ($\beta_1 = +0.7591$) naturally extrapolates downward trends.

### Q3: Why didn't you randomly shuffle the data before splitting train and test sets?
* **Answer:**
  * In time-series forecasting, randomly shuffling data causes **temporal data leakage**.
  * If we randomly split by row, the model would be trained on sensor readings from 07:30 UTC to predict speeds at 07:05 UTC. In the real world, an AI platform cannot observe the future to predict the past.
  * Urbanix enforces an authentic **chronological split**: all training records occur strictly before 06:20:00 UTC, and all test records occur strictly after 06:25:00 UTC.

### Q4: How are R², RMSE, and MAE physically interpreted in traffic engineering?
* **Answer:**
  * **$R^2 = 0.7757$:** The model explains **77.57% of the total variance** in future traffic velocity during unseen morning rush hour.
  * **$\text{MAE} = 2.47\text{ km/h}$:** On average, the predicted speed deviates from actual highway sensor speed by less than **$2.5\text{ km/h}$**.
  * **$\text{RMSE} = 2.93\text{ km/h}$:** Because RMSE squares errors before averaging, large errors are heavily penalized. The small difference between RMSE ($2.93$) and MAE ($2.47$) mathematically proves that extreme prediction outliers are virtually absent.

