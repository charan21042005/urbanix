# URBANIX — DAY 5 DEEP-DIVE RESEARCH & IMPLEMENTATION REPORT
## MLlib Model Training, Chronological Evaluation, Mathematical Verification, and Academic Viva Guide

---

## Executive Summary & Milestone Identity

| Parameter | Specification / Result |
| :--- | :--- |
| **Project Identity** | Urbanix — Smart City IoT Traffic & Environmental Risk Forecasting Platform |
| **Day Milestone** | **Day 5 of 12: MLlib Model Training, Evaluation, and Reproducibility** |
| **Repository & Branch** | `https://github.com/charan21042005/urbanix` (`main`) |
| **Operating System & Runtime** | Windows 11 / Python 3.10.11 / Apache Spark 3.5.1 / PySpark MLlib |
| **Input Data Partitions** | `data/train/` (365 rows) and `data/test/` (100 rows) |
| **Target Variable ($y$)** | `next_avg_speed_kmph` (observed velocity in consecutive $t+1$ 5-minute interval) |
| **Feature Vector ($\mathbf{x}$)** | 9-dimensional Spark ML `VectorUDT` |
| **Model A (Linear Regression)** | **$R^2 = 0.7757$**, **$\text{RMSE} = 2.9280\text{ km/h}$**, **$\text{MAE} = 2.4733\text{ km/h}$** |
| **Model B (Decision Tree Regressor)**| **$R^2 = -0.2064$**, **$\text{RMSE} = 6.7904\text{ km/h}$**, **$\text{MAE} = 5.1781\text{ km/h}$** |
| **Success Threshold ($R^2 > 0.75$)** | **PASSED [MET]** by Linear Regression ($0.7757 > 0.75$) |
| **Automated Test Results** | **9 passed in `test_model_training.py`**; **53 total tests passing across project** |
| **Independent Verification** | **ALL CHECKS PASSED** (`verify_models.py` parity $\Delta \le 3.6 \times 10^{-5}$) |
| **Downstream Readiness** | **Model A designated as primary live forecasting engine for Day 6 inference** |

---

## 1. Verified Day 4 Data Ingestion & Provenance Audit

Before fitting any machine-learning estimator, Day 5 re-verified the exact inputs produced during the Day 4 feature engineering and chronological train/test split.

### 1.1 Chronological Split Verification Summary
The supervised dataset was prepared from the tumbling 5-minute traffic feature table (`data/feature_table/traffic/`), derived from high-frequency sensor readings across five municipal zones in Los Angeles County.

```
Total Raw Feature Table Rows:   480 (96 five-minute window intervals * 5 zones)
Physical Speed-Quality Filter:  480 retained (0 dropped)
Continuity Gap Exclusion:       5 dropped (00:10 UTC window: 900s gap to 00:25 UTC)
Terminal Window Exclusion:      5 dropped (08:05 UTC terminal window: no target t+1)
Boundary Crossing Exclusion:    5 dropped (06:20 UTC feature window: target is at 06:25 UTC > cutoff)
---------------------------------------------------------------------------------------------------
Total Valid Modeling Samples:   465
Training Partition (80% split): 365 rows (temporal coverage: 2012-03-01 00:00:00 to 06:20:00 UTC)
Testing Partition (held-out):   100 rows (temporal coverage: 2012-03-01 06:25:00 to 08:00:00 UTC)
Cutoff Timestamp:               2012-03-01 06:24:00 UTC
Exact Conservation Equation:    480 = 365 + 100 + 5 + 5 + 5
```

### 1.2 Zero-Leakage Audit
1. **Temporal Non-Overlap:** The maximum training target timestamp is `2012-03-01 06:20:00 UTC`. The minimum testing feature timestamp is `2012-03-01 06:25:00 UTC`. The minimum testing target timestamp is `2012-03-01 06:30:00 UTC`. No training sample predicts a label situated inside or after the testing cutoff.
2. **Key Disjointness:** Training and testing partitions share zero sample keys `(window_start, zone_id)`.
3. **Transformer Isolation:** All learned preprocessing transformations (specifically `StringIndexer` for `zone_index`) were fitted strictly on the training partition and transformed onto the testing partition, guaranteeing zero distribution bleeding.
4. **Target Exclusion:** The feature vector contains 9 dimensions; `next_avg_speed_kmph` is strictly excluded from `VectorAssembler`.

---

## 2. Machine Learning Fundamentals for Distributed Traffic Forecasting

### 2.1 The Distributed ML Problem Formulation
In smart-city intelligent transportation systems (ITS), velocity forecasting is framed as a supervised spatiotemporal regression problem:
Given a feature vector $\mathbf{x}_i \in \mathbb{R}^d$ capturing historical rolling window telemetry, spatial topology, and diurnal cyclical phase for zone $z$ at time $t$, predict the future average velocity $\hat{y}_i \in \mathbb{R}^+$ for the identical zone at time $t + \Delta t$ ($\Delta t = 300\text{ seconds}$):

$$\hat{y}_i = f(\mathbf{x}_i; \mathbf{\Theta})$$

where $\mathbf{\Theta}$ represents the learned model parameters.

### 2.2 Feature Vector Architecture
The 9 features in the Spark `VectorUDT` are ordered deterministically:
1. `avg_speed_kmph` ($x_1$): Current 5-minute window mean vehicle speed ($\text{km/h}$).
2. `speed_spread_kmph` ($x_2$): Difference between maximum and minimum speed in current window ($\text{km/h}$).
3. `min_speed_kmph` ($x_3$): Minimum vehicle speed recorded in current window ($\text{km/h}$).
4. `max_speed_kmph` ($x_4$): Maximum vehicle speed recorded in current window ($\text{km/h}$).
5. `reading_count` ($x_5$): Number of discrete sensor events aggregated into the 5-minute window.
6. `sin_hour` ($x_6$): Diurnal harmonic component $\sin\left(\frac{2\pi \cdot \text{hour\_of\_day}}{24}\right)$.
7. `cos_hour` ($x_7$): Diurnal harmonic component $\cos\left(\frac{2\pi \cdot \text{hour\_of\_day}}{24}\right)$.
8. `zone_index` ($x_8$): String-indexed municipal zone categorical identifier ($0.0 \dots 4.0$).
9. `speed_lag_1` ($x_9$): Velocity observed in the preceding consecutive 5-minute window $t-1$ ($\text{km/h}$).

### 2.3 Why VectorAssembler is Required in Apache Spark MLlib
Unlike single-node Python frameworks (e.g. Scikit-Learn) that accept 2D NumPy arrays or Pandas DataFrames, Apache Spark operates across distributed resilient distributed datasets (RDDs). In Spark MLlib:
- Every regression and classification estimator expects a single unified column containing a `org.apache.spark.ml.linalg.Vector` (represented in PySpark as `VectorUDT`).
- `VectorAssembler` concatenates disparate numeric columns (doubles, integers, indexed floats) into a compact, contiguous in-memory vector per row without requiring JVM-to-Python serialization on every column access.
- This design enables low-overhead linear algebra operations using hardware-accelerated BLAS/LAPACK routines across worker nodes.

---

## 3. Estimator Deep-Dives: Architecture, Equations, and Behavior

### 3.1 Model A: Spark MLlib `LinearRegression`

#### Mathematical Formulation
Linear Regression models the conditional expectation of velocity as an affine linear combination of the input features:

$$\hat{y}_i = w_0 + \sum_{j=1}^{9} w_j x_{ij} = \mathbf{w}^T \mathbf{x}_i + w_0$$

where $\mathbf{w} \in \mathbb{R}^9$ is the weight vector and $w_0 \in \mathbb{R}$ is the intercept.

#### Loss Function and Optimization
The estimator minimizes the regularized mean squared error objective:

$$\mathcal{L}(\mathbf{w}, w_0) = \frac{1}{2n} \sum_{i=1}^n \left( y_i - (\mathbf{w}^T \mathbf{x}_i + w_0) \right)^2 + \lambda \left[ \frac{1 - \alpha}{2} \|\mathbf{w}\|_2^2 + \alpha \|\mathbf{w}\|_1 \right]$$

For our primary baseline, Ordinary Least Squares (OLS) was configured with `regParam=0.0` ($\lambda = 0$), `fitIntercept=True`, and `standardization=True`.

#### Empirical Learned Parameters
From the 365 training rows, Spark MLlib estimated:
- **Learned Intercept ($w_0$):** `44.0640`
- **Learned Coefficients ($\mathbf{w}$):**

| Feature Name | Index | Learned Weight ($w_j$) | Physical & Statistical Interpretation |
| :--- | :---: | :---: | :--- |
| `avg_speed_kmph` | 0 | **+0.7591** | Dominant autoregressive momentum: a 10 km/h increase in current speed predicts a +7.59 km/h increase in the next interval. |
| `speed_spread_kmph` | 1 | **+0.0117** | Slight positive effect of speed variance within the window. |
| `min_speed_kmph` | 2 | **-0.1205** | Negative dampener representing localized bottleneck braking. |
| `max_speed_kmph` | 3 | **-0.0129** | Minor negative dampener. |
| `reading_count` | 4 | **+0.0177** | Minor positive relationship between sensor volume density and target speed. |
| `sin_hour` | 5 | **-1.9880** | Cyclic diurnal phase modulation reflecting morning traffic volume buildup. |
| `cos_hour` | 6 | **+4.4857** | Diurnal component capturing early morning speed transitions. |
| `zone_index` | 7 | **-7.4699** | Spatial baseline offset across municipal zone identifiers. |
| `speed_lag_1` | 8 | **-0.1790** | Mean-reverting autoregressive second-order lag component. |

#### Multicollinearity Discovery & Spark Solver Adaptation
During training with `regParam=0.0`, Spark issued the following warning:
```
WARN Instrumentation: Cholesky solver failed due to singular covariance matrix. Retrying with Quasi-Newton solver.
```
**Academic Root Cause Analysis:**
In Day 4 feature engineering, `speed_spread_kmph` was defined as:
$$\text{speed\_spread\_kmph} = \text{max\_speed\_kmph} - \text{min\_speed\_kmph}$$
This constitutes an **exact linear dependency** ($\mathbf{x}_2 = \mathbf{x}_4 - \mathbf{x}_3$). Consequently, the empirical covariance matrix $\mathbf{X}^T \mathbf{X}$ has rank at most 8, not 9, making it mathematically singular ($\det(\mathbf{X}^T \mathbf{X}) = 0$). 
The standard closed-form normal equation solver:
$$\mathbf{w} = (\mathbf{X}^T \mathbf{X})^{-1} \mathbf{X}^T \mathbf{y}$$
cannot perform the Cholesky factorization of a singular matrix. 
Spark's resilient MLlib implementation automatically detects this singularity and seamlessly switches to the iterative **Quasi-Newton (L-BFGS)** optimizer. L-BFGS converges to the minimum-norm least-squares solution without requiring matrix inversion, yielding optimal predictive generalization ($R^2 = 0.7757$).

---

### 3.2 Model B: Spark MLlib `DecisionTreeRegressor`

#### Mathematical Formulation & Splitting Mechanics
A Decision Tree Regressor recursively partitions the 9-dimensional continuous feature space $\mathcal{X} \subset \mathbb{R}^9$ into $M$ disjoint hyper-rectangles (regions) $R_1, R_2, \dots, R_M$. 
For any input query vector $\mathbf{x} \in R_m$, the tree produces a **piecewise constant prediction** equal to the sample mean of training targets falling within leaf node $R_m$:

$$\hat{y}(\mathbf{x}) = \sum_{m=1}^M c_m \cdot \mathbb{I}(\mathbf{x} \in R_m), \quad \text{where } c_m = \frac{1}{|R_m|} \sum_{i \in R_m} y_i$$

At every candidate split node, the algorithm scans all 9 features $j \in \{1 \dots 9\}$ and threshold split values $s$ to maximize variance reduction (minimizing child Mean Squared Error):

$$\arg\min_{j, s} \left[ \sum_{i \in R_{\text{left}}(j, s)} (y_i - \bar{y}_{\text{left}})^2 + \sum_{i \in R_{\text{right}}(j, s)} (y_i - \bar{y}_{\text{right}})^2 \right]$$

#### Model Configuration & Structure
- **Hyperparameters:** `maxDepth=8`, `minInstancesPerNode=1`, `seed=42`.
- **Learned Tree Depth:** `8`
- **Total Tree Nodes:** `307` nodes (indicating extensive branching on the 365 training rows).
- **Feature Importances:**

| Feature Name | Feature Importance Score | Interpretation |
| :--- | :---: | :--- |
| `avg_speed_kmph` | **0.8704 (87.0%)** | Overwhelmingly dominant splitting feature at top levels of the tree. |
| `min_speed_kmph` | **0.0361 (3.6%)** | Secondary split criterion for localized congestion onset. |
| `max_speed_kmph` | **0.0251 (2.5%)** | Free-flow upper velocity bound checks. |
| `reading_count` | **0.0239 (2.4%)** | Traffic density splits. |
| `speed_spread_kmph` | **0.0222 (2.2%)** | Window turbulence splits. |
| `zone_index` | **0.0107 (1.1%)** | Spatial routing splits. |
| `sin_hour` | **0.0068 (0.7%)** | Time-of-day split. |
| `speed_lag_1` | **0.0047 (0.5%)** | Lag momentum. |
| `cos_hour` | **0.0000 (0.0%)** | Unused in optimal tree partition. |

---

## 4. The Fundamental Extrapolation Failure: Why the Decision Tree Yielded Negative $R^2$

One of the most profound and academically defensible insights discovered in Day 5 is the stark contrast between Linear Regression ($R^2 = +0.7757$) and Decision Tree Regressor ($R^2 = -0.2064$).

### 4.1 Theoretical Proof: Piecewise Constant Bounding
By definition, a decision tree's prediction is bounded by the minimum and maximum target values observed in the training partition:

$$\min_{i \in \mathcal{D}_{\text{train}}} y_i \le \hat{y}_{\text{tree}}(\mathbf{x}) \le \max_{i \in \mathcal{D}_{\text{train}}} y_i$$

More critically, in every leaf region $R_m$, the prediction is a **horizontal constant surface** with zero gradient:
$$\nabla_{\mathbf{x}} \hat{y}_{\text{tree}}(\mathbf{x}) = \mathbf{0}$$

### 4.2 The Chronological Regime Shift
In a random train/test split (common in non-temporal ML), training and testing rows are independently and identically distributed (i.i.d.) across the entire 24-hour cycle. In that naive setting, decision trees often appear to achieve high $R^2$ because test rows are surrounded by nearby training samples from the same hour.

However, in our **chronologically split time series**:
- **Training Set (00:00 to 06:20 UTC):** Spans the late-night and early dawn period. Vehicle volumes were minimal, and speeds were consistently high and free-flowing.
- **Testing Set (06:25 to 08:00 UTC):** Spans the onset and peak of the morning commuter rush hour. As vehicle density surges, velocities experience an unprecedented downward trend (speed collapse).

### 4.3 Why the Tree Failed while Linear Regression Succeeded
1. **Decision Tree (Failure to Extrapolate):** When presented with the morning rush-hour features, the tree navigates down to its terminal leaves. Because it never witnessed the morning speed drop during training, its leaf predictions $\hat{y}_m$ are anchored to late-night free-flow averages. The tree **cannot extrapolate a downward trend beyond the training domain**. Consequently, its prediction errors are massive.
2. **Linear Regression (Successful Gradient Extrapolation):** Linear regression learns a global parametric hyperplane with negative coefficients on `sin_hour` ($-1.9880$) and positive slopes on current speed. As time advances and diurnal features shift into the morning quadrant, the linear model continuously scales its predictions downward, closely matching the actual physical deceleration of traffic.

### 4.4 The Mathematical Derivation of Negative $R^2$
The Coefficient of Determination is defined as:

$$R^2 = 1 - \frac{\text{SS}_{\text{res}}}{\text{SS}_{\text{tot}}} = 1 - \frac{\sum_{i=1}^n (y_i - \hat{y}_i)^2}{\sum_{i=1}^n (y_i - \bar{y})^2}$$

where $\bar{y} = \frac{1}{n} \sum_{i=1}^n y_i$ is the empirical mean of the held-out test partition.
- $\text{SS}_{\text{tot}}$ measures the variance of the true test labels around their own mean.
- $\text{SS}_{\text{res}}$ measures the sum of squared prediction residuals.

If a model predicts the test sample mean for every point ($\hat{y}_i = \bar{y}$), $\text{SS}_{\text{res}} = \text{SS}_{\text{tot}}$, and $R^2 = 0.0$.
If a model's predictions are **worse than predicting the simple horizontal mean of the test labels**, then:
$$\text{SS}_{\text{res}} > \text{SS}_{\text{tot}} \implies \frac{\text{SS}_{\text{res}}}{\text{SS}_{\text{tot}}} > 1 \implies R^2 < 0$$

For our Decision Tree:
$$\text{SS}_{\text{res}} = 100 \times \text{MSE} = 100 \times (6.7904)^2 = 4610.95$$
$$\text{SS}_{\text{tot}} = 100 \times \text{Var}(y_{\text{test}}) = 100 \times (6.1824)^2 = 3822.21$$
$$R^2 = 1 - \frac{4610.95}{3822.21} = 1 - 1.2064 = -0.2064$$

This negative $R^2$ is not a software bug or implementation defect; it is rigorous empirical proof that single decision trees without trend extrapolation are fundamentally unsuited for non-stationary chronological forecasting.

---

## 5. Actual Experimental Results & Metric Analysis

### 5.1 Aggregate Model Comparison Table

Evaluated on the exact 100 held-out test rows across all 5 municipal zones.

| Model Architecture | Estimator Class | Hyperparameters | $R^2$ Score | RMSE ($\text{km/h}$) | MAE ($\text{km/h}$) | Evaluated Test Rows | Roadmap Threshold ($R^2 > 0.75$) | Selected for Day 6? |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Linear Regression** | `pyspark.ml.regression.LinearRegression` | `regParam=0.0`, `fitIntercept=True`, `standardization=True` | **+0.7757** | **2.9280** | **2.4733** | 100 | **PASSED (0.7757 > 0.75)** | **YES (Primary Live Engine)** |
| **Decision Tree Regressor** | `pyspark.ml.regression.DecisionTreeRegressor` | `maxDepth=8`, `minInstances=1`, `seed=42` | **-0.2064** | **6.7904** | **5.1781** | 100 | **FAILED (-0.2064 < 0.75)** | **NO (Documented Baseline)** |

> **Key Milestone Achievement:** Linear Regression successfully surpasses the project roadmap milestone of $R^2 > 0.75$, demonstrating exceptional predictive accuracy with an average absolute error of just $2.47\text{ km/h}$ across 100 unseen future rush-hour observations.

---

### 5.2 Granular Per-Zone Performance Comparison

Each of the 5 municipal zones contains exactly 20 consecutive 5-minute test observations during the morning rush hour (06:25 to 08:00 UTC).

| Zone ID | Municipal Zone Description | Test Samples ($n$) | Linear Regression $R^2$ | Linear Regression RMSE ($\text{km/h}$) | Linear Regression MAE ($\text{km/h}$) | Decision Tree $R^2$ | Decision Tree RMSE ($\text{km/h}$) | Decision Tree MAE ($\text{km/h}$) |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `zone_1` | Downtown Core / Central Business District | 20 | **0.6622** | **3.6885** | **3.2586** | -0.1066 | 4.7937 | 4.2885 |
| `zone_2` | San Fernando Valley Arterial Corridor | 20 | **0.6109** | **2.5649** | **2.3580** | -0.0076 | 4.1264 | 3.5134 |
| `zone_3` | San Gabriel Foothills / Eastern Highway | 20 | **0.8278** | **2.0458** | **1.6137** | +0.4728 | 3.5855 | 2.9261 |
| `zone_4` | South Central Freight / Transit Corridor | 20 | **0.6674** | **3.4688** | **3.0181** | -0.6277 | 7.6493 | 5.5670 |
| `zone_5` | Harbor & Outer Coastal Ring Road | 20 | **0.8390** | **2.4042** | **2.1180** | -1.1561 | 10.7410 | 9.5954 |

#### Per-Zone Insights
1. **Outer Ring & Foothills (`zone_3`, `zone_5`):** Linear regression achieves outstanding accuracy ($R^2 = 0.8278$ and $0.8390$) with RMSE around $2.0\text{--}2.4\text{ km/h}$. These zones experience smoother, more predictable velocity transitions.
2. **Dense Arterials (`zone_1`, `zone_2`, `zone_4`):** Velocity variance is higher due to stop-and-go bottlenecking. Linear regression still delivers solid forecasting ($R^2 \ge 0.61$), keeping RMSE under $3.7\text{ km/h}$.
3. **Decision Tree Degradation:** In `zone_5`, the decision tree experienced an $R^2$ of $-1.1561$ with RMSE of $10.74\text{ km/h}$, directly reflecting its inability to predict the sharp morning deceleration on the coastal ring road.

---

## 6. Mathematical Formulas & Step-by-Step Metric Derivations

### 6.1 Coefficient of Determination ($R^2$)
$$R^2 = 1 - \frac{\text{SS}_{\text{res}}}{\text{SS}_{\text{tot}}} = 1 - \frac{\sum_{i=1}^n (y_i - \hat{y}_i)^2}{\sum_{i=1}^n (y_i - \bar{y})^2}$$

- **Range:** $(-\infty, 1.0]$. A perfect model achieves $R^2 = 1.0$. A baseline model predicting the label mean achieves $R^2 = 0.0$.
- **Interpretation:** Represents the proportion of variance in future traffic speed explained by the model's feature vector.

### 6.2 Root Mean Squared Error (RMSE)
$$\text{RMSE} = \sqrt{\frac{1}{n} \sum_{i=1}^n (y_i - \hat{y}_i)^2}$$

- **Units:** Physical speed units ($\text{km/h}$).
- **Property:** Due to the quadratic squaring of errors before averaging, RMSE penalizes large forecast outliers severely. In traffic management, severe velocity forecast errors (e.g., failing to predict a sudden 30 km/h deceleration) are much more hazardous than several small 2 km/h deviations.

### 6.3 Mean Absolute Error (MAE)
$$\text{MAE} = \frac{1}{n} \sum_{i=1}^n |y_i - \hat{y}_i|$$

- **Units:** Physical speed units ($\text{km/h}$).
- **Property:** Measures the linear expected magnitude of forecast errors without disproportionate weighting on outliers.

### 6.4 Mathematical Relationship: Why $\text{RMSE} \ge \text{MAE}$
By the Cauchy-Schwarz inequality (and Jensen's inequality applied to the convex square function $f(u) = u^2$):

$$\left( \frac{1}{n} \sum_{i=1}^n |e_i| \right)^2 \le \frac{1}{n} \sum_{i=1}^n |e_i|^2 \implies \text{MAE} \le \text{RMSE}$$

Equality holds if and only if all individual absolute prediction errors are identical ($|e_1| = |e_2| = \dots = |e_n|$).
In our Linear Regression results:
$$\text{RMSE} = 2.9280\text{ km/h} \ge \text{MAE} = 2.4733\text{ km/h}$$
The modest gap ($0.4547\text{ km/h}$) confirms that the linear regression model does not suffer from extreme outlier prediction spikes on the held-out test partition.

---

## 7. Model Persistence & Independent Verification Audit

### 7.1 Persisted Artifacts Inventory
All model artifacts were serialized using native Apache Spark MLlib persistence:
- `models/linear_regression/`: Contains `metadata/` and Parquet model parameters (weights, intercept).
- `models/decision_tree/`: Contains `metadata/` and Parquet decision tree topology (nodes, split thresholds).
- `reports/day-05/metrics.json`: Machine-readable evaluation summary containing full provenance, hyperparameters, aggregate scores, and per-zone metrics.
- `reports/day-05/per_zone_metrics.json`: Granular per-zone metrics JSON.
- `reports/day-05/results.md`: Human-readable markdown results document.
- `reports/day-05/verification.md`: Formal verification audit checklist.

### 7.2 Independent Verification Results (`verify_models.py`)
An independent post-training audit script was executed to reload the saved models from disk, transform `data/test/`, and verify numerical reproducibility.

```
===========================================================================
URBANIX DAY 5: INDEPENDENT MODEL AUDIT & VERIFICATION
===========================================================================

---------------------------------------------------------------------------
VERIFICATION CHECKLIST SUMMARY
---------------------------------------------------------------------------
[PASS] Linear Regression Model Artifact     : Path: models/linear_regression
[PASS] Decision Tree Model Artifact         : Path: models/decision_tree
[PASS] Persisted Metrics JSON               : Path: reports/day-05/metrics.json
[PASS] Held-Out Test Dataset                : Path: data/test
[PASS] Test Sample Count Reconciliation     : Expected 100, Observed 100
[PASS] Per-Zone Sample Uniformity (20/zone) : Observed zones: {'zone_1': 20, 'zone_2': 20, 'zone_3': 20, 'zone_4': 20, 'zone_5': 20}
[PASS] Linear Regression Reload & Parity    : delta_R2=0.000020, delta_RMSE=0.000036, delta_MAE=0.000007
[PASS] Decision Tree Reload & Parity        : delta_R2=0.000036, delta_RMSE=0.000019, delta_MAE=0.000000
[PASS] Prediction Numerical Sanity          : LR invalid: 0, DT invalid: 0
[PASS] Success Threshold Honesty Audit      : LR R2=0.7757 > 0.75 -> True
---------------------------------------------------------------------------

[+] FINAL VERIFICATION RESULT: ALL CHECKS PASSED.
```

---

## 8. Automated Test Suite Execution Evidence

The project test suite was executed across all components to ensure zero regressions:

### 8.1 Day 5 Model Training & Evaluation Tests (`test_model_training.py`)
```bash
pytest services/streaming/tests/test_model_training.py -v
```
**Results (9 passed in 72.19s):**
- `test_compute_regression_metrics_deterministic` — **PASSED** (asserts exact mathematical calculation of RMSE, MAE, R² against hand-derived values).
- `test_compute_regression_metrics_constant_label` — **PASSED** (verifies that zero label variance returns `R² = None` without division-by-zero crashes).
- `test_compute_regression_metrics_empty_df` — **PASSED** (verifies graceful handling of 0 rows).
- `test_validate_input_dataframe_missing_column` — **PASSED** (verifies strict schema enforcement).
- `test_validate_input_dataframe_invalid_dimension` — **PASSED** (verifies rejection of non-9-dimensional vectors).
- `test_evaluate_per_zone_groups_accurately` — **PASSED** (verifies per-zone isolation and metric computation).
- `test_real_day4_dataset_existence_and_dimensions` — **PASSED** (reconciles 365 train rows and 100 test rows).
- `test_full_pipeline_execution_and_persistence` — **PASSED** (runs end-to-end training and reload in temp directory).
- `test_independent_verifier_tool` — **PASSED** (verifies automated execution of `verify_models.py`).

### 8.2 Comprehensive Project-Wide Regression Suite
- `services/streaming/tests/test_training_data.py`: **11 passed** (Day 4 split & leakage tests).
- `services/streaming/tests/test_streaming.py`: **14 passed** (Day 3 streaming & windowing tests).
- `services/producers/tests/test_producers.py`: **19 passed** (Day 2 sensor simulation & Kafka payload tests).
- **Total Passing Tests Across Entire Project:** **53 tests passing, 0 failures, 0 errors.**

---

## 9. Academic Project Viva Voce Defense Guide (15 Master Q&A Pairs)

This section prepares the student for rigorous viva voce examination by academic evaluators, external examiners, and industry project reviewers.

---

### Q1: Why did you train two different regression estimators instead of just one?
**Answer:**  
In empirical research, evaluating a single model in isolation provides no baseline for comparison. We implemented a dual-model benchmarking strategy:
1. **Model A (Linear Regression):** Serves as an interpretable parametric baseline that models continuous linear slopes and autoregressive temporal patterns.
2. **Model B (Decision Tree Regressor):** Serves as a non-parametric, non-linear estimator capable of capturing complex feature interactions.
Evaluating both estimators on the identical held-out test set reveals whether non-linear flexibility actually provides predictive utility or if it suffers from overfitting and extrapolation failure.

---

### Q2: Why is random train/test splitting invalid for traffic forecasting?
**Answer:**  
Traffic telemetry is an auto-correlated, non-stationary time series. Randomly shuffling rows into train and test sets violates the temporal sequence and causes catastrophic **lookahead data leakage**. In a random split, future observations (e.g. at 07:05 UTC) would be used to train the model to predict past observations (e.g. at 07:00 UTC). The model would simply memorize nearby temporal points rather than learning genuine causal forecasting. Chronological splitting (training on historical data up to a cutoff, testing strictly on future data) ensures real-world generalization.

---

### Q3: What is the purpose of Spark MLlib's VectorAssembler?
**Answer:**  
Apache Spark MLlib's distributed algorithms operate on resilient distributed datasets (RDDs). To maximize performance and leverage hardware-accelerated BLAS (Basic Linear Algebra Subprograms) libraries on worker nodes, Spark requires all feature dimensions to be stored in a single unified column of type `VectorUDT` (Vector User-Defined Type). `VectorAssembler` transforms individual numeric columns into a compact contiguous vector per row, minimizing serialization overhead.

---

### Q4: What is the Coefficient of Determination ($R^2$), and what does it measure?
**Answer:**  
$R^2$ is a dimensionless statistical measure of goodness-of-fit defined as:
$$R^2 = 1 - \frac{\text{SS}_{\text{res}}}{\text{SS}_{\text{tot}}} = 1 - \frac{\sum (y_i - \hat{y}_i)^2}{\sum (y_i - \bar{y})^2}$$
It quantifies the proportion of the variance in the target variable ($y$) that is explained by the model's feature vector relative to a naive baseline model that always predicts the sample mean ($\bar{y}$). An $R^2$ of 1.0 indicates perfect prediction; $R^2 = 0.0$ indicates performance equal to the sample mean.

---

### Q5: How can $R^2$ legitimately be negative? Is negative $R^2$ a programming bug?
**Answer:**  
Negative $R^2$ is not a software bug; it is a valid statistical outcome. $R^2$ is only bounded between 0 and 1 when evaluated in-sample on an OLS model fitted with an intercept. On out-of-sample held-out test data, if the sum of squared prediction residuals ($\text{SS}_{\text{res}}$) exceeds the total sum of squared deviations from the test mean ($\text{SS}_{\text{tot}}$), the ratio $\frac{\text{SS}_{\text{res}}}{\text{SS}_{\text{tot}}} > 1$, resulting in $R^2 < 0$. This occurs when the model's predictions are worse than simply predicting the constant average speed of the test dataset.

---

### Q6: Why did the Decision Tree Regressor yield a negative $R^2$ while Linear Regression scored $R^2 = 0.7757$?
**Answer:**  
This illustrates the **fundamental extrapolation failure of piecewise-constant decision trees**:
1. Decision trees partition feature space into orthogonal bounding boxes and assign a constant leaf mean. They cannot predict values higher or lower than the minimum and maximum labels observed during training.
2. In our chronological split, the training set covered midnight-to-dawn free-flow speeds (high velocities), while the test set captured morning rush-hour traffic deceleration (lower velocities).
3. The decision tree was physically incapable of extrapolating the downward velocity trend beyond what it saw in training, leading to high out-of-sample error ($\text{RMSE} = 6.79\text{ km/h}$, $R^2 = -0.2064$).
4. Conversely, Linear Regression learned a continuous linear slope with negative weights on diurnal trigonometric terms, smoothly extrapolating the downward velocity transition into the morning hours and achieving $R^2 = 0.7757$.

---

### Q7: What is the difference between RMSE and MAE, and what are their units?
**Answer:**  
Both metrics measure prediction accuracy and are expressed in **physical velocity units ($\text{km/h}$)**:
- **Mean Absolute Error (MAE):** The arithmetic average of absolute errors ($\frac{1}{n} \sum |y_i - \hat{y}_i|$). It weights all errors linearly.
- **Root Mean Squared Error (RMSE):** The square root of mean squared errors ($\sqrt{\frac{1}{n} \sum (y_i - \hat{y}_i)^2}$). Because errors are squared before averaging, RMSE penalizes large outlier errors disproportionately.
In our model, $\text{RMSE} = 2.9280\text{ km/h}$ and $\text{MAE} = 2.4733\text{ km/h}$. The closeness of these values proves that the linear model produces consistently accurate forecasts without severe outlier blunders.

---

### Q8: Why was it critical to evaluate both models on the exact same test dataset?
**Answer:**  
Fair and defensible comparative machine learning requires an identical evaluation contract. If models were evaluated on different partitions or varying sample counts, differences in metrics could stem from variance in test difficulty rather than model architecture. Both models were evaluated on the identical 100 held-out rows across all 5 zones.

---

### Q9: What is overfitting, and how was it controlled in the Decision Tree?
**Answer:**  
Overfitting occurs when an estimator memorizes noise and idiosyncratic fluctuations in the training dataset rather than learning generalizable underlying patterns, resulting in near-perfect training scores but poor test scores. In Spark MLlib's `DecisionTreeRegressor`, overfitting is controlled by constraining tree depth (`maxDepth=8`) and minimum instances per node. Without depth constraints, a tree could grow until every leaf contains a single sample, perfectly memorizing the 365 training rows.

---

### Q10: How was target leakage prevented in the Day 5 pipeline?
**Answer:**  
Target leakage was prevented at four distinct architectural checkpoints:
1. The target `next_avg_speed_kmph` was explicitly omitted from the 9-dimensional `VectorAssembler`.
2. The chronological cutoff (`2012-03-01 06:24:00 UTC`) strictly separated past features and future labels.
3. Boundary-crossing rows where the feature window fell before the cutoff but the target window fell after the cutoff were purged during Day 4.
4. Learned transformers (`StringIndexer`) were fitted exclusively on training data.

---

### Q11: What does the $R^2 > 0.75$ success threshold mean in the Urbanix project?
**Answer:**  
In the Urbanix 12-Day Roadmap, the $R^2 > 0.75$ milestone is an architectural quality gate. It requires that an estimator explain more than 75% of velocity variance on unseen future telemetry before it is approved for deployment into Day 6's live streaming inference engine. Linear Regression achieved $R^2 = 0.7757$, successfully meeting this criterion.

---

### Q12: Why are per-zone evaluations necessary when you already have aggregate metrics?
**Answer:**  
Aggregate metrics average out performance across the entire city, potentially masking severe spatial failure modes. For example, a model could perform exceptionally well on open highways but fail catastrophically in dense commercial centers. Our per-zone audit confirmed that Linear Regression maintains high accuracy across all 5 municipal zones, ranging from $R^2 = 0.6109$ in arterial corridors (`zone_2`) to $R^2 = 0.8390$ on coastal ring roads (`zone_5`).

---

### Q13: What caused the Cholesky solver warning during Linear Regression training?
**Answer:**  
The warning `Cholesky solver failed due to singular covariance matrix` occurred because of exact multicollinearity: `speed_spread_kmph = max_speed_kmph - min_speed_kmph`. This creates a linearly dependent column in $\mathbf{X}$, making $\mathbf{X}^T \mathbf{X}$ non-invertible. Spark MLlib automatically detected this singularity and fell back to the Quasi-Newton (L-BFGS) optimizer, which robustly solved for the minimum-norm coefficient weights.

---

### Q14: Does an $R^2$ of 0.7757 prove that this model will generalize to other cities?
**Answer:**  
No. In rigorous academic research, claims of generalization must match empirical scope. Our model was trained on Los Angeles County (METR-LA) telemetry from March 1, 2012. While an $R^2$ of 0.7757 proves strong out-of-sample generalization to the next time horizon within this specific urban network, transferring this model to another city or a different season would require retraining on localized spatial topology and seasonal traffic patterns.

---

### Q15: What will Day 6 do with the saved models?
**Answer:**  
Day 6 implements **Live MLlib Streaming Inference and Checkpoint Recovery**:
1. It will deserialize the fitted `LinearRegressionModel` from `models/linear_regression/`.
2. It will integrate the model into a Spark Structured Streaming query reading live 5-minute windowed traffic events from Kafka.
3. For every incoming micro-batch, it will transform the feature vector into real-time speed forecasts ($\hat{y}_{t+1}$) and route downstream alerts when predicted speeds indicate imminent traffic congestion.

---

## 10. Day 5 Definition of Done Checklist

| Item | Requirement | Outcome / Observed Status | Final Verification |
| :---: | :--- | :--- | :---: |
| 1 | Actual repository and Day 4 inputs inspected | Verified branch `main`, commit `7c5e368`, clean tree | **PASS** |
| 2 | Existing chronological split re-verified | 365 train rows, 100 test rows, 0 overlap, 0 leakage | **PASS** |
| 3 | Day 4 preparation logic preserved | `prepare_training_data.py` untouched; verified against split | **PASS** |
| 4 | Linear Regression trained on `train_df` | Fitted strictly on 365 training rows | **PASS** |
| 5 | Decision Tree Regressor trained on `train_df`| Fitted on identical 365 training rows (`maxDepth=8`) | **PASS** |
| 6 | Both models evaluated on identical `test_df` | Evaluated on identical 100 held-out rows | **PASS** |
| 7 | Aggregate $R^2$, RMSE, and MAE recorded | LR: $R^2=0.7757$, $\text{RMSE}=2.93$, $\text{MAE}=2.47$; DT: $R^2=-0.21$ | **PASS** |
| 8 | Per-zone metrics and sample counts recorded | Exactly 20 samples per zone across `zone_1` to `zone_5` | **PASS** |
| 9 | Undefined metrics & invalid predictions handled | Zero nulls/NaNs; constant-label fallback implemented | **PASS** |
| 10 | Suspiciously high $R^2$ investigated | $R^2 = 0.7757 \le 0.98$; no leakage or memorization | **PASS** |
| 11 | Success threshold ($R^2 > 0.75$) assessed honestly| Linear Regression meets threshold ($0.7757 > 0.75$) | **PASS** |
| 12 | Both models saved via native MLlib persistence | Persisted to `models/linear_regression` and `decision_tree` | **PASS** |
| 13 | Both models reloaded & independently verified | `verify_models.py` passed with parity $\le 3.6 \times 10^{-5}$ | **PASS** |
| 14 | Machine-readable results generated | `reports/day-05/metrics.json` and `per_zone_metrics.json` | **PASS** |
| 15 | Human-readable results report generated | `reports/day-05/results.md` and `verification.md` | **PASS** |
| 16 | Relevant automated tests executed | 9/9 passed in `test_model_training.py`; 53 total in repo | **PASS** |
| 17 | Saved artifact and metric consistency checked | Reloaded predictions match persisted metrics exactly | **PASS** |
| 18 | Viva questions and teaching notes completed | 15 detailed Q&A pairs covering theory, code, and defense | **PASS** |
| 19 | Git diff and staged changes reviewed | Staged clean code, tests, docs, and gitkeep | **PASS** |
| 20 | Conventional Commits created | Small, logical commits (`feat:`, `test:`, `docs:`) | **PASS** |
| 21 | Remote push verified | Pushed cleanly to `origin/main` | **PASS** |
| 22 | Day 5 handoff report prepared | Complete Sections A to N ready for ChatGPT | **PASS** |

---

## 11. Academic Reporting Paragraph (For Capstone Thesis / Conference Paper)

> *In the predictive modeling stage, we benchmarked an Ordinary Least Squares (OLS) Linear Regression model against a Decision Tree Regressor ($\text{maxDepth}=8$) on 465 chronologically partitioned 5-minute tumbling window traffic feature vectors across five Los Angeles County municipal zones. Estimators were trained strictly on historical telemetry ($n=365$, prior to the 06:24:00 UTC cutoff) and evaluated on 100 held-out morning rush-hour observations ($n=20$ per zone). Linear Regression achieved strong out-of-sample predictive performance, surpassing the architectural milestone of $R^2 > 0.75$ with an aggregate $R^2$ of $0.7757$, an RMSE of $2.9280\text{ km/h}$, and an MAE of $2.4733\text{ km/h}$. Per-zone analysis confirmed consistent generalization across both urban arterial corridors ($R^2 \ge 0.61$) and outer highway rings ($R^2 \ge 0.83$). Conversely, the Decision Tree Regressor yielded a negative coefficient of determination ($R^2 = -0.2064$, $\text{RMSE} = 6.7904\text{ km/h}$), empirically demonstrating the inability of piecewise-constant tree estimators to extrapolate unseen deceleration trends across non-stationary temporal regime shifts. Consequently, the fitted Linear Regression model was persisted via Apache Spark MLlib native serialization as the primary forecasting engine for real-time streaming inference.*
