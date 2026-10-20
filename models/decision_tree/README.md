# 🌲 Model B: Spark MLlib Decision Tree Regressor (`models/decision_tree/`)

![Status](https://img.shields.io/badge/Status-Comparative%20Baseline%20(Day%205)-yellow)
![Model Type](https://img.shields.io/badge/Estimator-pyspark.ml.regression.DecisionTreeRegressor-blue)
![Quality Gate](https://img.shields.io/badge/R%C2%B2_Score--0.2064_(Failed_%3C_0.75)-red)
![Accuracy](https://img.shields.io/badge/RMSE-6.7904_km%2Fh-orange)
![Accuracy](https://img.shields.io/badge/MAE-5.1781_km%2Fh-purple)

This directory contains the serialized native Apache Spark MLlib **Decision Tree Regression Model** trained as a non-linear comparative benchmark for traffic velocity forecasting.

---

## 🏛️ 1. Model Profile & Architecture

- **Estimator Class:** `pyspark.ml.regression.DecisionTreeRegressor`
- **Trained Model Class:** `pyspark.ml.regression.DecisionTreeRegressionModel`
- **Training Samples:** 365 rows (`data/train/`).
- **Evaluation Samples:** 100 rows (`data/test/`).
- **Hyperparameters:**
  - `maxDepth = 8`
  - `minInstancesPerNode = 1`
  - `seed = 42`
- **Learned Topology:** Depth = 8, Total Nodes = 307 (154 decision splits, 153 terminal leaves).

---

## 📊 2. Feature Importances (Variance Reduction Contribution)

```
avg_speed_kmph      : 0.8704 (87.04%) - Dominant top-level root splits
min_speed_kmph      : 0.0361 ( 3.61%) - Secondary bottleneck checks
max_speed_kmph      : 0.0251 ( 2.51%) - Upper speed limits
reading_count       : 0.0239 ( 2.39%) - Traffic density checks
speed_spread_kmph   : 0.0222 ( 2.22%) - Velocity spread
zone_index          | 0.0107 ( 1.07%) - Geographic corridor branches
sin_hour            : 0.0068 ( 0.68%) - Diurnal time partition
speed_lag_1         : 0.0047 ( 0.47%) - Lag momentum
cos_hour            : 0.0000 ( 0.00%) - Unused in optimal partition
```

---

## 📉 3. Performance Summary & The Extrapolation Breakdown

- **Aggregate Held-Out Metrics (100 samples):**
  - **$R^2$:** `-0.2064` (Roadmap milestone $R^2 > 0.75$: **FAILED**)
  - **RMSE:** `6.7904 km/h`
  - **MAE:** `5.1781 km/h`

### Why Did the Tree Produce Negative $R^2$?
Decision trees are **piecewise constant estimators**. Their predictions are strictly bounded by the minimum and maximum label values observed in the training leaves ($\min y_{\text{train}} \le \hat{y} \le \max y_{\text{train}}$) and have zero gradient ($\nabla_{\mathbf{x}} \hat{y} = \mathbf{0}$).
Because the training set covers midnight-to-dawn free-flow speeds ($55\text{--}65\text{ km/h}$) and the held-out test set covers morning rush-hour deceleration ($35\text{--}45\text{ km/h}$), the tree is **physically incapable of extrapolating downward trends**.
Its residual error exceeds the sample variance of the test set ($\text{SS}_{\text{res}} > \text{SS}_{\text{tot}}$), resulting mathematically in $R^2 = 1 - \frac{4610.95}{3822.21} = -0.2064$.

---

## 💻 4. Programmatic Deserialization in PySpark

```python
from pyspark.ml.regression import DecisionTreeRegressionModel

# Load serialized model from disk
dt_model = DecisionTreeRegressionModel.load("models/decision_tree")
print("Tree Depth:", dt_model.depth)
print("Node Count:", dt_model.numNodes)
```
