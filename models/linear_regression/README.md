# 📈 Model A: Spark MLlib Linear Regression (`models/linear_regression/`)

![Status](https://img.shields.io/badge/Status-Approved%20for%20Production%20(Day%205)-brightgreen)
![Model Type](https://img.shields.io/badge/Estimator-pyspark.ml.regression.LinearRegression-blue)
![Quality Gate](https://img.shields.io/badge/R%C2%B2_Score-0.7757_(Passed_%3E_0.75)-brightgreen)
![Accuracy](https://img.shields.io/badge/RMSE-2.9280_km%2Fh-orange)
![Accuracy](https://img.shields.io/badge/MAE-2.4733_km%2Fh-purple)

This directory contains the serialized native Apache Spark MLlib **Linear Regression Model** chosen as the primary velocity forecasting engine for Day 6 live streaming inference.

---

## 🏛️ 1. Model Profile & Architecture

- **Estimator Class:** `pyspark.ml.regression.LinearRegression`
- **Trained Model Class:** `pyspark.ml.regression.LinearRegressionModel`
- **Training Samples:** 365 rows (`data/train/`, historical telemetry before `2012-03-01 06:20:00 UTC`).
- **Evaluation Samples:** 100 rows (`data/test/`, held-out morning rush-hour observations).
- **Optimization Algorithm:** Quasi-Newton (L-BFGS) with line-search zoom (automatically selected by Spark upon detecting singular covariance from feature collinearity).
- **Hyperparameters:**
  - `regParam = 0.0` (Unconstrained Ordinary Least Squares)
  - `elasticNetParam = 0.0` (L2/L1 mixing)
  - `fitIntercept = True`
  - `standardization = True`

---

## 📐 2. Learned Mathematical Parameters

$$\hat{y}(\mathbf{x}) = 44.0640 + \sum_{j=0}^8 w_j x_j$$

| Feature Index | Feature Name | Learned Weight ($w_j$) | Domain Interpretation |
| :---: | :--- | :---: | :--- |
| **0** | `avg_speed_kmph` | **+0.7591** | Dominant autoregressive momentum. Every +10 km/h in current speed predicts +7.59 km/h in next window. |
| **1** | `speed_spread_kmph` | **+0.0117** | Slight positive effect of speed variance within the window. |
| **2** | `min_speed_kmph` | **-0.1205** | Negative dampener representing localized bottleneck shockwaves. |
| **3** | `max_speed_kmph` | **-0.0129** | Free-flow upper boundary adjustment. |
| **4** | `reading_count` | **+0.0177** | Sensor volume density adjustment. |
| **5** | `sin_hour` | **-1.9880** | Diurnal harmonic term: captures morning peak deceleration. |
| **6** | `cos_hour` | **+4.4857** | Diurnal phase offset distinguishing early morning from midday. |
| **7** | `zone_index` | **-7.4699** | Spatial baseline offset across municipal corridors. |
| **8** | `speed_lag_1` | **-0.1790** | Second-order temporal momentum correction. |

---

## 📊 3. Granular Evaluation Metrics

- **Aggregate Held-Out Metrics (100 samples):**
  - **$R^2$:** `0.7757` (Roadmap milestone $R^2 > 0.75$: **PASSED**)
  - **RMSE:** `2.9280 km/h`
  - **MAE:** `2.4733 km/h`
- **Per-Zone Evaluation Breakdown ($n=20$ per zone):**
  - `zone_1` (Downtown Core): $R^2 = 0.6622$, $\text{RMSE} = 3.6885\text{ km/h}$, $\text{MAE} = 3.2586\text{ km/h}$
  - `zone_2` (San Fernando Valley): $R^2 = 0.6109$, $\text{RMSE} = 2.5649\text{ km/h}$, $\text{MAE} = 2.3580\text{ km/h}$
  - `zone_3` (San Gabriel Foothills): $R^2 = 0.8278$, $\text{RMSE} = 2.0458\text{ km/h}$, $\text{MAE} = 1.6137\text{ km/h}$
  - `zone_4` (South Central Corridor): $R^2 = 0.6674$, $\text{RMSE} = 3.3106\text{ km/h}$, $\text{MAE} = 3.0507\text{ km/h}$
  - `zone_5` (Harbor & Outer Ring): $R^2 = 0.8390$, $\text{RMSE} = 2.7456\text{ km/h}$, $\text{MAE} = 2.0856\text{ km/h}$

---

## 💻 4. Programmatic Deserialization in PySpark

To load and use this model in Day 6 live streaming inference:
```python
from pyspark.ml.regression import LinearRegressionModel

# Load serialized model from disk
model = LinearRegressionModel.load("models/linear_regression")

# Predict on streaming DataFrame with 9-dimensional 'features' vector
predictions_df = model.transform(streaming_feature_df)
# Output column 'prediction' contains predicted next_avg_speed_kmph
```
