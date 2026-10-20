# Urbanix Day 5 — Model Training & Evaluation Results

**Evaluation Date (UTC):** 2026-10-20T09:50:54.437800+00:00  
**Training Dataset:** `data/train` (365 rows, 80% chronological split up to `2012-03-01 06:20:00 UTC`)  
**Testing Dataset:** `data/test` (100 rows, held-out split starting `2012-03-01 06:25:00 UTC`)  
**Target Variable:** `next_avg_speed_kmph` (observed speed in the next consecutive 5-minute interval)  
**Feature Space:** 9-dimensional Spark ML `VectorUDT` (assembled from rolling window aggregations, lag velocity, cyclical hour features, and zone indices).

---

## 1. Aggregate Model Performance Comparison

Evaluated on the exact 100 held-out test rows across all 5 municipal zones.

| Model Architecture | R² Score | RMSE (km/h) | MAE (km/h) | Test Count | Success Threshold (R² > 0.75) | Selected for Day 6? |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Linear Regression (OLS)** | **0.7757** | **2.9280** | **2.4733** | **100** | **PASSED (0.7757 > 0.75)** | **YES (Primary Live Model)** |
| **Decision Tree Regressor (d=8)** | -0.2064 | 6.7904 | 5.1781 | 100 | FAILED (-0.2064 < 0.75) | NO (Baseline / Comparative) |

> **Key Finding:** Linear Regression successfully surpasses the project roadmap milestone of **R² > 0.75** with **R² = 0.7757**, an RMSE of **2.9280 km/h**, and an MAE of **2.4733 km/h**. Decision Tree Regressor yields a negative R² under strict chronological splitting due to its piecewise-constant nature being unable to extrapolate morning congestion drops beyond the free-flow training regime.

---

## 2. Granular Per-Zone Performance Comparison

Each municipal zone contains exactly 20 consecutive 5-minute test observations during the morning rush-hour progression (06:25 to 08:00 UTC).

| Zone Identifier | Zone Description | Samples ($n$) | Linear Regression R² | Linear Regression RMSE (km/h) | Linear Regression MAE (km/h) | Decision Tree R² | Decision Tree RMSE (km/h) | Decision Tree MAE (km/h) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `zone_1` | Downtown Core / Central Business District | 20 | **0.6622** | **3.6885** | **3.2586** | -0.7874 | 8.4841 | 6.8355 |
| `zone_2` | San Fernando Valley Arterial Corridor | 20 | **0.6109** | **2.5649** | **2.3580** | -0.2892 | 4.6686 | 3.9805 |
| `zone_3` | San Gabriel Foothills / Eastern Highway | 20 | **0.8278** | **2.0458** | **1.6137** | 0.3422 | 3.9982 | 3.1002 |
| `zone_4` | South Central Freight / Transit Corridor | 20 | **0.6674** | **3.3106** | **3.0507** | -1.4896 | 9.0579 | 7.3130 |
| `zone_5` | Harbor & Outer Coastal Ring Road | 20 | **0.8390** | **2.7456** | **2.0856** | 0.1726 | 6.2241 | 4.6612 |

---

## 3. Persisted Artifacts

1. **Linear Regression Model:** `models/linear_regression/`
2. **Decision Tree Regressor Model:** `models/decision_tree/`
3. **Machine-Readable Metrics:** `reports/day-05/metrics.json`
4. **Per-Zone Metrics:** `reports/day-05/per_zone_metrics.json`
