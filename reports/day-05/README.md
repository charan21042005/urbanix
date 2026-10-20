# 📑 Day 5 Evaluation Reports & Provenance Dossier (`reports/day-05/`)

![Milestone](https://img.shields.io/badge/Milestone-Day_5_MLlib_Forecasting-brightgreen)
![Status](https://img.shields.io/badge/Audit-All_Checks_Passed-brightgreen)
![Linear Regression](https://img.shields.io/badge/Linear_Regression-R%C2%B2_%3D_0.7757-blue)
![Decision Tree](https://img.shields.io/badge/Decision_Tree-R%C2%B2_%3D_-0.2064-red)

This directory contains the authoritative machine-readable and human-readable evaluation outputs produced by the Day 5 machine learning training pipeline (`services/streaming/train_evaluate.py`).

---

## 📁 1. Artifact Inventory

| File Name | Format | Role & Contents |
| :--- | :---: | :--- |
| [`metrics.json`](metrics.json) | JSON | Complete machine-readable manifest: execution timestamp, data paths, row counts, feature names, hyperparameters, learned weights/nodes, aggregate metrics, per-zone scores, and threshold pass/fail flags. |
| [`per_zone_metrics.json`](per_zone_metrics.json) | JSON | Isolated granular breakdown of $R^2$, RMSE, and MAE across all 5 municipal zones for both models. |
| [`results.md`](results.md) | Markdown | Formatted academic report containing executive summaries, aggregate model comparison tables, and per-zone performance matrices. |
| [`verification.md`](verification.md) | Markdown | Formal post-training verification audit checklist recording PASS/FAIL outcomes across 10 integrity checks. |

---

## 📊 2. Key Measured Results Summary

### Aggregate Model Performance (100 Held-Out Rush-Hour Test Rows)
- **Linear Regression (OLS):**
  - **$R^2$:** `+0.7757` *(Roadmap milestone $R^2 > 0.75$: **PASSED**)*
  - **RMSE:** `2.9280 km/h`
  - **MAE:** `2.4733 km/h`
  - **Decision:** **Selected as primary live forecasting engine for Day 6 streaming inference.**
- **Decision Tree Regressor ($d=8$):**
  - **$R^2$:** `-0.2064` *(Roadmap milestone $R^2 > 0.75$: **FAILED**)*
  - **RMSE:** `6.7904 km/h`
  - **MAE:** `5.1781 km/h`
  - **Decision:** Documented as an empirical demonstration of the extrapolation breakdown of piecewise-constant tree models under chronological temporal regime shifts.

---

## 🔬 3. How to Regenerate or Verify These Reports

```powershell
# 1. Regenerate all report files by running model training:
python services/streaming/train_evaluate.py

# 2. Independently verify reloaded model predictions against metrics.json:
python services/streaming/verify_models.py
```
