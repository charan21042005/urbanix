# Urbanix — Day 5: MLlib Regressor Training, Evaluation & Reproducibility

Welcome to the **Day 5 Module Documentation** for Project Urbanix.

## Milestone Summary
- **Module:** Weekend 2 — Distributed Analytics & Graph Engine (Day 5 of 12)
- **Primary Language:** Python (PySpark)
- **Core Engine:** Apache Spark MLlib 3.5.0 (`LinearRegression`, `DecisionTreeRegressor`, `RegressionEvaluator`)
- **Key Deliverables:** Supervised regression model training on 365 train windows, evaluation on 100 held-out test windows, Linear Regression success ($R^2 = 0.7757 > 0.75$, $\text{RMSE} = 2.9280\text{ km/h}$), Decision Tree extrapolation failure analysis ($R^2 = -0.2064$), native model serialization under `models/linear_regression/`
- **Status:** **VERIFIED & FORMALLY CLOSED** (All Acceptance Gates Passed)
- **Total Tests:** 9 Passing Automated Tests in `services/streaming/tests/test_model_training.py`

## Directory Contents
- [DAY_05_ML_FORECASTING_REPORT.md](DAY_05_ML_FORECASTING_REPORT.md) — Exhaustive 1,022-line pedagogical compendium and academic viva defense manual covering Ordinary Least Squares matrix calculus, L-BFGS Quasi-Newton solvers, variance reduction splitting, piecewise-constant extrapolation failure, and 25 viva questions.
- [../../services/streaming/train_evaluate.py](../../services/streaming/train_evaluate.py) — Distributed model training and evaluation script.
- [../../services/streaming/verify_models.py](../../services/streaming/verify_models.py) — Independent model artifact and performance verifier (12 checks).
- [../../models/linear_regression/](../../models/linear_regression/) — Persisted production Linear Regression model artifact.
- [../../reports/day-05/](../../reports/day-05/) — Formal metric reports (`metrics.json`, `per_zone_metrics.json`, `results.md`, `verification.md`).

## Quick Execution Commands
```powershell
# Navigate to streaming directory
cd services/streaming

# 1. Run automated model training test suite (9 tests)
python -m pytest tests/test_model_training.py -v

# 2. Run independent model artifact verifier (12 checks)
python verify_models.py
```
