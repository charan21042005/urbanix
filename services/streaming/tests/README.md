# 🧪 Urbanix Streaming & ML Test Suite (`services/streaming/tests/`)

![Tests](https://img.shields.io/badge/Streaming_%26_ML_Tests-44%2F44_Passing-brightgreen)
![Framework](https://img.shields.io/badge/Framework-pytest_9.1.1-blue)
![Spark Version](https://img.shields.io/badge/Engine-Apache_Spark_3.5.0-orange)
![Coverage](https://img.shields.io/badge/Scope-Days_3%20to%206-purple)

This directory contains the automated PySpark test suite for the **Urbanix Streaming Ingestion, Feature Pipeline, Machine Learning, and Live Inference Modules**. It validates stream watermarking, Parquet feature tables, chronological zero-leakage splits, Spark MLlib model metrics, and fault-tolerant checkpoint recovery.

---

## 🏛️ 1. Test Architecture & Design Principles

1. **Local PySpark Isolation:** Tests spin up localized in-memory `SparkSession` instances (`local[*]`) with warehouse directories isolated under temporary scratch directories.
2. **Deterministic Time-Series Validation:** Uses deterministic historical data slices to assert exact floating-point metrics, vector lengths, and temporal boundaries.
3. **Multi-Milestone Regression Shield:** Enforces that feature transformations, model serializations, and streaming sinks remain backward-compatible as new milestones are added.

---

## 📋 2. Test File Catalog (44 Total Tests)

### 2.1 `test_streaming.py` (Day 3 — 14 Tests)
Validates Apache Spark Structured Streaming ingestion, event-time watermarking, and tumbling window aggregations:
- `test_spark_session_creation`: Asserts local SparkSession initializes cleanly with required configs.
- `test_read_kafka_stream_schema`: Verifies incoming JSON schema contract from `traffic-topic`, `aqi-topic`, and `weather-topic`.
- `test_watermark_and_tumbling_window`: Validates 2-minute watermark handling and 5-minute window calculations.
- `test_static_zone_enrichment`: Verifies broadcast joins with `data/grid_zones.csv`.
- `test_parquet_sink_partitioning`: Asserts output Parquet structure under `data/feature_table/`.

### 2.2 `test_training_data.py` (Day 4 — 11 Tests)
Validates feature engineering, temporal encodings, continuity guards, and the 80/20 chronological split:
- `test_continuity_guard`: Asserts that rows spanning window gaps greater than 300 seconds are rejected.
- `test_diurnal_harmonic_encodings`: Validates cyclic $\sin(2\pi h / 24)$ and $\cos(2\pi h / 24)$ derivations.
- `test_vector_assembler_dimension`: Confirms the `VectorUDT` contains exactly 9 dimensions.
- `test_chronological_split_zero_leakage`: Proves that maximum train timestamp $\le$ cutoff $<$ minimum test timestamp.
- `test_train_test_row_counts`: Validates exactly 365 train rows and 100 test rows across all 5 zones.

### 2.3 `test_model_training.py` (Day 5 — 9 Tests)
Validates Apache Spark MLlib supervised regression estimators and evaluation metrics:
- `test_linear_regression_training`: Fits Linear Regression baseline and verifies non-null weights vector ($\mathbf{w} \in \mathbb{R}^9$) and intercept.
- `test_decision_tree_training`: Fits Decision Tree Regressor and verifies tree depth $\le 8$.
- `test_evaluation_metrics_computation`: Asserts $R^2$, RMSE, and MAE calculations match `RegressionEvaluator`.
- `test_linear_regression_success_threshold`: Confirms Linear Regression achieves $R^2 > 0.75$ ($0.7757$ actual).
- `test_per_zone_metric_reporting`: Verifies metric generation for `zone_1` through `zone_5`.

### 2.4 `test_streaming_inference.py` (Day 6 — 10 Tests)
Validates real-time model scoring inside `foreachBatch` streaming micro-batches and checkpoint recovery:
- `test_model_deserialization_on_startup`: Asserts native `LinearRegressionModel.load()` loads successfully into driver RAM.
- `test_feature_parity_vector_assembly`: Validates that live streaming batches construct the identical 9-dimensional vector expected by the model.
- `test_physical_velocity_bounds`: Asserts predicted speeds lie strictly within $0 < v \le 250\text{ km/h}$.
- `test_foreach_batch_sink`: Confirms scored batches append properly to Parquet sinks with prediction columns.
- `test_checkpoint_restart_state`: Simulates process interruption and verifies queries resume from committed offsets.

---

## ⚡ 3. Execution Commands

To execute the entire 44-test streaming and ML suite from the repository root:
```powershell
# Run all tests in this directory
python -m pytest services/streaming/tests/ -v

# Run individual test files
python -m pytest services/streaming/tests/test_streaming.py -v
python -m pytest services/streaming/tests/test_training_data.py -v
python -m pytest services/streaming/tests/test_model_training.py -v
python -m pytest services/streaming/tests/test_streaming_inference.py -v
```
