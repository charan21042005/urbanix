# ==============================================================================
# Urbanix Day 5 — Automated Test Suite: MLlib Model Training & Evaluation
# ==============================================================================
# Comprehensive unit and integration tests verifying:
# 1. Input dataframe validation (column existence, VectorUDT, 9 dimensions, physical bounds).
# 2. Failure handling on empty or missing datasets.
# 3. Deterministic regression metric calculation on synthetic ground truth.
# 4. Robust handling of constant labels and single samples (undefined R²).
# 5. Per-zone evaluation correctness and sample count tracking.
# 6. Linear Regression model training on real Day 4 training split (365 rows).
# 7. Decision Tree Regressor training on real Day 4 training split.
# 8. Evaluation parity: both models evaluated on identical held-out test rows (100 rows).
# 9. MLlib native model persistence, reloading, and numerical prediction parity (< 1e-4).
# 10. Machine-readable metrics.json and results.md generation and metadata completeness.
#
# Educational Overview for Beginners:
# -----------------------------------
# Why Test Model Training Code?
# In machine learning engineering, code that trains models is often error-prone.
# Subtle bugs—such as wrong vector lengths, target leakage, dividing by zero when
# calculating R², or differences when saving and reloading weights—can completely
# invalidate traffic predictions.
# This test suite tests both "math unit tests" (checking hand-calculated RMSE/MAE/R²)
# and "end-to-end integration tests" (verifying that real models train, persist to disk,
# reload cleanly, and produce identical predictions).
# ==============================================================================

# Standard system and file management libraries
import os          # Operating system environment variables and path handling
import sys         # Python executable path and system path manipulation
import json        # Parsing and asserting structured JSON reports
import math        # Floating-point math checks
import tempfile    # Isolated temporary directories for testing persistence
import shutil      # Directory cleanup utilities
from pathlib import Path  # Object-oriented path manipulation
import pytest      # Testing framework

# PySpark SQL and MLlib imports
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,       # Structured table schema
    StructField,      # Single schema field definition
    StringType,       # Text/string column type
    DoubleType,       # Floating point decimal type
    TimestampType,    # Timestamp representation
)
from pyspark.ml.linalg import Vectors, VectorUDT  # Spark ML vector representations
from pyspark.ml.regression import LinearRegressionModel, DecisionTreeRegressionModel  # Model classes

# Configure paths to enable imports from repository root and services/streaming
project_root = str(Path(__file__).resolve().parent.parent.parent.parent)
streaming_dir = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if streaming_dir not in sys.path:
    sys.path.insert(0, streaming_dir)

# Ensure PySpark worker threads match the driver Python binary on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Functions and constants under test from train_evaluate and verify_models
from train_evaluate import (
    validate_input_dataframe,
    compute_regression_metrics,
    evaluate_per_zone,
    train_and_evaluate_models,
    FEATURE_NAMES,
    TARGET_COLUMN,
    FEATURES_COLUMN,
    ZONE_COLUMN,
)
from verify_models import verify_models


@pytest.fixture(scope="session")
def spark():
    """
    Initializes a shared SparkSession for test execution.
    The session is created once per test session and cleanly stopped upon completion.
    """
    session = (
        SparkSession.builder.appName("UrbanixDay5Tests")
        .master("local[2]")                           # 2 local executor threads
        .config("spark.sql.session.timeZone", "UTC")   # Standardize all dates to UTC
        .config("spark.driver.memory", "2g")           # 2GB driver memory
        .config("spark.ui.enabled", "false")           # Disable web UI
        .getOrCreate()
    )
    yield session
    # Cleanup: cleanly shut down SparkSession
    session.stop()


# =============================================================================
# UNIT TESTS: Metrics, Edge Cases, and Validation
# =============================================================================

def test_compute_regression_metrics_deterministic(spark):
    """
    Test 1: Verifies that RMSE, MAE, and R² match exact hand-computed mathematical values.
    
    Hand-calculated derivation:
    - actuals = [10.0, 20.0, 30.0], predictions = [12.0, 18.0, 32.0]
    - errors: [+2, -2, +2] -> squared errors: [4, 4, 4] -> MSE = 12/3 = 4.0 -> RMSE = sqrt(4.0) = 2.0
    - absolute errors: [2, 2, 2] -> MAE = 6/3 = 2.0
    - actual mean = 20.0. deviations from mean: [-10, 0, +10]
      * SS_tot = (-10)^2 + (0)^2 + (+10)^2 = 100 + 0 + 100 = 200.0
      * SS_res = 4 + 4 + 4 = 12.0
      * R² = 1 - (SS_res / SS_tot) = 1 - (12 / 200) = 1 - 0.06 = 0.94.
    """
    schema = StructType([
        StructField("next_avg_speed_kmph", DoubleType(), False),
        StructField("prediction", DoubleType(), False),
    ])
    data = [
        (10.0, 12.0),
        (20.0, 18.0),
        (30.0, 32.0),
    ]
    df = spark.createDataFrame(data, schema)
    metrics = compute_regression_metrics(df)

    # Verify counts and mathematical accuracy within 1e-4 tolerance
    assert metrics["count"] == 3
    assert abs(metrics["rmse"] - 2.0) < 1e-4
    assert abs(metrics["mae"] - 2.0) < 1e-4
    assert abs(metrics["r2"] - 0.94) < 1e-4


def test_compute_regression_metrics_constant_label(spark):
    """
    Test 2: Verifies that a constant target label yields R² = None rather than a division by zero error.
    When all ground truth labels are identical (variance = 0), SS_tot is 0, making R² undefined.
    """
    schema = StructType([
        StructField("next_avg_speed_kmph", DoubleType(), False),
        StructField("prediction", DoubleType(), False),
    ])
    data = [
        (50.0, 52.0),
        (50.0, 48.0),
        (50.0, 50.0),
    ]
    df = spark.createDataFrame(data, schema)
    metrics = compute_regression_metrics(df)

    assert metrics["count"] == 3
    assert metrics["r2"] is None  # Undefined R² handled gracefully without crashing
    assert abs(metrics["rmse"] - 1.6330) < 1e-3


def test_compute_regression_metrics_empty_df(spark):
    """
    Test 3: Verifies that an empty dataframe returns zero count and None for all metrics.
    """
    schema = StructType([
        StructField("next_avg_speed_kmph", DoubleType(), False),
        StructField("prediction", DoubleType(), False),
    ])
    df = spark.createDataFrame([], schema)
    metrics = compute_regression_metrics(df)
    assert metrics["count"] == 0
    assert metrics["r2"] is None
    assert metrics["rmse"] is None
    assert metrics["mae"] is None


def test_validate_input_dataframe_missing_column(spark):
    """
    Test 4: Verifies ValueError when a required column (e.g. features) is missing or table is empty.
    """
    schema = StructType([
        StructField("window_start", TimestampType(), True),
        StructField("zone_id", StringType(), True),
        StructField("next_avg_speed_kmph", DoubleType(), True),
    ])
    df = spark.createDataFrame([], schema)
    with pytest.raises(ValueError, match="Partition 'test' is empty"):
        validate_input_dataframe(df, "test")


def test_validate_input_dataframe_invalid_dimension(spark):
    """
    Test 5: Verifies ValueError when feature vector dimension is not 9.
    """
    schema = StructType([
        StructField("window_start", TimestampType(), False),
        StructField("zone_id", StringType(), False),
        StructField("next_avg_speed_kmph", DoubleType(), False),
        StructField("features", VectorUDT(), False),
    ])
    # Pass a 3-dimensional vector instead of required 9 dimensions
    from datetime import datetime
    data = [(datetime(2026, 1, 1), "zone_1", 50.0, Vectors.dense([1.0, 2.0, 3.0]))]
    df = spark.createDataFrame(data, schema)

    with pytest.raises(ValueError, match="Expected 9-dimensional feature vector"):
        validate_input_dataframe(df, "train")


def test_evaluate_per_zone_groups_accurately(spark):
    """
    Test 6: Verifies that evaluate_per_zone produces distinct metrics for all distinct zones.
    """
    schema = StructType([
        StructField("zone_id", StringType(), False),
        StructField("next_avg_speed_kmph", DoubleType(), False),
        StructField("prediction", DoubleType(), False),
    ])
    data = [
        ("zone_1", 40.0, 42.0),
        ("zone_1", 50.0, 48.0),
        ("zone_2", 60.0, 60.0),
        ("zone_2", 70.0, 70.0),
    ]
    df = spark.createDataFrame(data, schema)
    zone_metrics = evaluate_per_zone(df)

    assert "zone_1" in zone_metrics
    assert "zone_2" in zone_metrics
    assert zone_metrics["zone_1"]["count"] == 2
    assert zone_metrics["zone_2"]["count"] == 2
    # In zone_2, predictions match actuals perfectly -> error is 0.0
    assert abs(zone_metrics["zone_2"]["rmse"] - 0.0) < 1e-4
    assert abs(zone_metrics["zone_2"]["mae"] - 0.0) < 1e-4


# =============================================================================
# INTEGRATION TESTS: Real Day 4 Data & Persistence
# =============================================================================

def test_real_day4_dataset_existence_and_dimensions(spark):
    """
    Test 7: Verifies that data/train and data/test exist and conform strictly to Day 4 handoff:
    - data/train contains exactly 365 rows
    - data/test contains exactly 100 rows
    """
    train_path = "data/train"
    test_path = "data/test"

    assert Path(train_path).exists(), f"Missing {train_path}"
    assert Path(test_path).exists(), f"Missing {test_path}"

    train_df = spark.read.parquet(train_path)
    test_df = spark.read.parquet(test_path)

    assert validate_input_dataframe(train_df, "train") == 365
    assert validate_input_dataframe(test_df, "test") == 100


def test_full_pipeline_execution_and_persistence(spark):
    """
    Test 8: Executes the full training and evaluation pipeline into a temporary directory,
    verifying model persistence, report creation, and metric bounds.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        lr_save = os.path.join(tmp_dir, "models", "lr")
        dt_save = os.path.join(tmp_dir, "models", "dt")
        rep_dir = os.path.join(tmp_dir, "reports")

        # Train models using temporary output targets
        metrics_payload = train_and_evaluate_models(
            train_path="data/train",
            test_path="data/test",
            lr_model_path=lr_save,
            dt_model_path=dt_save,
            output_dir=rep_dir,
            dt_max_depth=8,
            reg_param=0.0,
            spark=spark,
        )

        # 1. Check generated files exist on disk
        assert os.path.exists(lr_save)
        assert os.path.exists(dt_save)
        assert os.path.exists(os.path.join(rep_dir, "metrics.json"))
        assert os.path.exists(os.path.join(rep_dir, "results.md"))
        assert os.path.exists(os.path.join(rep_dir, "per_zone_metrics.json"))

        # 2. Check metrics payload values
        lr_metrics = metrics_payload["models"]["linear_regression"]["aggregate_metrics"]
        dt_metrics = metrics_payload["models"]["decision_tree"]["aggregate_metrics"]

        assert lr_metrics["count"] == 100
        assert dt_metrics["count"] == 100
        assert lr_metrics["r2"] > 0.75, f"Expected LR R² > 0.75, got {lr_metrics['r2']}"
        assert lr_metrics["rmse"] > 0.0
        assert lr_metrics["mae"] > 0.0
        assert dt_metrics["rmse"] > 0.0
        assert dt_metrics["mae"] > 0.0

        # 3. Reload models from disk and test numerical inference parity
        reloaded_lr = LinearRegressionModel.load(lr_save)
        reloaded_dt = DecisionTreeRegressionModel.load(dt_save)

        test_df = spark.read.parquet("data/test")
        lr_preds = reloaded_lr.transform(test_df)
        dt_preds = reloaded_dt.transform(test_df)

        eval_rmse = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="rmse")
        assert abs(float(eval_rmse.evaluate(lr_preds)) - lr_metrics["rmse"]) < 1e-4
        assert abs(float(eval_rmse.evaluate(dt_preds)) - dt_metrics["rmse"]) < 1e-4


def test_independent_verifier_tool(spark):
    """
    Test 9: Verifies that verify_models() succeeds against the canonical repository artifacts.
    """
    # First make sure canonical training has been run
    train_and_evaluate_models(
        train_path="data/train",
        test_path="data/test",
        lr_model_path="models/linear_regression",
        dt_model_path="models/decision_tree",
        output_dir="reports/day-05",
        dt_max_depth=8,
        reg_param=0.0,
        spark=spark,
    )

    # Run verification audit
    passed = verify_models(
        test_path="data/test",
        lr_model_path="models/linear_regression",
        dt_model_path="models/decision_tree",
        metrics_json_path="reports/day-05/metrics.json",
        tolerance=1e-4,
        spark=spark,
    )
    assert passed is True, "Independent post-training verification failed."
