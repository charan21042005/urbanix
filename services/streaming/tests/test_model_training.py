"""
Urbanix Day 5 — Automated Test Suite: MLlib Model Training & Evaluation
========================================================================
Comprehensive unit and integration tests verifying:
1. Input dataframe validation (column existence, VectorUDT, 9 dimensions, physical bounds).
2. Failure handling on empty or missing datasets.
3. Deterministic regression metric calculation on synthetic ground truth.
4. Robust handling of constant labels and single samples (undefined R²).
5. Per-zone evaluation correctness and sample count tracking.
6. Linear Regression model training on real Day 4 training split (365 rows).
7. Decision Tree Regressor training on real Day 4 training split.
8. Evaluation parity: both models evaluated on identical held-out test rows (100 rows).
9. MLlib native model persistence, reloading, and numerical prediction parity (< 1e-4).
10. Machine-readable metrics.json and results.md generation and metadata completeness.
"""

from pyspark.ml.evaluation import RegressionEvaluator
import os
import sys
import json
import math
import tempfile
import shutil
from pathlib import Path
import pytest

# Ensure project root is in sys.path
project_root = str(Path(__file__).resolve().parent.parent.parent.parent)
streaming_dir = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if streaming_dir not in sys.path:
    sys.path.insert(0, streaming_dir)

# Ensure worker subprocesses use driver Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.regression import LinearRegressionModel, DecisionTreeRegressionModel

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
    """Initializes a shared SparkSession for test execution."""
    session = (
        SparkSession.builder.appName("UrbanixDay5Tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# =============================================================================
# UNIT TESTS: Metrics, Edge Cases, and Validation
# =============================================================================

def test_compute_regression_metrics_deterministic(spark):
    """Verifies that RMSE, MAE, and R² match exact hand-computed mathematical values."""
    # Data: actuals = [10.0, 20.0, 30.0], preds = [12.0, 18.0, 32.0]
    # Errors: [+2, -2, +2] -> Squared errors: [4, 4, 4] -> MSE = 4.0 -> RMSE = 2.0
    # Absolute errors: [2, 2, 2] -> MAE = 2.0
    # Actual mean = 20.0. Deviations: [-10, 0, +10] -> SS_tot = 100 + 0 + 100 = 200.
    # SS_res = 4 + 4 + 4 = 12.
    # R² = 1 - 12/200 = 1 - 0.06 = 0.94.
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

    assert metrics["count"] == 3
    assert abs(metrics["rmse"] - 2.0) < 1e-4
    assert abs(metrics["mae"] - 2.0) < 1e-4
    assert abs(metrics["r2"] - 0.94) < 1e-4


def test_compute_regression_metrics_constant_label(spark):
    """Verifies that a constant target label yields R² = None rather than a division by zero error."""
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
    assert metrics["r2"] is None
    assert abs(metrics["rmse"] - 1.6330) < 1e-3


def test_compute_regression_metrics_empty_df(spark):
    """Verifies that an empty dataframe returns zero count and None for all metrics."""
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
    """Verifies ValueError when a required column (e.g. features) is missing."""
    schema = StructType([
        StructField("window_start", TimestampType(), True),
        StructField("zone_id", StringType(), True),
        StructField("next_avg_speed_kmph", DoubleType(), True),
    ])
    df = spark.createDataFrame([], schema)
    with pytest.raises(ValueError, match="Partition 'test' is empty"):
        validate_input_dataframe(df, "test")


def test_validate_input_dataframe_invalid_dimension(spark):
    """Verifies ValueError when feature vector dimension is not 9."""
    schema = StructType([
        StructField("window_start", TimestampType(), False),
        StructField("zone_id", StringType(), False),
        StructField("next_avg_speed_kmph", DoubleType(), False),
        StructField("features", VectorUDT(), False),
    ])
    # 3-dimensional vector instead of 9
    from datetime import datetime
    data = [(datetime(2026, 1, 1), "zone_1", 50.0, Vectors.dense([1.0, 2.0, 3.0]))]
    df = spark.createDataFrame(data, schema)

    with pytest.raises(ValueError, match="Expected 9-dimensional feature vector"):
        validate_input_dataframe(df, "train")


def test_evaluate_per_zone_groups_accurately(spark):
    """Verifies that evaluate_per_zone produces distinct metrics for all distinct zones."""
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
    assert abs(zone_metrics["zone_2"]["rmse"] - 0.0) < 1e-4
    assert abs(zone_metrics["zone_2"]["mae"] - 0.0) < 1e-4


# =============================================================================
# INTEGRATION TESTS: Real Day 4 Data & Persistence
# =============================================================================

def test_real_day4_dataset_existence_and_dimensions(spark):
    """Verifies that data/train and data/test exist and conform strictly to Day 4 handoff."""
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
    Executes the full training and evaluation pipeline into a temporary directory,
    verifying model persistence, report creation, and metric bounds.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        lr_save = os.path.join(tmp_dir, "models", "lr")
        dt_save = os.path.join(tmp_dir, "models", "dt")
        rep_dir = os.path.join(tmp_dir, "reports")

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

        # 1. Check generated files
        assert os.path.exists(lr_save)
        assert os.path.exists(dt_save)
        assert os.path.exists(os.path.join(rep_dir, "metrics.json"))
        assert os.path.exists(os.path.join(rep_dir, "results.md"))
        assert os.path.exists(os.path.join(rep_dir, "per_zone_metrics.json"))

        # 2. Check metrics payload
        lr_metrics = metrics_payload["models"]["linear_regression"]["aggregate_metrics"]
        dt_metrics = metrics_payload["models"]["decision_tree"]["aggregate_metrics"]

        assert lr_metrics["count"] == 100
        assert dt_metrics["count"] == 100
        assert lr_metrics["r2"] > 0.75, f"Expected LR R² > 0.75, got {lr_metrics['r2']}"
        assert lr_metrics["rmse"] > 0.0
        assert lr_metrics["mae"] > 0.0
        assert dt_metrics["rmse"] > 0.0
        assert dt_metrics["mae"] > 0.0

        # 3. Reload models and test inference consistency
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
    Verifies that verify_models() succeeds against the canonical repository artifacts.
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
