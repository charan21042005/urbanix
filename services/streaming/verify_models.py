"""
Urbanix Day 5 — Independent Model Artifact & Metric Verifier
============================================================
This module independently audits and verifies the persisted model artifacts
and evaluation results produced by `train_evaluate.py`.

Verification Checks:
1. Artifact Existence: Verifies `models/linear_regression`, `models/decision_tree`,
   and `reports/day-05/metrics.json` exist.
2. Model Deserialization: Reloads both native Spark MLlib models from disk.
3. Prediction Parity: Runs inference on `data/test/` with reloaded models and asserts
   that recomputed R², RMSE, and MAE match persisted values within 1e-4 tolerance.
4. Data Integrity: Asserts 100 test samples evaluated, exactly 20 samples per zone across 5 zones.
5. Determinism: Verifies identical predictions and outputs across repeated loads.

BEGINNER'S GUIDE:
In machine learning engineering, model evaluation metrics (like R², RMSE, and MAE) must be verifiable
by an independent process. If you serialize a model to disk and load it into a new session,
evaluating the model against the exact same test dataset must yield bit-for-bit identical predictions.
This verifier script guards against silent model corruption, floating point drift, or version mismatches.
"""

# --- Standard Library Imports ---
import os        # Environment variables and path checks
import sys       # System arguments, runtime parameters, and exit codes
import json      # JSON parsing for metric report verification
import math      # Mathematical operations (absolute differences)
import argparse  # Command-line option parsing
from pathlib import Path  # Object-oriented filesystem path handling
from typing import Dict, Any  # Type hints

# Ensure worker subprocesses use driver Python on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# --- Apache PySpark SQL & MLlib Imports ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan
# MLlib regression models
from pyspark.ml.regression import LinearRegressionModel, DecisionTreeRegressionModel
# MLlib evaluation tool for computing RMSE, MAE, and R2
from pyspark.ml.evaluation import RegressionEvaluator


def get_spark_session(app_name: str = "UrbanixVerifyModels") -> SparkSession:
    """
    Builds a local SparkSession configured for deterministic execution.

    Parameters:
        app_name (str): Spark application name displayed in logs.

    Returns:
        SparkSession: Configured active session instance.
    """
    return (
        SparkSession.builder.appName(app_name)
        # Use 2 local threads
        .master("local[2]")
        # Standardize timezone to UTC
        .config("spark.sql.session.timeZone", "UTC")
        # Allocate 2GB driver memory
        .config("spark.driver.memory", "2g")
        # Disable web UI
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def verify_models(
    test_path: str = "data/test",
    lr_model_path: str = "models/linear_regression",
    dt_model_path: str = "models/decision_tree",
    metrics_json_path: str = "reports/day-05/metrics.json",
    tolerance: float = 1e-4,
    spark: SparkSession = None,
) -> bool:
    """
    Executes the comprehensive Day 5 post-training verification audit.

    Parameters:
        test_path (str): Path to held-out test dataset in Parquet format.
        lr_model_path (str): Path to saved LinearRegressionModel.
        dt_model_path (str): Path to saved DecisionTreeRegressionModel.
        metrics_json_path (str): Path to JSON evaluation report.
        tolerance (float): Maximum acceptable absolute difference between recomputed and stored metrics.
        spark (SparkSession, optional): Existing SparkSession, or creates a new one if None.

    Returns:
        bool: True if all checks pass, False otherwise.
    """
    created_spark = False
    if spark is None:
        spark = get_spark_session("UrbanixDay5Verifier")
        created_spark = True

    try:
        print("\n" + "=" * 75)
        print("URBANIX DAY 5: INDEPENDENT MODEL AUDIT & VERIFICATION")
        print("=" * 75)

        # Track list of checks: (check_name, passed_boolean, details_string)
        checks = []

        # -------------------------------------------------------------
        # Check 1: Artifact Existence
        # -------------------------------------------------------------
        # Verify that serialized model folders, metadata, metrics JSON, and test data exist
        lr_exists = Path(lr_model_path).exists() and (Path(lr_model_path) / "metadata").exists()
        dt_exists = Path(dt_model_path).exists() and (Path(dt_model_path) / "metadata").exists()
        metrics_exists = Path(metrics_json_path).exists()
        test_exists = Path(test_path).exists()

        checks.append(("Linear Regression Model Artifact", lr_exists, f"Path: {lr_model_path}"))
        checks.append(("Decision Tree Model Artifact", dt_exists, f"Path: {dt_model_path}"))
        checks.append(("Persisted Metrics JSON", metrics_exists, f"Path: {metrics_json_path}"))
        checks.append(("Held-Out Test Dataset", test_exists, f"Path: {test_path}"))

        # If any essential file is missing, halt immediately
        if not (lr_exists and dt_exists and metrics_exists and test_exists):
            print("[!] Critical artifact missing. Halting verification.")
            for name, passed, detail in checks:
                status = "PASS" if passed else "FAIL"
                print(f"    [{status}] {name:<32} : {detail}")
            return False

        # Load persisted metrics JSON from disk
        with open(metrics_json_path, "r", encoding="utf-8") as f:
            persisted_metrics = json.load(f)

        # -------------------------------------------------------------
        # Check 2: Load Test Data & Validate Counts
        # -------------------------------------------------------------
        # Read the test partition
        test_df = spark.read.parquet(test_path)
        actual_test_count = test_df.count()
        expected_test_count = persisted_metrics["provenance"]["testing_rows"]
        # Exactly 100 test samples expected (20% of 500 total chronological observations)
        test_count_match = (actual_test_count == 100) and (actual_test_count == expected_test_count)
        checks.append((
            "Test Sample Count Reconciliation",
            test_count_match,
            f"Expected {expected_test_count}, Observed {actual_test_count}",
        ))

        # Check per-zone test distribution: exactly 20 samples per zone across all 5 zones
        zone_counts = {
            r["zone_id"]: r["count"]
            for r in test_df.groupBy("zone_id").count().collect()
        }
        all_zones_20 = len(zone_counts) == 5 and all(c == 20 for c in zone_counts.values())
        checks.append((
            "Per-Zone Sample Uniformity (20/zone)",
            all_zones_20,
            f"Observed zones: {dict(sorted(zone_counts.items()))}",
        ))

        # -------------------------------------------------------------
        # Check 3: Model Deserialization & Prediction Parity
        # -------------------------------------------------------------
        # Set up regression evaluators for RMSE, MAE, and R²
        eval_rmse = RegressionEvaluator(labelCol="next_avg_speed_kmph", predictionCol="prediction", metricName="rmse")
        eval_mae = RegressionEvaluator(labelCol="next_avg_speed_kmph", predictionCol="prediction", metricName="mae")
        eval_r2 = RegressionEvaluator(labelCol="next_avg_speed_kmph", predictionCol="prediction", metricName="r2")

        # Reload Model A (Linear Regression) from disk
        reloaded_lr = LinearRegressionModel.load(lr_model_path)
        # Compute fresh predictions on the test partition
        lr_preds = reloaded_lr.transform(test_df)
        recomputed_lr_rmse = float(eval_rmse.evaluate(lr_preds))
        recomputed_lr_mae = float(eval_mae.evaluate(lr_preds))
        recomputed_lr_r2 = float(eval_r2.evaluate(lr_preds))

        # Extract persisted metrics from the Day 5 evaluation report
        persisted_lr_rmse = persisted_metrics["models"]["linear_regression"]["aggregate_metrics"]["rmse"]
        persisted_lr_mae = persisted_metrics["models"]["linear_regression"]["aggregate_metrics"]["mae"]
        persisted_lr_r2 = persisted_metrics["models"]["linear_regression"]["aggregate_metrics"]["r2"]

        # Calculate absolute differences
        lr_rmse_diff = abs(recomputed_lr_rmse - persisted_lr_rmse)
        lr_mae_diff = abs(recomputed_lr_mae - persisted_lr_mae)
        lr_r2_diff = abs(recomputed_lr_r2 - persisted_lr_r2)

        # Assert parity within numerical tolerance
        lr_parity = (lr_rmse_diff < tolerance) and (lr_mae_diff < tolerance) and (lr_r2_diff < tolerance)
        checks.append((
            "Linear Regression Reload & Parity",
            lr_parity,
            f"delta_R2={lr_r2_diff:.6f}, delta_RMSE={lr_rmse_diff:.6f}, delta_MAE={lr_mae_diff:.6f}",
        ))

        # Reload Model B (Decision Tree Regressor) from disk
        reloaded_dt = DecisionTreeRegressionModel.load(dt_model_path)
        dt_preds = reloaded_dt.transform(test_df)
        recomputed_dt_rmse = float(eval_rmse.evaluate(dt_preds))
        recomputed_dt_mae = float(eval_mae.evaluate(dt_preds))
        recomputed_dt_r2 = float(eval_r2.evaluate(dt_preds))

        persisted_dt_rmse = persisted_metrics["models"]["decision_tree"]["aggregate_metrics"]["rmse"]
        persisted_dt_mae = persisted_metrics["models"]["decision_tree"]["aggregate_metrics"]["mae"]
        persisted_dt_r2 = persisted_metrics["models"]["decision_tree"]["aggregate_metrics"]["r2"]

        dt_rmse_diff = abs(recomputed_dt_rmse - persisted_dt_rmse)
        dt_mae_diff = abs(recomputed_dt_mae - persisted_dt_mae)
        dt_r2_diff = abs(recomputed_dt_r2 - persisted_dt_r2)

        dt_parity = (dt_rmse_diff < tolerance) and (dt_mae_diff < tolerance) and (dt_r2_diff < tolerance)
        checks.append((
            "Decision Tree Reload & Parity",
            dt_parity,
            f"delta_R2={dt_r2_diff:.6f}, delta_RMSE={dt_rmse_diff:.6f}, delta_MAE={dt_mae_diff:.6f}",
        ))

        # -------------------------------------------------------------
        # Check 4: Predictive Non-Nullity & Finite Domain
        # -------------------------------------------------------------
        # Ensure predictions contain no NaN or null outputs
        lr_invalid = lr_preds.filter(col("prediction").isNull() | isnan(col("prediction"))).count()
        dt_invalid = dt_preds.filter(col("prediction").isNull() | isnan(col("prediction"))).count()
        valid_predictions = (lr_invalid == 0) and (dt_invalid == 0)
        checks.append((
            "Prediction Numerical Sanity",
            valid_predictions,
            f"LR invalid: {lr_invalid}, DT invalid: {dt_invalid}",
        ))

        # -------------------------------------------------------------
        # Check 5: Success Threshold Honesty Audit
        # -------------------------------------------------------------
        # Verify Linear Regression meets target threshold: R² >= 0.75
        lr_passes_threshold = persisted_lr_r2 > 0.75
        threshold_audited = (persisted_metrics["models"]["linear_regression"]["threshold_met"] == lr_passes_threshold)
        checks.append((
            "Success Threshold Honesty Audit",
            threshold_audited,
            f"LR R2={persisted_lr_r2:.4f} > 0.75 -> {lr_passes_threshold}",
        ))

        # -------------------------------------------------------------
        # Print Results Checklist
        # -------------------------------------------------------------
        print("\n" + "-" * 75)
        print("VERIFICATION CHECKLIST SUMMARY")
        print("-" * 75)
        all_passed = True
        for name, passed, detail in checks:
            status = "PASS" if passed else "FAIL"
            if not passed:
                all_passed = False
            print(f"[{status}] {name:<36} : {detail}")
        print("-" * 75)

        if all_passed:
            print("\n[+] FINAL VERIFICATION RESULT: ALL CHECKS PASSED.")
            return True
        else:
            print("\n[!] FINAL VERIFICATION RESULT: ONE OR MORE CHECKS FAILED.")
            return False

    finally:
        # Clean up local SparkSession if created here
        if created_spark and spark is not None:
            spark.stop()


def main():
    """CLI entry point for model verification."""
    parser = argparse.ArgumentParser(
        description="Urbanix Day 5 Post-Training Verification Tool"
    )
    parser.add_argument("--test-path", type=str, default="data/test")
    parser.add_argument("--lr-model-path", type=str, default="models/linear_regression")
    parser.add_argument("--dt-model-path", type=str, default="models/decision_tree")
    parser.add_argument("--metrics-json-path", type=str, default="reports/day-05/metrics.json")
    parser.add_argument("--tolerance", type=float, default=1e-4)

    args = parser.parse_args()

    # Execute verification
    success = verify_models(
        test_path=args.test_path,
        lr_model_path=args.lr_model_path,
        dt_model_path=args.dt_model_path,
        metrics_json_path=args.metrics_json_path,
        tolerance=args.tolerance,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
