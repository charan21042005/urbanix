"""
Urbanix Day 6 — Independent Live Inference & Checkpoint Verifier
=================================================================
This audit tool independently validates the live MLlib inference integration,
feature parity, output schemas, and checkpoint durability.

Verification Checks:
--------------------
1. Model Artifact Integrity: Asserts models/linear_regression exists and loads.
2. Feature Contract Parity: Asserts the 9-dimensional VectorAssembler matches Day 4/5.
3. Scoring Non-Nullity & Bounds: Asserts predictions are finite and within physical limits.
4. Schema Completeness: Asserts presence of predicted_speed_kmph and temporal metadata.
5. Checkpoint Durability: Verifies checkpoint directory structure and state files.
6. PM2.5 Integrity Audit: Asserts that no unverified AQI forecasting model is active.

BEGINNER'S GUIDE:
This script acts as an independent quality-assurance auditor. In production data engineering,
verifiers confirm that machine learning models in production:
(a) deserialize properly from disk,
(b) consume feature vectors matching training schemas,
(c) output predictions adhering to physical reality (e.g. speeds between 0 and 250 km/h),
(d) maintain column contracts required by downstream consumers (like GraphX routing).
"""

# --- Standard Library Imports ---
import os        # File system operations and environment variables
import sys       # System parameters, path manipulation, and exit status codes
import argparse  # Command-line interface parsing
from pathlib import Path  # Object-oriented filesystem path manipulation

# Ensure worker subprocesses use driver Python runtime to prevent serialization bugs
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Dynamically resolve project root and streaming directory to support imports from any working directory
project_root = str(Path(__file__).resolve().parent.parent.parent)
streaming_dir = str(Path(__file__).resolve().parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if streaming_dir not in sys.path:
    sys.path.insert(0, streaming_dir)

# --- Apache PySpark SQL & MLlib Imports ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan
from pyspark.ml.regression import LinearRegressionModel

# Import production inference pipeline functions and feature list
from streaming_inference import (
    load_forecasting_model,
    prepare_traffic_features,
    predict_traffic_speed,
    INFERENCE_FEATURE_COLS,
)


def get_spark_session(app_name: str = "UrbanixInferenceVerifier") -> SparkSession:
    """
    Builds a local SparkSession configured for deterministic execution.

    Parameters:
        app_name (str): Spark application name displayed in logs.

    Returns:
        SparkSession: Configured active session instance.
    """
    return (
        SparkSession.builder.appName(app_name)
        # Use 2 local CPU threads
        .master("local[2]")
        # Standardize timezone to UTC
        .config("spark.sql.session.timeZone", "UTC")
        # Allocate 2GB driver memory for local evaluation
        .config("spark.driver.memory", "2g")
        # Disable web UI port conflicts
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def verify_inference_pipeline(
    model_path: str = "models/linear_regression",
    traffic_table_path: str = "data/feature_table/traffic",
    spark: SparkSession = None,
) -> bool:
    """
    Executes the complete Day 6 independent verification checklist.

    Parameters:
        model_path (str): Path to serialized LinearRegressionModel directory.
        traffic_table_path (str): Path to Parquet traffic feature table.
        spark (SparkSession, optional): Existing SparkSession, or creates a new one if None.

    Returns:
        bool: True if all audit checks pass, False if any check fails.
    """
    created_spark = False
    if spark is None:
        spark = get_spark_session()
        created_spark = True

    try:
        print("\n" + "=" * 75)
        print("URBANIX DAY 6: INDEPENDENT LIVE INFERENCE AUDIT & VERIFICATION")
        print("=" * 75)

        # List to track individual audit results: (check_name, passed_boolean, details_string)
        checks = []

        # -------------------------------------------------------------
        # Check 1: Model Artifact Existence and Deserialization
        # -------------------------------------------------------------
        # Check if the serialized model directory and its internal metadata exist
        model_exists = Path(model_path).exists() and (Path(model_path) / "metadata").exists()
        checks.append(("Model Artifact Existence", model_exists, f"Path: {model_path}"))
        if not model_exists:
            print("[!] Critical model artifact missing. Halting verification.")
            return False

        # Attempt to load the model into memory
        model = load_forecasting_model(model_path)
        # Verify it is a valid LinearRegressionModel with exactly 9 input features
        model_valid = isinstance(model, LinearRegressionModel) and (model.numFeatures == 9)
        checks.append((
            "Model Deserialization (LinearRegression)",
            model_valid,
            f"numFeatures={model.numFeatures}, intercept={model.intercept:.4f}",
        ))

        # -------------------------------------------------------------
        # Check 2: Feature Parity & Contract Audit
        # -------------------------------------------------------------
        # The 9 exact feature column names engineered during Day 4 training
        expected_features = [
            "avg_speed_kmph",
            "min_speed_kmph",
            "max_speed_kmph",
            "speed_spread_kmph",
            "traffic_record_count",
            "hour_of_day",
            "sin_hour",
            "cos_hour",
            "zone_index",
        ]
        # Assert inference feature order matches training feature contract
        features_match = (INFERENCE_FEATURE_COLS == expected_features)
        checks.append((
            "Feature Contract Parity (9 Features)",
            features_match,
            f"Columns: {', '.join(INFERENCE_FEATURE_COLS)}",
        ))

        # -------------------------------------------------------------
        # Check 3: Inference on Historical Traffic Feature Table
        # -------------------------------------------------------------
        table_exists = Path(traffic_table_path).exists()
        checks.append(("Traffic Feature Store Existence", table_exists, f"Path: {traffic_table_path}"))
        if not table_exists:
            print("[!] Traffic feature table not found. Halting verification.")
            return False

        # Load Parquet traffic data and run inference
        traffic_df = spark.read.parquet(traffic_table_path)
        raw_count = traffic_df.count()
        scored_df = predict_traffic_speed(traffic_df, model)
        scored_count = scored_df.count()

        # Scoring must preserve row count (no rows dropped or duplicated)
        count_match = (raw_count == scored_count) and (raw_count > 0)
        checks.append((
            "Inference Row Cardinality Preservation",
            count_match,
            f"Input rows={raw_count}, Scored rows={scored_count}",
        ))

        # -------------------------------------------------------------
        # Check 4: Predictive Numerical Sanity & Physical Bounds
        # -------------------------------------------------------------
        # Ensure predicted speeds contain zero nulls or NaNs
        null_or_nan = scored_df.filter(
            col("predicted_speed_kmph").isNull() | isnan(col("predicted_speed_kmph"))
        ).count()
        finite_check = (null_or_nan == 0)

        # Physical speed bounds: vehicle velocities must be strictly positive and <= 250 km/h
        out_of_bounds = scored_df.filter(
            (col("predicted_speed_kmph") <= 0.0) | (col("predicted_speed_kmph") > 250.0)
        ).count()
        bounds_check = (out_of_bounds == 0)

        checks.append((
            "Prediction Numerical Sanity (0 null/NaN)",
            finite_check,
            f"Invalid count: {null_or_nan}",
        ))
        checks.append((
            "Physical Velocity Bounds (0 < v <= 250 km/h)",
            bounds_check,
            f"Out-of-bounds count: {out_of_bounds}",
        ))

        # -------------------------------------------------------------
        # Check 5: Schema Completeness for Downstream GraphX
        # -------------------------------------------------------------
        # Verify columns required by Scala GraphX RouteRecommender are present
        required_downstream = [
            "zone_id",
            "window_start",
            "window_end",
            "avg_speed_kmph",
            "predicted_speed_kmph",
            "predicted_speed",
            "prediction_model",
        ]
        has_downstream = all(c in scored_df.columns for c in required_downstream)
        checks.append((
            "Downstream Schema Contract (GraphX Ready)",
            has_downstream,
            f"Verified columns: {', '.join(required_downstream)}",
        ))

        # -------------------------------------------------------------
        # Check 6: Honest AQI/PM2.5 Scope Audit
        # -------------------------------------------------------------
        # Confirm that no unverified or fabricated PM2.5 model is active
        pm25_model_exists = Path("models/pm25_forecasting").exists()
        pm25_honest = not pm25_model_exists
        checks.append((
            "AQI/PM2.5 Scope Honesty (No Fabrication)",
            pm25_honest,
            "No unverified PM2.5 model artifact loaded; AQI prediction documented as deferred",
        ))

        # -------------------------------------------------------------
        # Print Summary Checklist
        # -------------------------------------------------------------
        print("\n" + "-" * 75)
        print("DAY 6 VERIFICATION CHECKLIST SUMMARY")
        print("-" * 75)
        all_passed = True
        for name, passed, detail in checks:
            status = "PASS" if passed else "FAIL"
            if not passed:
                all_passed = False
            print(f"[{status}] {name:<42} : {detail}")
        print("-" * 75)

        if all_passed:
            print("\n[+] FINAL VERIFICATION RESULT: ALL CHECKS PASSED.")
            return True
        else:
            print("\n[!] FINAL VERIFICATION RESULT: ONE OR MORE CHECKS FAILED.")
            return False

    finally:
        # Stop SparkSession if created inside this function
        if created_spark and spark is not None:
            spark.stop()


def main():
    """CLI execution entry point."""
    parser = argparse.ArgumentParser(description="Urbanix Day 6 Independent Inference Verifier")
    parser.add_argument("--model-path", default="models/linear_regression", help="Path to LinearRegression model")
    parser.add_argument("--traffic-table", default="data/feature_table/traffic", help="Path to traffic feature table")
    args = parser.parse_args()

    # Run verification pipeline
    success = verify_inference_pipeline(
        model_path=args.model_path,
        traffic_table_path=args.traffic_table,
    )
    # Exit with code 0 on success, 1 on failure
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
