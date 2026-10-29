#!/usr/bin/env python3
# ==============================================================================
# 🌟 URBANIX — PYSPARK MLLIB LINEAR REGRESSION TRAINING DEMONSTRATION
# services/streaming/train_linear_regression.py
# ==============================================================================
# Welcome to the Urbanix PySpark MLlib Linear Regression Training Module! 👋
#
# ------------------------------------------------------------------------------
# 🎓 WHAT IS THIS FILE FOR?
# ------------------------------------------------------------------------------
# In big data and smart city platforms, training machine learning models on massive
# clusters requires distributed algorithms. Single-machine libraries like Scikit-Learn
# load all data into one computer's RAM, which crashes when datasets grow to gigabytes
# or terabytes.
#
# Apache Spark MLlib solves this by distributing data across multiple worker nodes!
# This Python file trains a high-throughput, distributed Linear Regression model
# to forecast near-future traffic speeds across 5 Los Angeles municipal zones.
#
# ------------------------------------------------------------------------------
# 🏛️ HISTORICAL PROVENANCE & ARCHITECTURAL INTEGRITY NOTICE:
# ------------------------------------------------------------------------------
# In the original Urbanix Day 5 milestone, both Linear Regression (Model A) and
# Decision Tree (Model B) were trained together inside:
#   `services/streaming/train_evaluate.py`
#
# This standalone file was extracted specifically for your B.Tech capstone presentation
# and viva voce examination. It exposes the exact Linear Regression training pipeline
# in complete isolation, with zero changes to data schemas, target columns, hyperparameters,
# or mathematical results.
#
# ------------------------------------------------------------------------------
# 🧠 THE MATHEMATICS OF LINEAR REGRESSION (VIVA VOCE EXPLANATION):
# ------------------------------------------------------------------------------
# 1. THE FORECASTING GOAL:
#    Given the traffic conditions right now (in 5-minute window t), what will the
#    average vehicle speed be in the NEXT 5-minute window (t+1)?
#
# 2. THE MULTIVARIATE LINEAR EQUATION:
#    Linear Regression models future speed (y_hat) as a linear combination of features:
#
#    y_hat = beta_0 + (beta_1 * x_1) + (beta_2 * x_2) + ... + (beta_9 * x_9)
#
#    Where:
#      - y_hat is the predicted velocity for the next 5-minute interval (in km/h).
#      - beta_0 is the Intercept (the baseline speed when all input features are zero).
#      - beta_1 ... beta_9 are Learned Weights (coefficients) for each of the 9 features.
#      - x_1 ... x_9 are the 9 input feature values in the feature vector.
#
# 3. ORDINARY LEAST SQUARES (OLS) OPTIMIZATION:
#    How does Spark find the best beta values?
#    Spark minimizes the Residual Sum of Squares (RSS):
#
#    RSS = Sum( (y_actual - y_predicted)^2 )
#
#    By minimizing the squared differences, Spark finds the line of best fit that
#    generalizes across all smart city zones!
#
# 4. CHRONOLOGICAL SPLIT (ZERO TEMPORAL LEAKAGE):
#    In time-series forecasting, we NEVER use random train/test splits.
#    - Training set: First 80% of time (365 rows up to 06:20:00 UTC).
#    - Testing set: Subsequent 20% of time (100 rows starting 06:25:00 UTC).
#    - This ensures the model is evaluated on future data it has never seen!
# ==============================================================================

# ------------------------------------------------------------------------------
# 📦 STEP 1: IMPORT STANDARD PYTHON LIBRARIES
# ------------------------------------------------------------------------------

# 'os' allows Python to interact with the underlying operating system (environment variables, paths)
import os

# 'sys' provides access to system parameters, interpreter paths, and standard input/output streams
import sys

# 'math' provides standard mathematical functions like isnan() and isinf() to guard against numerical errors
import math

# 'argparse' makes this script runnable from the terminal with custom command-line flags (e.g. --train-path)
import argparse

# 'pathlib.Path' is Python's modern, cross-platform tool for manipulating filesystem directory paths safely
from pathlib import Path

# 'typing' adds type hints to function signatures, making code self-documenting and easier for beginners to read
from typing import Dict, Any, Optional

# ------------------------------------------------------------------------------
# 🔧 PYTHON RUNTIME BINDING FOR DISTRIBUTED WORKERS
# ------------------------------------------------------------------------------
# In distributed Apache Spark environments (and especially local Windows systems),
# worker subprocesses must execute using the exact same Python binary as the driver.
# If these environment variables are omitted, PySpark can default to system Python,
# leading to PicklingError and version mismatch crashes.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# ------------------------------------------------------------------------------
# 📦 STEP 2: IMPORT APACHE PYSPARK SQL & MLLIB MODULES
# ------------------------------------------------------------------------------

# 'SparkSession' is the unified gateway to all Spark functionality (DataFrames, SQL queries, cluster management)
# 'DataFrame' is Spark's distributed two-dimensional table of rows and typed columns
from pyspark.sql import SparkSession, DataFrame

# 'col' creates column reference expressions (e.g. col("avg_speed_kmph") > 0)
# 'isnan' detects IEEE NaN (Not-a-Number) floating point corruptions in DataFrames
from pyspark.sql.functions import col, isnan

# 'VectorUDT' (User-Defined Type) is Spark's internal schema data type for representing dense/sparse vectors
from pyspark.ml.linalg import VectorUDT

# 'VectorAssembler' is a feature transformer that bundles multiple numeric columns into a single vector
from pyspark.ml.feature import VectorAssembler

# 'LinearRegression' is the MLlib Estimator that trains the linear model using .fit()
# 'LinearRegressionModel' is the resulting MLlib Transformer that computes predictions using .transform()
from pyspark.ml.regression import LinearRegression, LinearRegressionModel

# 'RegressionEvaluator' calculates standard statistical metrics: R² (R-squared), RMSE, and MAE
from pyspark.ml.evaluation import RegressionEvaluator

# ------------------------------------------------------------------------------
# 📋 STEP 3: DEFINE THE 9 CANONICAL FEATURE COLUMNS & TARGET
# ------------------------------------------------------------------------------
# These 9 feature names match the exact schema engineered in Day 4 (prepare_training_data.py)
# and verified in Day 5 (train_evaluate.py).
FEATURE_NAMES = [
    # Feature 0: Current 5-minute average speed in km/h (strongest autoregressive predictor)
    "avg_speed_kmph",

    # Feature 1: Max speed minus min speed in the window (measures traffic speed volatility/burstiness)
    "speed_spread_kmph",

    # Feature 2: Minimum speed recorded by any sensor loop in the zone during the 5-minute window
    "min_speed_kmph",

    # Feature 3: Maximum speed recorded by any sensor loop in the zone during the 5-minute window
    "max_speed_kmph",

    # Feature 4: Number of sensor loop observations in the window (acts as a proxy for vehicle volume)
    "reading_count",

    # Feature 5: Sine encoding of time-of-day: sin(2 * pi * hour / 24), captures daily cyclic rhythm
    "sin_hour",

    # Feature 6: Cosine encoding of time-of-day: cos(2 * pi * hour / 24), ensures 23:59 wraps smoothly to 00:00
    "cos_hour",

    # Feature 7: Numeric zone index (0.0=Downtown, 1.0=Valley, 2.0=Foothills, 3.0=South, 4.0=Harbor)
    "zone_index",

    # Feature 8: Speed in the previous 5-minute interval (t-1), enabling the model to sense acceleration/braking
    "speed_lag_1",
]

# The supervised target label (y): ground truth average speed in the NEXT 5-minute interval (t+1)
TARGET_COLUMN = "next_avg_speed_kmph"

# The name of the VectorUDT column where the 9 features are packaged together for MLlib
FEATURES_COLUMN = "features"

# The identifier column grouping observations into municipal zones (e.g. 'zone_1' ... 'zone_5')
ZONE_COLUMN = "zone_id"


# ------------------------------------------------------------------------------
# ⚙️ STEP 4: SPARK SESSION CREATION FUNCTION
# ------------------------------------------------------------------------------
def create_spark_session(app_name: str = "UrbanixTrainLinearRegression") -> SparkSession:
    """
    Initializes and configures a local Apache SparkSession for deterministic model training.

    Why these specific configurations?
      - master('local[2]'): Allocates 2 CPU worker threads on your computer to process data in parallel.
      - timeZone('UTC'): Forces all timestamps to Universal Time Coordinated, preventing daylight savings bugs.
      - memory('2g'): Allocates 2 Gigabytes of RAM to the Spark driver process to prevent OutOfMemory errors.
      - ui.enabled('false'): Disables the Spark Web UI server on port 4040 to speed up execution and free ports.
    """
    # Build and return the configured SparkSession
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")                           # 2 local worker execution threads
        .config("spark.sql.session.timeZone", "UTC")   # Consistent UTC clock standard
        .config("spark.driver.memory", "2g")           # 2 GB Java heap memory allocation
        .config("spark.ui.enabled", "false")           # Suppress UI background server for speed
        .getOrCreate()                                # Retrieve existing session or initialize new one
    )


# ------------------------------------------------------------------------------
# 🔍 STEP 5: DEFENSIVE DATA VALIDATION & SCHEMA HYGIENE
# ------------------------------------------------------------------------------
def validate_dataset(df: DataFrame, name: str) -> int:
    """
    Validates the dataset before training to ensure strict data quality.
    In distributed systems, catching schema or data corruption early prevents silent training failures!

    Validation Checks:
      1. Non-emptiness: Verifies the partition has at least 1 record.
      2. Required Columns: Verifies target, features, and zone columns exist.
      3. Vector Type: Verifies 'features' is an authentic MLlib VectorUDT.
      4. Target Cleanliness: Asserts that no NaN or Null values exist in the target column.
    """
    # Count rows in this distributed partition
    count = df.count()
    if count == 0:
        # Raise an exception if the dataset is completely empty
        raise ValueError(f"CRITICAL ERROR: Dataset partition '{name}' is empty (0 rows).")

    # Verify that all mandatory columns are present in the DataFrame schema
    required_columns = [TARGET_COLUMN, FEATURES_COLUMN, ZONE_COLUMN]
    for column_name in required_columns:
        if column_name not in df.columns:
            raise ValueError(f"CRITICAL ERROR: Missing required column '{column_name}' in '{name}'.")

    # Verify that the 'features' column is actually a VectorUDT (user-defined vector type)
    features_field = [field for field in df.schema.fields if field.name == FEATURES_COLUMN][0]
    if not isinstance(features_field.dataType, VectorUDT):
        raise TypeError(
            f"CRITICAL ERROR: Column '{FEATURES_COLUMN}' must be VectorUDT, but found {features_field.dataType}."
        )

    # Check for invalid NaN or Null targets (which would break gradient descent / normal equations)
    invalid_targets = df.filter(col(TARGET_COLUMN).isNull() | isnan(col(TARGET_COLUMN))).count()
    if invalid_targets > 0:
        raise ValueError(f"CRITICAL ERROR: Dataset '{name}' contains {invalid_targets} null or NaN targets.")

    # Return total valid row count
    return count


# ------------------------------------------------------------------------------
# 📊 STEP 6: EVALUATION METRICS CALCULATION (R², RMSE, MAE)
# ------------------------------------------------------------------------------
def evaluate_predictions(predictions_df: DataFrame) -> Dict[str, Any]:
    """
    Calculates the 3 standard academic regression evaluation metrics:
      1. R² (Coefficient of Determination):
         Measures the proportion of variance in traffic speed explained by the model.
         Formula: R² = 1 - (SS_residual / SS_total). 1.0 = perfect score. Project goal: > 0.75.
      2. RMSE (Root Mean Squared Error):
         Typical magnitude of prediction errors in km/h. Heavily penalizes large outliers.
         Formula: RMSE = sqrt( mean( (y - y_hat)^2 ) ).
      3. MAE (Mean Absolute Error):
         Average absolute prediction difference in km/h. Linear error penalty.
         Formula: MAE = mean( |y - y_hat| ).
    """
    # Total number of test samples being evaluated
    sample_count = predictions_df.count()
    if sample_count == 0:
        return {"r2": None, "rmse": None, "mae": None, "count": 0}

    # Initialize PySpark's RegressionEvaluator for R²
    evaluator_r2 = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="r2"
    )

    # Initialize PySpark's RegressionEvaluator for RMSE
    evaluator_rmse = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="rmse"
    )

    # Initialize PySpark's RegressionEvaluator for MAE
    evaluator_mae = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="mae"
    )

    # Evaluate test predictions across distributed worker nodes
    raw_r2 = float(evaluator_r2.evaluate(predictions_df))
    raw_rmse = float(evaluator_rmse.evaluate(predictions_df))
    raw_mae = float(evaluator_mae.evaluate(predictions_df))

    # Guard against NaN or Infinite R² (can happen if test sample variance is zero)
    clean_r2 = raw_r2 if not (math.isnan(raw_r2) or math.isinf(raw_r2)) else None

    # Return clean, formatted dictionary rounded to 4 decimal places
    return {
        "r2": round(clean_r2, 4) if clean_r2 is not None else None,
        "rmse": round(raw_rmse, 4),
        "mae": round(raw_mae, 4),
        "count": sample_count,
    }


# ------------------------------------------------------------------------------
# 🚀 STEP 7: FULL TRAINING, TESTING & EVALUATION WORKFLOW
# ------------------------------------------------------------------------------
def run_linear_regression_pipeline(
    train_path: str = "data/train",
    test_path: str = "data/test",
    model_save_path: Optional[str] = None,
    reg_param: float = 0.0,
    elastic_net_param: float = 0.0,
    spark: Optional[SparkSession] = None,
) -> Dict[str, Any]:
    """
    Orchestrates the entire training lifecycle:
      1. Loads chronological Parquet datasets from disk.
      2. Validates schema and VectorUDT dimensional parity.
      3. Configures and trains the MLlib LinearRegression estimator.
      4. Inspects learned intercept (beta_0) and feature coefficients (beta_1..9).
      5. Runs inference on held-out test data using .transform().
      6. Evaluates R², RMSE, and MAE against ground truth.
      7. Evaluates accuracy per municipal zone.
      8. Optionally saves the fitted model to disk.
    """
    # Track whether this function initialized its own SparkSession (so it knows to shut it down)
    created_own_spark = False
    if spark is None:
        spark = create_spark_session()
        created_own_spark = True

    try:
        # Print informational startup banner
        print("=" * 80)
        print("  URBANIX — PYSPARK MLLIB LINEAR REGRESSION TRAINING PIPELINE")
        print("=" * 80)
        print(f"[*] Training Data Source : {train_path}")
        print(f"[*] Testing Data Source  : {test_path}")
        print(f"[*] Target Column (y)    : {TARGET_COLUMN}")
        print(f"[*] Features (X)         : {len(FEATURE_NAMES)} dimensions ({', '.join(FEATURE_NAMES)})")
        print(f"[*] Hyperparameters      : regParam={reg_param}, elasticNetParam={elastic_net_param}")

        # ----------------------------------------------------------------------
        # SUB-STEP A: LOAD CHRONOLOGICALLY PARTITIONED DATASETS
        # ----------------------------------------------------------------------
        # Read the columnar Parquet files from disk into distributed Spark DataFrames
        train_df = spark.read.parquet(train_path)
        test_df = spark.read.parquet(test_path)

        # Validate that both partitions meet all data quality requirements
        train_rows = validate_dataset(train_df, "Training Set")
        test_rows = validate_dataset(test_df, "Testing Set")

        print(f"\n[+] Loaded Datasets Successfully:")
        print(f"    - Training Partition : {train_rows} rows (Chronological split <= 06:20 UTC)")
        print(f"    - Testing Partition  : {test_rows} rows (Held-out evaluation >= 06:25 UTC)")

        # ----------------------------------------------------------------------
        # SUB-STEP B: CONFIGURE THE LINEAR REGRESSION ESTIMATOR
        # ----------------------------------------------------------------------
        # Explanation of Estimator Parameters:
        #   - featuresCol: Tells Spark which column contains the input vector ('features')
        #   - labelCol: Tells Spark which column contains the ground truth speed ('next_avg_speed_kmph')
        #   - predictionCol: Name of the output column Spark will create ('prediction')
        #   - regParam=0.0: Regularization penalty (0.0 = Ordinary Least Squares with no L2 penalty)
        #   - elasticNetParam=0.0: Mixing parameter (0.0 = pure L2 Ridge, 1.0 = pure L1 Lasso)
        #   - fitIntercept=True: Computes constant beta_0 offset
        #   - standardization=True: Standardizes features to zero mean and unit variance before fitting
        lr = LinearRegression(
            featuresCol=FEATURES_COLUMN,
            labelCol=TARGET_COLUMN,
            predictionCol="prediction",
            regParam=reg_param,
            elasticNetParam=elastic_net_param,
            fitIntercept=True,
            standardization=True,
        )

        # ----------------------------------------------------------------------
        # SUB-STEP C: FIT THE MODEL ON TRAINING DATA (THE LEARNING STEP)
        # ----------------------------------------------------------------------
        # In Spark MLlib:
        #   - An Estimator uses '.fit(df)' to examine training data and calculate weights.
        #   - '.fit()' returns a trained Transformer model instance ('LinearRegressionModel').
        print("\n[*] Fitting LinearRegression Estimator on Training Data...")
        lr_model: LinearRegressionModel = lr.fit(train_df)
        print("[+] Model Fitting Completed Successfully!")

        # ----------------------------------------------------------------------
        # SUB-STEP D: INSPECT LEARNED MATHEMATICAL PARAMETERS
        # ----------------------------------------------------------------------
        # The intercept is the baseline speed when all 9 features are zero:
        intercept = float(lr_model.intercept)
        # The coefficients are the learned multipliers (slopes) for each feature:
        coefficients = [float(c) for c in lr_model.coefficients]

        print("\n" + "-" * 60)
        print("LEARNED PARAMETERS (COEFFICIENTS & INTERCEPT)")
        print("-" * 60)
        print(f"  Intercept (beta_0): {intercept:+.4f}")
        for name, coef in zip(FEATURE_NAMES, coefficients):
            # Print feature weight formatted with plus/minus sign
            print(f"  * {name:<18}: {coef:+.4f}")

        # ----------------------------------------------------------------------
        # SUB-STEP E: OUT-OF-SAMPLE EVALUATION ON HELD-OUT TEST DATA
        # ----------------------------------------------------------------------
        # In Spark MLlib:
        #   - A Model/Transformer uses '.transform(df)' to generate predictions.
        #   - It takes the test feature vectors, multiplies by learned coefficients,
        #     and appends the 'prediction' column to the DataFrame!
        print("\n[*] Evaluating Model on Held-Out Test Partition (100 rows)...")
        predictions_df = lr_model.transform(test_df)

        # Calculate R², RMSE, and MAE across the entire test set
        overall_metrics = evaluate_predictions(predictions_df)

        print("\n" + "-" * 60)
        print("OVERALL MODEL PERFORMANCE METRICS")
        print("-" * 60)
        print(f"  R² (Variance Explained) : {overall_metrics['r2']} (Project Target: > 0.75)")
        print(f"  RMSE (Typical Error)    : {overall_metrics['rmse']} km/h")
        print(f"  MAE (Mean Error)        : {overall_metrics['mae']} km/h")
        print(f"  Evaluated Samples       : {overall_metrics['count']} rows")

        # ----------------------------------------------------------------------
        # SUB-STEP F: ROADMAP SUCCESS THRESHOLD CHECK
        # ----------------------------------------------------------------------
        # Our capstone project specification requires R² > 0.75 on held-out test data
        passed_threshold = (overall_metrics["r2"] is not None) and (overall_metrics["r2"] > 0.75)
        print(f"\n[*] Roadmap Target Gate Check (R² > 0.75): {'PASSED [SUCCESS]' if passed_threshold else 'FAILED'}")

        # ----------------------------------------------------------------------
        # SUB-STEP G: PER-ZONE ACCURACY BREAKDOWN
        # ----------------------------------------------------------------------
        # Evaluate accuracy independently for each municipal zone to see where model performs best
        print("\n" + "-" * 60)
        print("PER-ZONE ACCURACY BREAKDOWN")
        print("-" * 60)
        # Extract unique zone IDs from the test set (zone_1 to zone_5)
        distinct_zones = [
            row[ZONE_COLUMN]
            for row in predictions_df.select(ZONE_COLUMN).distinct().sort(ZONE_COLUMN).collect()
        ]

        per_zone_results = {}
        for zone_id in distinct_zones:
            # Filter predictions for just this one zone
            zone_subset = predictions_df.filter(col(ZONE_COLUMN) == zone_id)
            # Compute R², RMSE, and MAE for this zone
            zone_metrics = evaluate_predictions(zone_subset)
            per_zone_results[zone_id] = zone_metrics
            print(
                f"  Zone '{zone_id}': R² = {zone_metrics['r2']}, "
                f"RMSE = {zone_metrics['rmse']} km/h, "
                f"MAE = {zone_metrics['mae']} km/h (n={zone_metrics['count']})"
            )

        # ----------------------------------------------------------------------
        # SUB-STEP H: OPTIONAL MODEL PERSISTENCE
        # ----------------------------------------------------------------------
        # If the caller provided a model save directory, serialize the fitted model
        if model_save_path:
            print(f"\n[*] Persisting fitted LinearRegressionModel to: {model_save_path}")
            Path(model_save_path).parent.mkdir(parents=True, exist_ok=True)
            # Spark's native write().overwrite().save() stores weights and metadata on disk
            lr_model.write().overwrite().save(model_save_path)
            print(f"[+] Model persisted successfully to {model_save_path}")

        # Return a summary dictionary for programmatic consumption
        return {
            "model_type": "LinearRegression",
            "overall_metrics": overall_metrics,
            "per_zone_metrics": per_zone_results,
            "intercept": intercept,
            "coefficients": dict(zip(FEATURE_NAMES, coefficients)),
            "threshold_met": passed_threshold,
        }

    finally:
        # Guarantee that SparkSession stops cleanly to release CPU threads and JVM heap memory
        if created_own_spark:
            spark.stop()
            print("\n[OK] SparkSession stopped cleanly.")


# ------------------------------------------------------------------------------
# 💻 STEP 8: COMMAND-LINE EXECUTION ENTRY POINT
# ------------------------------------------------------------------------------
# This block runs when you launch the file directly from the terminal:
#   python services/streaming/train_linear_regression.py
def main():
    """
    Parses CLI flags and executes the Linear Regression training pipeline.
    """
    # Setup CLI flag parser
    parser = argparse.ArgumentParser(
        description="Urbanix PySpark MLlib Linear Regression Training Module"
    )
    # CLI flag for training Parquet path (default: data/train)
    parser.add_argument(
        "--train-path",
        default="data/train",
        help="Path to training Parquet dataset directory"
    )
    # CLI flag for testing Parquet path (default: data/test)
    parser.add_argument(
        "--test-path",
        default="data/test",
        help="Path to testing Parquet dataset directory"
    )
    # Optional CLI flag to save fitted model artifact
    parser.add_argument(
        "--save-path",
        default=None,
        help="Optional destination path to save fitted model artifact"
    )
    # L2 Regularization penalty parameter (default: 0.0 for OLS)
    parser.add_argument(
        "--reg-param",
        type=float,
        default=0.0,
        help="L2 regularization parameter (default: 0.0)"
    )
    # ElasticNet mixing parameter (default: 0.0 for pure L2)
    parser.add_argument(
        "--elastic-net-param",
        type=float,
        default=0.0,
        help="ElasticNet mixing parameter (default: 0.0)"
    )

    # Parse arguments from command line
    args = parser.parse_args()

    # Launch pipeline with parsed configuration
    run_linear_regression_pipeline(
        train_path=args.train_path,
        test_path=args.test_path,
        model_save_path=args.save_path,
        reg_param=args.reg_param,
        elastic_net_param=args.elastic_net_param,
    )


# Standard Python module guard
if __name__ == "__main__":
    main()
