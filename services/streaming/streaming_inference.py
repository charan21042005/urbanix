"""
Urbanix — Day 6 Live MLlib Streaming Inference Engine
=====================================================
This module provides the live inference transformation layer for integrating
the batch-trained Apache Spark MLlib Linear Regression model (from Day 5)
into the active Spark Structured Streaming pipeline (from Day 3).

Core Concepts & Architecture:
-----------------------------
1. Model Deserialization:
   The fitted LinearRegressionModel is loaded ONCE on the Spark driver at job startup
   (outside the per-batch function). Loading once prevents heavy disk I/O, repeated JSON
   metadata parsing, and weight vector deserialization on every 10-second micro-batch trigger.

2. Feature Parity (Zero Divergence from Training):
   The inference feature vector reproduces the EXACT 9-dimensional schema established
   in Day 4 (prepare_training_data.py) and evaluated in Day 5 (train_evaluate.py):
     [0] avg_speed_kmph       : Window average velocity (km/h)
     [1] min_speed_kmph       : Window minimum velocity (km/h)
     [2] max_speed_kmph       : Window maximum velocity (km/h)
     [3] speed_spread_kmph    : max_speed_kmph - min_speed_kmph (velocity burstiness)
     [4] traffic_record_count : Total sensor observations in window (density proxy)
     [5] hour_of_day          : Hour extracted from window_start (0 to 23)
     [6] sin_hour             : Cyclic diurnal encoding: sin(2 * pi * hour / 24)
     [7] cos_hour             : Cyclic diurnal encoding: cos(2 * pi * hour / 24)
     [8] zone_index           : Deterministic categorical spatial offset:
                                zone_1 -> 0.0, zone_2 -> 1.0, zone_3 -> 2.0,
                                zone_4 -> 3.0, zone_5 -> 4.0, other -> 5.0

3. Why foreachBatch instead of direct .transform() on Streaming DataFrame?
   Apache Spark MLlib estimators and models (like LinearRegressionModel) operate exclusively
   on static DataFrames. Passing a streaming DataFrame directly to model.transform() fails
   because MLlib algorithms are not designed for incremental streaming state generation.
   Structured Streaming's `foreachBatch` sink provides the canonical bridge: it delivers
   each finalized micro-batch as a standard static DataFrame, enabling native MLlib
   transformation and custom multi-sink persistence.

4. At-Least-Once Delivery Semantics:
   Structured Streaming maintains source offsets in its durable checkpoint directory.
   When combined with foreachBatch and a Parquet file append sink, this architecture
   guarantees at-least-once delivery: in the event of an ungraceful crash, uncommitted
   micro-batches are re-processed upon query restart from the last committed checkpoint offset.

BEGINNER'S GUIDE:
In real-time AI pipelines, you train a model offline on historical data (Day 5), then deploy it
to make live predictions on incoming live data (Day 6).
This script takes aggregated 5-minute traffic windows as they arrive in real-time, calculates the same
9 features the model was trained on, asks the MLlib LinearRegressionModel for the predicted future speed,
and writes the predictions to Parquet so the GraphX routing engine can re-route cars!
"""

# --- Standard Library Imports ---
import os        # File path resolution and directory creation
import sys       # System arguments and runtime interpreter path
import logging   # Structured operational logging for distributed streaming micro-batches
from pathlib import Path  # Object-oriented filesystem path manipulation
from typing import Dict, Any, Optional, Callable  # Type annotations

# Bind driver python executable for worker subprocesses on Windows to prevent pickling errors
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# --- Apache PySpark SQL & MLlib Imports ---
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,                # Column reference expression
    hour,               # Extracts hour of day (0-23) from timestamp
    sin,                # Mathematical sine function for cyclic encoding
    cos,                # Mathematical cosine function for cyclic encoding
    lit,                # Creates literal constant column values
    pi,                 # Mathematical Pi constant
    when,               # Conditional SQL CASE/WHEN expression builder
    round as spark_round,# Rounds floating-point numbers to specified decimal places
    current_timestamp,  # Returns the current system timestamp
    isnan,              # Checks if a numeric column contains NaN (Not a Number)
)
# VectorAssembler combines multiple numeric columns into a single VectorUDT feature column
from pyspark.ml.feature import VectorAssembler
# LinearRegressionModel represents a pre-trained linear regression model artifact
from pyspark.ml.regression import LinearRegressionModel

# Initialize logger for streaming inference events
logger = logging.getLogger("UrbanixInference")

# Canonical 9-feature ordering matching Day 4 split metadata and Day 5 model weights
INFERENCE_FEATURE_COLS = [
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

# Deterministic spatial indexing matching StringIndexer fitted on Day 4 training partition
ZONE_INDEX_MAPPING: Dict[str, float] = {
    "zone_1": 0.0,
    "zone_2": 1.0,
    "zone_3": 2.0,
    "zone_4": 3.0,
    "zone_5": 4.0,
}


def load_forecasting_model(model_path: str = "models/linear_regression") -> LinearRegressionModel:
    """
    Deserializes the trained Spark MLlib LinearRegressionModel from disk.
    
    This function is executed ONCE at streaming job startup on the driver node.
    It asserts that the target model directory and its MLlib metadata exist,
    preventing silent failures or fallbacks to untrained heuristics.
    
    Args:
        model_path (str): Filesystem path to the native Spark MLlib model directory.
        
    Returns:
        LinearRegressionModel: Loaded model ready for batch transformation.
        
    Raises:
        FileNotFoundError: If the model path or metadata does not exist.
        ValueError: If the model expects a different number of features than 9.
    """
    model_dir = Path(model_path)
    metadata_dir = model_dir / "metadata"
    
    # Verify that the model artifact folder and internal metadata subfolder exist
    if not model_dir.exists() or not metadata_dir.exists():
        raise FileNotFoundError(
            f"Trained model artifact not found at '{model_path}'. "
            "Ensure Day 5 training has been executed via 'python services/streaming/train_evaluate.py'."
        )
    
    logger.info(f"Loading native Spark MLlib model from: {model_path}")
    # Load the persisted model using Spark MLlib's native loader
    model = LinearRegressionModel.load(str(model_path))
    
    # Verify that the model expects exactly 9 features matching our feature schema
    num_features = model.numFeatures
    if num_features != len(INFERENCE_FEATURE_COLS):
        raise ValueError(
            f"Model feature dimension mismatch: expected {len(INFERENCE_FEATURE_COLS)} features, "
            f"but loaded model expects {num_features} features."
        )
    
    logger.info(
        f"Successfully loaded LinearRegressionModel: numFeatures={num_features}, "
        f"intercept={model.intercept:.4f}"
    )
    return model


def prepare_traffic_features(df: DataFrame) -> DataFrame:
    """
    Derives the mathematical features required by the 9-dimensional ML model
    from windowed traffic telemetry and assembles them into a VectorUDT column.
    
    Transformations:
      1. speed_spread_kmph = max_speed_kmph - min_speed_kmph
      2. hour_of_day = hour(window_start)
      3. sin_hour = sin(2 * pi * hour_of_day / 24)
      4. cos_hour = cos(2 * pi * hour_of_day / 24)
      5. zone_index = deterministic spatial corridor mapping
      6. VectorAssembler -> 'features'
      
    Args:
        df (DataFrame): Aggregated windowed traffic DataFrame containing zone_id, window_start,
                        avg_speed_kmph, min_speed_kmph, max_speed_kmph, traffic_record_count.
            
    Returns:
        DataFrame: Augmented with intermediate feature columns and the assembled 'features' vector.
    """
    # Enforce mandatory input columns
    required_inputs = [
        "zone_id",
        "window_start",
        "avg_speed_kmph",
        "min_speed_kmph",
        "max_speed_kmph",
        "traffic_record_count",
    ]
    for c in required_inputs:
        if c not in df.columns:
            raise ValueError(f"Input DataFrame missing mandatory feature column: '{c}'")
    
    # 1. Deterministic zone index expression (encodes categorical zone_id as float 0.0 to 4.0)
    zone_expr = (
        when(col("zone_id") == "zone_1", 0.0)
        .when(col("zone_id") == "zone_2", 1.0)
        .when(col("zone_id") == "zone_3", 2.0)
        .when(col("zone_id") == "zone_4", 3.0)
        .when(col("zone_id") == "zone_5", 4.0)
        .otherwise(5.0)
    )
    
    # 2. Derive speed spread and cyclic temporal encodings
    featured_df = (
        # Speed spread measures traffic volatility / variance within the 5-minute window
        df.withColumn("speed_spread_kmph", col("max_speed_kmph") - col("min_speed_kmph"))
        # Extract integer hour (0-23)
        .withColumn("hour_of_day", hour(col("window_start")))
        # Sinusoidal diurnal encoding: smoothly wraps 23:59 to 00:00
        .withColumn(
            "sin_hour",
            sin(lit(2.0) * lit(3.141592653589793) * col("hour_of_day") / lit(24.0)),
        )
        # Cosine diurnal encoding: orthogonal coordinate
        .withColumn(
            "cos_hour",
            cos(lit(2.0) * lit(3.141592653589793) * col("hour_of_day") / lit(24.0)),
        )
        .withColumn("zone_index", zone_expr)
    )
    
    # 3. Assemble all 9 features into a single VectorUDT column called 'features'
    assembler = VectorAssembler(
        inputCols=INFERENCE_FEATURE_COLS,
        outputCol="features",
        handleInvalid="error",  # Strictly error out if any input feature is null or NaN
    )
    assembled_df = assembler.transform(featured_df)
    return assembled_df


def predict_traffic_speed(
    df: DataFrame,
    model: LinearRegressionModel,
    prediction_col_name: str = "predicted_speed_kmph",
) -> DataFrame:
    """
    Applies the fitted Linear Regression model to the assembled features,
    producing calibrated velocity forecasts for the subsequent 5-minute interval.
    
    Args:
        df (DataFrame): DataFrame containing the assembled 'features' vector column.
        model (LinearRegressionModel): Pre-loaded LinearRegressionModel instance.
        prediction_col_name (str): Name of the primary predicted velocity column.
        
    Returns:
        DataFrame: Augmented with predicted speed columns and model metadata.
    """
    # If feature vector is not already present, build it
    if "features" not in df.columns:
        df = prepare_traffic_features(df)
    
    # MLlib adds a new column named 'prediction'
    raw_predictions = model.transform(df)
    
    # Round predictions to 2 decimal places and provide alias for downstream compatibility
    result_df = (
        raw_predictions.withColumn(
            prediction_col_name,
            spark_round(col("prediction"), 2),
        )
        # Provide duplicate alias 'predicted_speed' for backwards compatibility
        .withColumn("predicted_speed", col(prediction_col_name))
        # Document the model provenance
        .withColumn("prediction_model", lit("LinearRegression_OLS"))
        # Timestamp the inference execution time
        .withColumn("prediction_timestamp", current_timestamp())
    )
    
    # Ensure invalid predictions (null, NaN, or non-finite) are filtered out so that
    # malformed records cannot silently enter downstream sinks
    clean_predictions = result_df.filter(
        col(prediction_col_name).isNotNull() & ~isnan(col(prediction_col_name))
    )
    return clean_predictions


def make_traffic_batch_processor(
    model: LinearRegressionModel,
    output_path: str,
    prediction_col_name: str = "predicted_speed_kmph",
) -> Callable[[DataFrame, int], None]:
    """
    Factory function generating a thread-safe foreachBatch callback function.
    
    The returned callback is invoked by Spark Structured Streaming on each
    finalized micro-batch. Inside the callback, batch_df is a static DataFrame,
    enabling native MLlib transformation and durable Parquet append persistence.
    
    Args:
        model (LinearRegressionModel): Pre-loaded LinearRegressionModel on the driver.
        output_path (str): Destination directory for predicted traffic features.
        prediction_col_name (str): Output column name for predicted velocity.
        
    Returns:
        Callable process_batch(batch_df, batch_id) suitable for foreachBatch.
    """
    # Ensure destination directory exists on disk
    os.makedirs(output_path, exist_ok=True)
    
    def process_batch(batch_df: DataFrame, batch_id: int):
        """
        Processes a single micro-batch:
          1. Skips processing if batch is empty.
          2. Derives 9-dimensional feature vectors.
          3. Evaluates LinearRegressionModel.transform().
          4. Appends predicted records to the Parquet feature store.
        """
        # Micro-batch empty check (avoids empty parquet part-file pollution)
        if batch_df.rdd.isEmpty():
            logger.info(f"[Batch {batch_id}] Micro-batch is empty. Skipping inference.")
            return
        
        batch_count = batch_df.count()
        logger.info(f"[Batch {batch_id}] Ingested {batch_count} windowed traffic rows for scoring.")
        
        try:
            # Apply feature engineering and live MLlib inference
            scored_df = predict_traffic_speed(
                batch_df,
                model=model,
                prediction_col_name=prediction_col_name,
            )
            
            # Select clean output columns for downstream consumption (GraphX & Dashboard)
            output_cols = [
                "zone_id",
                "window_start",
                "window_end",
                "traffic_record_count",
                "avg_speed_kmph",
                "min_speed_kmph",
                "max_speed_kmph",
                "predicted_speed_kmph",
                "predicted_speed",
                "prediction_model",
                "prediction_timestamp",
            ]
            
            # Preserve optional zone metadata if present
            optional_cols = [
                "zone_name",
                "mapping_type",
                "mapping_description",
                "min_lat",
                "max_lat",
                "min_lon",
                "max_lon",
                "features",
            ]
            for opt in optional_cols:
                if opt in scored_df.columns:
                    output_cols.append(opt)
            
            final_df = scored_df.select([c for c in output_cols if c in scored_df.columns])
            
            # Write to Parquet feature store in Append mode
            final_df.write.mode("append").parquet(output_path)
            
            logger.info(
                f"[Batch {batch_id}] Successfully appended {batch_count} predicted traffic rows "
                f"to '{output_path}'"
            )
            
        except Exception as e:
            logger.error(f"[Batch {batch_id}] CRITICAL ERROR during streaming inference: {e}", exc_info=True)
            raise e
            
    return process_batch
