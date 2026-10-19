"""
Urbanix Day 4 — Feature Engineering & Chronological Train/Test Split Pipeline
=============================================================================
This module processes the 5-minute tumbling window traffic feature table
produced by Day 3 Structured Streaming (data/feature_table/traffic) into
model-ready, leakage-free training and testing datasets for Day 5 MLlib modeling.

Key Engineering Rules:
1. Target Definition: 'next_avg_speed_kmph' represents the observed average speed
   in the NEXT consecutive 5-minute window for the identical zone.
2. Continuity Guard: lead() is verified to be exactly 300 seconds ahead; gaps
   are NOT falsely treated as consecutive intervals.
3. Zero Target Leakage: The target is NEVER included in the feature vector.
4. Estimator Hygiene: Any learned transformer (StringIndexer) is fitted on the
   TRAINING split only, preventing test-set distribution leakage.
5. Conservative Boundary Policy:
   - Training rows: target_window_start <= cutoff
   - Testing rows: window_start > cutoff
   - Boundary-crossing rows (window_start <= cutoff AND target_window_start > cutoff)
     are strictly excluded so no training sample predicts a test-period label.
6. Persistence: Saves partitioned Parquet datasets to data/train/ and data/test/,
   alongside a versioned metadata summary (data/split_metadata.json).
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

# Explicitly bind Python executable for PySpark worker subprocesses on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    lead,
    lag,
    hour,
    minute,
    sin,
    cos,
    lit,
    pi,
    unix_timestamp,
    min as spark_min,
    max as spark_max,
    count as spark_count,
    when,
    date_format,
)
from pyspark.ml.feature import VectorAssembler, StringIndexer


def get_spark_session(app_name: str = "UrbanixPrepareTrainingData") -> SparkSession:
    """Builds a local SparkSession configured for deterministic UTC execution."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


class DataQualityError(Exception):
    """Raised when the input feature table violates integrity constraints."""
    pass


class InsufficientCoverageError(Exception):
    """Raised when the input timeline is too short to produce consecutive windows or splits."""
    pass


def prepare_forecasting_dataset(
    spark: SparkSession,
    input_path: str = "data/feature_table/traffic",
    train_output_path: str = "data/train",
    test_output_path: str = "data/test",
    metadata_output_path: str = "data/split_metadata.json",
    train_ratio: float = 0.8,
) -> dict:
    """
    Executes the full Day 4 feature engineering, chronological split,
    and Parquet persistence pipeline.
    """
    print(f"\n" + "=" * 70)
    print("URBANIX DAY 4: FEATURE ENGINEERING & CHRONOLOGICAL SPLIT PIPELINE")
    print("=" * 70)
    print(f"[*] Input feature table : {input_path}")
    print(f"[*] Train output path   : {train_output_path}")
    print(f"[*] Test output path    : {test_output_path}")
    print(f"[*] Metadata path       : {metadata_output_path}")
    print(f"[*] Train/Test ratio    : {train_ratio * 100:.1f}% / {(1 - train_ratio) * 100:.1f}%")

    # -------------------------------------------------------------------------
    # STAGE 1: INGESTION & DATA AUDIT
    # -------------------------------------------------------------------------
    if not os.path.exists(input_path):
        raise DataQualityError(f"Input path does not exist: {input_path}")

    raw_df = spark.read.parquet(input_path)
    total_raw_rows = raw_df.count()
    print(f"[*] Raw feature table row count: {total_raw_rows}")

    if total_raw_rows == 0:
        raise InsufficientCoverageError(
            f"Input table at {input_path} is completely empty. Cannot prepare training data."
        )

    # Required columns in Day 3 traffic feature table
    required_cols = [
        "zone_id",
        "window_start",
        "window_end",
        "avg_speed_kmph",
        "min_speed_kmph",
        "max_speed_kmph",
        "traffic_record_count",
    ]
    missing_cols = [c for c in required_cols if c not in raw_df.columns]
    if missing_cols:
        raise DataQualityError(f"Missing mandatory columns in input table: {missing_cols}")

    # Check for duplicate feature keys (window_start, zone_id)
    distinct_keys = raw_df.select("window_start", "zone_id").distinct().count()
    if distinct_keys != total_raw_rows:
        raise DataQualityError(
            f"Duplicate feature keys detected! Distinct: {distinct_keys}, Total: {total_raw_rows}"
        )

    # Filter out any non-finite or negative speeds
    clean_df = raw_df.filter(
        col("avg_speed_kmph").isNotNull()
        & (col("avg_speed_kmph") > 0.0)
        & (col("avg_speed_kmph") <= 250.0)
    )

    # -------------------------------------------------------------------------
    # STAGE 2: FUTURE-LABEL CONSTRUCTION (ZERO LEAKAGE)
    # -------------------------------------------------------------------------
    # We partition by 'zone_id' and order by 'window_start'.
    # lead('avg_speed_kmph', 1) retrieves the next window's speed for the same zone.
    # We also retrieve lead('window_start', 1) to verify interval continuity.
    zone_time_window = Window.partitionBy("zone_id").orderBy("window_start")

    labeled_candidates = (
        clean_df
        .withColumn("lead_speed", lead("avg_speed_kmph", 1).over(zone_time_window))
        .withColumn("lead_window_start", lead("window_start", 1).over(zone_time_window))
        .withColumn(
            "delta_seconds",
            unix_timestamp("lead_window_start") - unix_timestamp("window_start"),
        )
    )

    # CONTINUITY CHECK:
    # In a 5-minute tumbling window system, consecutive windows must be exactly 300 seconds apart.
    # If delta_seconds != 300, a telemetry gap occurred. We do NOT treat a gap as a consecutive interval.
    # The final window per zone naturally has lead_window_start as NULL (delta_seconds is NULL).
    labeled_df = labeled_candidates.withColumn(
        "next_avg_speed_kmph",
        when(col("delta_seconds") == 300, col("lead_speed")).otherwise(lit(None)),
    ).withColumn(
        "target_window_start",
        when(col("delta_seconds") == 300, col("lead_window_start")).otherwise(lit(None)),
    )

    # Exclude rows without a valid consecutive future target
    supervised_df = labeled_df.filter(
        col("next_avg_speed_kmph").isNotNull() & col("target_window_start").isNotNull()
    )

    eligible_count = supervised_df.count()
    print(f"[*] Valid labeled examples after continuity filtering: {eligible_count}")

    if eligible_count < 2:
        raise InsufficientCoverageError(
            f"Eligible supervised examples ({eligible_count}) is insufficient for train/test splitting. "
            "At least 2 consecutive windows per zone are required."
        )

    # -------------------------------------------------------------------------
    # STAGE 3: INPUT FEATURE ENGINEERING
    # -------------------------------------------------------------------------
    # Features available at observation time t (NEVER using future target information):
    # 1. avg_speed_kmph: Current window average speed
    # 2. min_speed_kmph: Current window minimum speed
    # 3. max_speed_kmph: Current window maximum speed
    # 4. speed_spread_kmph: max - min speed (traffic volatility / burstiness)
    # 5. traffic_record_count: Number of loop detector readings in window (density proxy)
    # 6. hour_of_day: 0-23 derived from current window_start
    # 7. sin_hour, cos_hour: Cyclic time-of-day encoding
    featured_df = (
        supervised_df
        .withColumn(
            "speed_spread_kmph",
            col("max_speed_kmph") - col("min_speed_kmph"),
        )
        .withColumn("hour_of_day", hour(col("window_start")))
        .withColumn("minute_of_hour", minute(col("window_start")))
        .withColumn(
            "sin_hour",
            sin(lit(2.0) * pi() * col("hour_of_day") / lit(24.0)),
        )
        .withColumn(
            "cos_hour",
            cos(lit(2.0) * pi() * col("hour_of_day") / lit(24.0)),
        )
    )

    # -------------------------------------------------------------------------
    # STAGE 4: CHRONOLOGICAL TIME CUTOFF COMPUTATION
    # -------------------------------------------------------------------------
    # Calculate earliest and latest feature window_start in epoch seconds
    time_bounds = featured_df.selectExpr(
        "min(unix_timestamp(window_start)) as t_min_epoch",
        "max(unix_timestamp(window_start)) as t_max_epoch",
        "date_format(min(window_start), 'yyyy-MM-dd HH:mm:ss') as t_min_ts_utc",
        "date_format(max(window_start), 'yyyy-MM-dd HH:mm:ss') as t_max_ts_utc",
    ).first()

    t_min_epoch = time_bounds["t_min_epoch"]
    t_max_epoch = time_bounds["t_max_epoch"]

    if t_min_epoch is None or t_max_epoch is None or t_min_epoch == t_max_epoch:
        raise InsufficientCoverageError(
            f"All eligible records share identical timestamp ({time_bounds['t_min_ts_utc']}). "
            "Cannot perform chronological split."
        )

    cutoff_epoch = t_min_epoch + train_ratio * (t_max_epoch - t_min_epoch)
    cutoff_dt = datetime.fromtimestamp(cutoff_epoch, tz=timezone.utc)
    cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[*] Temporal Horizon (Feature Time): {time_bounds['t_min_ts_utc']} to {time_bounds['t_max_ts_utc']} UTC")
    print(f"[*] Computed {train_ratio*100:.0f}% Chronological Cutoff: {cutoff_str} UTC (epoch {cutoff_epoch})")

    # -------------------------------------------------------------------------
    # STAGE 5: CONSERVATIVE BOUNDARY SPLITTING (ZERO TEMPORAL LEAKAGE)
    # -------------------------------------------------------------------------
    # Training Rule: target_window_start <= cutoff (all training labels occur before or at cutoff)
    # Testing Rule:  window_start > cutoff (all testing features occur strictly after cutoff)
    # Boundary Exclusions: window_start <= cutoff AND target_window_start > cutoff
    raw_train = featured_df.filter(unix_timestamp(col("target_window_start")) <= cutoff_epoch)
    raw_test = featured_df.filter(unix_timestamp(col("window_start")) > cutoff_epoch)

    # Boundary-crossing examples count
    boundary_excluded = featured_df.filter(
        (unix_timestamp(col("window_start")) <= cutoff_epoch)
        & (unix_timestamp(col("target_window_start")) > cutoff_epoch)
    )
    boundary_excluded_count = boundary_excluded.count()
    train_count = raw_train.count()
    test_count = raw_test.count()

    print(f"[*] Boundary-crossing examples excluded: {boundary_excluded_count}")
    print(f"[*] Training candidate rows             : {train_count}")
    print(f"[*] Testing candidate rows              : {test_count}")

    if train_count == 0 or test_count == 0:
        raise InsufficientCoverageError(
            f"Chronological split produced an empty partition! Train: {train_count}, Test: {test_count}. "
            "Ensure the feature table spans sufficient consecutive hours."
        )

    # -------------------------------------------------------------------------
    # STAGE 6: ESTIMATOR HYGIENE — FIT STRINGINDEXER ON TRAIN ONLY
    # -------------------------------------------------------------------------
    # Fitting on the entire dataset would leak test set zone distributions.
    # We fit strictly on raw_train, and transform both raw_train and raw_test.
    zone_indexer = StringIndexer(
        inputCol="zone_id",
        outputCol="zone_index",
        handleInvalid="keep",  # Gracefully handles unseen categories in test set
    )
    indexer_model = zone_indexer.fit(raw_train)
    indexed_train = indexer_model.transform(raw_train)
    indexed_test = indexer_model.transform(raw_test)

    # -------------------------------------------------------------------------
    # STAGE 7: VECTORASSEMBLER (ML-READY FEATURE VECTOR)
    # -------------------------------------------------------------------------
    feature_cols = [
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
    target_col = "next_avg_speed_kmph"

    # Strictly assert that the target is NOT among input feature columns
    assert target_col not in feature_cols, f"CRITICAL LEAKAGE: target {target_col} in feature_cols!"

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="error",  # Fail immediately on unexpected nulls
    )

    assembled_train = assembler.transform(indexed_train)
    assembled_test = assembler.transform(indexed_test)

    # Select final canonical modeling columns
    export_cols = [
        "zone_id",
        "window_start",
        "window_end",
        "target_window_start",
        target_col,
        "features",
    ] + feature_cols

    final_train = assembled_train.select(export_cols)
    final_test = assembled_test.select(export_cols)

    # -------------------------------------------------------------------------
    # STAGE 8: PRE-WRITE LEAKAGE & INTEGRITY ASSERTIONS
    # -------------------------------------------------------------------------
    train_time_stats = final_train.selectExpr(
        "min(unix_timestamp(window_start)) as min_feat_epoch",
        "max(unix_timestamp(target_window_start)) as max_target_epoch",
        "date_format(min(window_start), 'yyyy-MM-dd HH:mm:ss') as min_feat_ts_utc",
        "date_format(max(target_window_start), 'yyyy-MM-dd HH:mm:ss') as max_target_ts_utc",
    ).first()

    test_time_stats = final_test.selectExpr(
        "min(unix_timestamp(window_start)) as min_feat_epoch",
        "max(unix_timestamp(target_window_start)) as max_target_epoch",
        "date_format(min(window_start), 'yyyy-MM-dd HH:mm:ss') as min_feat_ts_utc",
        "date_format(max(target_window_start), 'yyyy-MM-dd HH:mm:ss') as max_target_ts_utc",
    ).first()

    max_train_target_epoch = train_time_stats["max_target_epoch"]
    min_test_feature_epoch = test_time_stats["min_feat_epoch"]

    print(f"[*] Latest Training Target Timestamp  : {train_time_stats['max_target_ts_utc']} UTC (epoch {max_train_target_epoch})")
    print(f"[*] Earliest Testing Feature Timestamp: {test_time_stats['min_feat_ts_utc']} UTC (epoch {min_test_feature_epoch})")

    # ZERO TEMPORAL LEAKAGE ASSERTIONS:
    # 1. The newest target in training must strictly precede the oldest feature in testing!
    assert (
        max_train_target_epoch < min_test_feature_epoch
    ), f"CRITICAL TEMPORAL LEAKAGE! Train target epoch ({max_train_target_epoch}) >= Test feature epoch ({min_test_feature_epoch})"

    # 2. Training target is at or before cutoff
    assert (
        max_train_target_epoch <= cutoff_epoch
    ), f"Training target exceeds cutoff: {max_train_target_epoch} > {cutoff_epoch}"

    # 3. Test feature is strictly after cutoff
    assert (
        min_test_feature_epoch > cutoff_epoch
    ), f"Testing feature precedes cutoff: {min_test_feature_epoch} <= {cutoff_epoch}"

    print("[PASS] Zero Temporal Leakage Verified: max(train.target) < min(test.feature)!")

    # -------------------------------------------------------------------------
    # STAGE 9: PARQUET PERSISTENCE & METADATA EXPORT
    # -------------------------------------------------------------------------
    Path(train_output_path).mkdir(parents=True, exist_ok=True)
    Path(test_output_path).mkdir(parents=True, exist_ok=True)

    final_train.write.mode("overwrite").parquet(train_output_path)
    final_test.write.mode("overwrite").parquet(test_output_path)

    metadata = {
        "status": "PASS",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_feature_table": input_path,
        "train_output_path": train_output_path,
        "test_output_path": test_output_path,
        "train_ratio": train_ratio,
        "cutoff_timestamp_utc": cutoff_str,
        "cutoff_epoch_seconds": cutoff_epoch,
        "train_row_count": final_train.count(),
        "test_row_count": final_test.count(),
        "boundary_excluded_count": boundary_excluded_count,
        "earliest_train_feature_ts_utc": train_time_stats["min_feat_ts_utc"],
        "latest_train_target_ts_utc": train_time_stats["max_target_ts_utc"],
        "earliest_test_feature_ts_utc": test_time_stats["min_feat_ts_utc"],
        "latest_test_target_ts_utc": test_time_stats["max_target_ts_utc"],
        "earliest_train_feature_ts": train_time_stats["min_feat_ts_utc"],
        "latest_train_target_ts": train_time_stats["max_target_ts_utc"],
        "earliest_test_feature_ts": test_time_stats["min_feat_ts_utc"],
        "latest_test_target_ts": test_time_stats["max_target_ts_utc"],
        "feature_columns": feature_cols,
        "target_column": target_col,
        "feature_timestamp_column": "window_start",
        "target_timestamp_column": "target_window_start",
    }

    with open(metadata_output_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"[*] Successfully saved split metadata to {metadata_output_path}")
    print(f"[SUCCESS] Day 4 Dataset Preparation Complete!")
    print("=" * 70 + "\n")
    return metadata


def main():
    parser = argparse.ArgumentParser(
        description="Urbanix Day 4 Feature Engineering & Chronological Split Pipeline"
    )
    parser.add_argument(
        "--input-path",
        default="data/feature_table/traffic",
        help="Path to windowed traffic feature table Parquet directory",
    )
    parser.add_argument(
        "--train-output",
        default="data/train",
        help="Output directory for training Parquet dataset",
    )
    parser.add_argument(
        "--test-output",
        default="data/test",
        help="Output directory for testing Parquet dataset",
    )
    parser.add_argument(
        "--metadata-output",
        default="data/split_metadata.json",
        help="Path to output split metadata JSON",
    )
    parser.add_argument(
        "--train-ratio",
        type=float,
        default=0.8,
        help="Chronological training split ratio (default: 0.8)",
    )
    args = parser.parse_args()

    spark = get_spark_session()
    try:
        prepare_forecasting_dataset(
            spark=spark,
            input_path=args.input_path,
            train_output_path=args.train_output,
            test_output_path=args.test_output,
            metadata_output_path=args.metadata_output,
            train_ratio=args.train_ratio,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
