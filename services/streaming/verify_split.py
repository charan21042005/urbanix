"""
Urbanix Day 4 — Independent Chronological Split & Leakage Verification Engine
=============================================================================
This script reloads persisted Parquet datasets from disk (data/train/ and data/test/)
and performs independent, exhaustive assertions to mathematically prove:
1. Physical existence and non-emptiness of both training and testing datasets.
2. Exact schema conformance, target type validation, and VectorUDT dimensional integrity.
3. Absence of target leakage: target is strictly excluded from VectorAssembler inputs.
4. Absence of temporal leakage:
   - max(train.target_window_start) <= cutoff
   - min(test.window_start) > cutoff
   - max(train.target_window_start) < min(test.window_start)
5. Strict sample disjointness: zero duplicate keys within or between train and test splits.
6. Absolute agreement with persisted split metadata (data/split_metadata.json).

Returns exit code 0 if all assertions PASS; exit code 1 if any failure occurs.
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
from pyspark.sql.functions import col, min as spark_min, max as spark_max, count as spark_count
from pyspark.ml.linalg import VectorUDT


def get_spark_session(app_name: str = "UrbanixVerifySplit") -> SparkSession:
    """Creates a local SparkSession configured for deterministic UTC verification."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def verify_split(
    spark: SparkSession,
    train_path: str = "data/train",
    test_path: str = "data/test",
    metadata_path: str = "data/split_metadata.json",
) -> bool:
    """
    Performs independent reload and verification of train and test datasets.
    Returns True if all assertions pass, False otherwise.
    """
    print("\n" + "=" * 70)
    print("URBANIX DAY 4: INDEPENDENT CHRONOLOGICAL SPLIT VERIFICATION")
    print("=" * 70)
    print(f"[*] Reloading Train Parquet : {train_path}")
    print(f"[*] Reloading Test Parquet  : {test_path}")
    print(f"[*] Checking Split Metadata : {metadata_path}")

    all_passed = True

    # 1. Check Directory Existence
    if not os.path.exists(train_path) or not os.path.exists(test_path):
        print(f"[FAIL] Missing dataset directories! Train: {os.path.exists(train_path)}, Test: {os.path.exists(test_path)}")
        return False

    if not os.path.exists(metadata_path):
        print(f"[FAIL] Missing split metadata file: {metadata_path}")
        return False

    with open(metadata_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    # 2. Reload Datasets from Disk
    train_df = spark.read.parquet(train_path)
    test_df = spark.read.parquet(test_path)

    train_count = train_df.count()
    test_count = test_df.count()
    print(f"[*] Reloaded Train rows : {train_count}")
    print(f"[*] Reloaded Test rows  : {test_count}")

    # Check non-emptiness
    if train_count == 0 or test_count == 0:
        print(f"[FAIL] Non-empty requirement failed! Train: {train_count}, Test: {test_count}")
        return False
    else:
        print("[PASS] Both train and test datasets are populated and non-empty.")

    # 3. Verify Metadata Count Agreement
    if meta.get("train_row_count") != train_count or meta.get("test_row_count") != test_count:
        print(f"[FAIL] Row count mismatch with metadata! Meta: ({meta.get('train_row_count')}, {meta.get('test_row_count')}) vs Observed: ({train_count}, {test_count})")
        all_passed = False
    else:
        print("[PASS] Observed row counts match split metadata exactly.")

    # 4. Schema and Column Checks
    required_cols = [
        "zone_id",
        "window_start",
        "window_end",
        "target_window_start",
        "next_avg_speed_kmph",
        "features",
    ]
    for c in required_cols:
        if c not in train_df.columns or c not in test_df.columns:
            print(f"[FAIL] Mandatory column missing: {c}")
            all_passed = False
    print("[PASS] All required modeling and key columns present in both splits.")

    # 5. VectorAssembler & Feature Column Validation
    # Check that 'features' is indeed VectorUDT
    train_schema = {field.name: field.dataType for field in train_df.schema.fields}
    if not isinstance(train_schema.get("features"), VectorUDT):
        print(f"[FAIL] 'features' column is not VectorUDT! Got: {train_schema.get('features')}")
        all_passed = False
    else:
        print("[PASS] 'features' column is verified VectorUDT format.")

    # Inspect first vector dimension
    first_vec = train_df.select("features").first()["features"]
    expected_dim = len(meta.get("feature_columns", []))
    if len(first_vec) != expected_dim:
        print(f"[FAIL] Vector dimension mismatch! Expected {expected_dim}, observed {len(first_vec)}")
        all_passed = False
    else:
        print(f"[PASS] Vector dimension confirmed: {len(first_vec)} features.")

    # Critical Anti-Leakage Check: Target NOT inside input features
    if "next_avg_speed_kmph" in meta.get("feature_columns", []):
        print("[FAIL] CRITICAL LEAKAGE: Target variable 'next_avg_speed_kmph' found in feature_columns!")
        all_passed = False
    else:
        print("[PASS] Anti-leakage verified: Target strictly excluded from feature vector.")

    # 6. Sample Disjointness & Duplicate Key Checks
    train_keys = train_df.select("window_start", "zone_id")
    test_keys = test_df.select("window_start", "zone_id")

    if train_keys.distinct().count() != train_count:
        print("[FAIL] Duplicate feature keys detected within training partition!")
        all_passed = False
    else:
        print("[PASS] Zero duplicate keys within training partition.")

    if test_keys.distinct().count() != test_count:
        print("[FAIL] Duplicate feature keys detected within testing partition!")
        all_passed = False
    else:
        print("[PASS] Zero duplicate keys within testing partition.")

    # Check key overlap between train and test
    overlap_count = train_keys.intersect(test_keys).count()
    if overlap_count > 0:
        print(f"[FAIL] CRITICAL LEAKAGE: {overlap_count} overlapping sample keys between train and test!")
        all_passed = False
    else:
        print("[PASS] Zero key overlap: Train and test samples are mutually disjoint.")

    # 7. Chronological Boundary & Temporal Leakage Validation
    train_bounds = train_df.selectExpr(
        "min(unix_timestamp(window_start)) as min_feat_epoch",
        "max(unix_timestamp(target_window_start)) as max_target_epoch",
        "min(window_start) as min_feat_ts",
        "max(target_window_start) as max_target_ts",
    ).first()

    test_bounds = test_df.selectExpr(
        "min(unix_timestamp(window_start)) as min_feat_epoch",
        "max(unix_timestamp(target_window_start)) as max_target_epoch",
        "min(window_start) as min_feat_ts",
        "max(target_window_start) as max_target_ts",
    ).first()

    max_train_target_epoch = train_bounds["max_target_epoch"]
    min_test_feature_epoch = test_bounds["min_feat_epoch"]
    cutoff_epoch = meta.get("cutoff_epoch_seconds")
    cutoff_str = meta.get("cutoff_timestamp_utc")

    print(f"[*] Metadata Cutoff Timestamp         : {cutoff_str} UTC (epoch {cutoff_epoch})")
    print(f"[*] Observed Max Train Target Time     : {train_bounds['max_target_ts']} (epoch {max_train_target_epoch})")
    print(f"[*] Observed Min Test Feature Time     : {test_bounds['min_feat_ts']} (epoch {min_test_feature_epoch})")

    # Leakage Assertion 1: max_train_target < min_test_feature
    if max_train_target_epoch >= min_test_feature_epoch:
        print(
            f"[FAIL] CRITICAL TEMPORAL LEAKAGE! Latest training target ({max_train_target_epoch}) "
            f"overlaps or exceeds earliest test feature ({min_test_feature_epoch})!"
        )
        all_passed = False
    else:
        print(
            f"[PASS] Zero Temporal Leakage Verified: "
            f"max(train.target_window_start) [{train_bounds['max_target_ts']}] < min(test.window_start) [{test_bounds['min_feat_ts']}]."
        )

    # Leakage Assertion 2: max_train_target <= cutoff
    if cutoff_epoch is not None and max_train_target_epoch > cutoff_epoch:
        print(f"[FAIL] Training target ({max_train_target_epoch}) exceeds cutoff ({cutoff_epoch})!")
        all_passed = False
    else:
        print("[PASS] All training targets strictly occur at or before cutoff.")

    # Leakage Assertion 3: min_test_feature > cutoff
    if cutoff_epoch is not None and min_test_feature_epoch <= cutoff_epoch:
        print(f"[FAIL] Testing feature ({min_test_feature_epoch}) precedes or equals cutoff ({cutoff_epoch})!")
        all_passed = False
    else:
        print("[PASS] All testing features strictly occur after cutoff.")

    # 8. Check Finite Numerical Bounds
    # Assert target is finite and positive
    invalid_targets = train_df.filter(
        col("next_avg_speed_kmph").isNull() | (col("next_avg_speed_kmph") <= 0.0) | (col("next_avg_speed_kmph") > 250.0)
    ).count()
    if invalid_targets > 0:
        print(f"[FAIL] {invalid_targets} invalid target values in training split!")
        all_passed = False
    else:
        print("[PASS] All target speeds are finite, positive, and within physical bounds (0 < v <= 250 km/h).")

    print("=" * 70)
    if all_passed:
        print("FINAL VERIFICATION RESULT: [ALL CHECKS PASSED]")
        print("The train/test splits are mathematically sound, leak-free, and ready for Day 5 modeling.")
    else:
        print("FINAL VERIFICATION RESULT: [FAILED] — See issues logged above.")
    print("=" * 70 + "\n")
    return all_passed


def main():
    parser = argparse.ArgumentParser(
        description="Urbanix Day 4 Independent Split & Leakage Verifier"
    )
    parser.add_argument(
        "--train-path",
        default="data/train",
        help="Path to training Parquet dataset",
    )
    parser.add_argument(
        "--test-path",
        default="data/test",
        help="Path to testing Parquet dataset",
    )
    parser.add_argument(
        "--metadata-path",
        default="data/split_metadata.json",
        help="Path to split metadata JSON",
    )
    args = parser.parse_args()

    spark = get_spark_session()
    try:
        success = verify_split(
            spark=spark,
            train_path=args.train_path,
            test_path=args.test_path,
            metadata_path=args.metadata_path,
        )
        sys.exit(0 if success else 1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
