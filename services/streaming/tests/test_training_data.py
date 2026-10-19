"""
Urbanix Day 4 — Automated Test Suite: Feature Engineering & Chronological Split
================================================================================
Comprehensive unit and integration tests verifying:
1. Future label construction using lead() partitioned by zone.
2. Prevention of cross-zone label bleeding.
3. Telemetry gap detection (no false consecutive labels across gaps).
4. Correct exclusion of the final unlabelled window per zone.
5. Detection of duplicate keys and data quality errors.
6. VectorAssembler correctness and target exclusion.
7. Chronological 80% cutoff computation.
8. Conservative boundary-crossing exclusion and zero temporal leakage.
9. Fitting of StringIndexer on the training partition only.
10. Parquet round-trip persistence and independent verification.
"""

import os
import sys
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
import pytest

# Ensure project root is in sys.path
import sys, os
from pathlib import Path
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
    LongType,
    TimestampType,
)
from pyspark.ml.linalg import VectorUDT

from prepare_training_data import (
    prepare_forecasting_dataset,
    DataQualityError,
    InsufficientCoverageError,
)
from verify_split import verify_split


@pytest.fixture(scope="session")
def spark():
    """Initializes a shared SparkSession for test execution."""
    session = (
        SparkSession.builder.appName("UrbanixDay4Tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def create_synthetic_traffic_data(spark, num_windows=10, zones=["zone_1", "zone_2"], base_time=None):
    """Generates a synthetic multi-window traffic feature DataFrame."""
    if base_time is None:
        base_time = datetime(2012, 3, 1, 0, 0, 0, tzinfo=timezone.utc)

    schema = StructType([
        StructField("zone_id", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("traffic_record_count", LongType(), False),
        StructField("avg_speed_kmph", DoubleType(), False),
        StructField("min_speed_kmph", DoubleType(), False),
        StructField("max_speed_kmph", DoubleType(), False),
    ])

    rows = []
    for w in range(num_windows):
        w_start = base_time + timedelta(seconds=w * 300)
        w_end = w_start + timedelta(seconds=300)
        for z in zones:
            speed = 90.0 + (w % 5) * 5.0 + (10.0 if z == "zone_2" else 0.0)
            rows.append((
                z,
                w_start,
                w_end,
                50,
                speed,
                speed - 15.0,
                speed + 15.0,
            ))

    return spark.createDataFrame(rows, schema)


def test_future_label_construction_per_zone(spark):
    """Verifies that lead() shifts speed by exactly one window per zone."""
    df = create_synthetic_traffic_data(spark, num_windows=4, zones=["zone_1"])
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        train_path = os.path.join(tmp_dir, "train")
        test_path = os.path.join(tmp_dir, "test")
        meta_path = os.path.join(tmp_dir, "meta.json")

        df.write.parquet(input_path)
        meta = prepare_forecasting_dataset(
            spark,
            input_path=input_path,
            train_output_path=train_path,
            test_output_path=test_path,
            metadata_output_path=meta_path,
            train_ratio=0.75,
        )

        assert meta["status"] == "PASS"
        train_df = spark.read.parquet(train_path)
        first_row = train_df.orderBy("window_start").first()
        # For w=0, speed=90.0; for w=1, speed=95.0. Next speed must be 95.0.
        assert first_row["avg_speed_kmph"] == 90.0
        assert first_row["next_avg_speed_kmph"] == 95.0


def test_no_cross_zone_leakage(spark):
    """Asserts that window ordering and labels do not cross zone boundaries."""
    df = create_synthetic_traffic_data(spark, num_windows=5, zones=["zone_1", "zone_2"])
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        train_path = os.path.join(tmp_dir, "train")
        test_path = os.path.join(tmp_dir, "test")
        meta_path = os.path.join(tmp_dir, "meta.json")

        df.write.parquet(input_path)
        prepare_forecasting_dataset(
            spark,
            input_path=input_path,
            train_output_path=train_path,
            test_output_path=test_path,
            metadata_output_path=meta_path,
            train_ratio=0.8,
        )

        train_df = spark.read.parquet(train_path)
        # Verify zone_1 speeds stay in 90-110 range, zone_2 in 100-120 range
        z1_rows = train_df.filter("zone_id = 'zone_1'").collect()
        z2_rows = train_df.filter("zone_id = 'zone_2'").collect()

        for r in z1_rows:
            assert r["avg_speed_kmph"] <= 110.0
            assert r["next_avg_speed_kmph"] <= 110.0
        for r in z2_rows:
            assert r["avg_speed_kmph"] >= 100.0
            assert r["next_avg_speed_kmph"] >= 100.0


def test_last_row_per_zone_dropped(spark):
    """Verifies that the final window of each zone timeline is excluded from supervised learning."""
    df = create_synthetic_traffic_data(spark, num_windows=6, zones=["zone_1"])
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        train_path = os.path.join(tmp_dir, "train")
        test_path = os.path.join(tmp_dir, "test")
        meta_path = os.path.join(tmp_dir, "meta.json")

        df.write.parquet(input_path)
        meta = prepare_forecasting_dataset(
            spark,
            input_path=input_path,
            train_output_path=train_path,
            test_output_path=test_path,
            metadata_output_path=meta_path,
            train_ratio=0.8,
        )

        # 6 raw windows -> exactly 5 supervised pairs. With 1 boundary-crossing excluded, total <= 5.
        total_retained = meta["train_row_count"] + meta["test_row_count"] + meta["boundary_excluded_count"]
        assert total_retained == 5


def test_gap_handling_no_false_consecutive(spark):
    """Verifies that time gaps (> 300s) are NOT assigned consecutive future labels."""
    schema = StructType([
        StructField("zone_id", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("traffic_record_count", LongType(), False),
        StructField("avg_speed_kmph", DoubleType(), False),
        StructField("min_speed_kmph", DoubleType(), False),
        StructField("max_speed_kmph", DoubleType(), False),
    ])

    base = datetime(2012, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    # Window 0: 00:00, Window 1: 00:05 (delta 300s - valid)
    # Window 2: 01:00 (delta 55 minutes - GAP!)
    # Window 3: 01:05 (delta 300s - valid)
    rows = [
        ("zone_1", base, base + timedelta(seconds=300), 50, 90.0, 80.0, 100.0),
        ("zone_1", base + timedelta(seconds=300), base + timedelta(seconds=600), 50, 92.0, 80.0, 100.0),
        ("zone_1", base + timedelta(minutes=60), base + timedelta(minutes=65), 50, 85.0, 75.0, 95.0),
        ("zone_1", base + timedelta(minutes=65), base + timedelta(minutes=70), 50, 88.0, 75.0, 95.0),
        ("zone_1", base + timedelta(minutes=70), base + timedelta(minutes=75), 50, 91.0, 75.0, 95.0),
    ]
    df = spark.createDataFrame(rows, schema)

    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        train_path = os.path.join(tmp_dir, "train")
        test_path = os.path.join(tmp_dir, "test")
        meta_path = os.path.join(tmp_dir, "meta.json")

        df.write.parquet(input_path)
        meta = prepare_forecasting_dataset(
            spark,
            input_path=input_path,
            train_output_path=train_path,
            test_output_path=test_path,
            metadata_output_path=meta_path,
            train_ratio=0.5,
        )

        # In this dataset:
        # Row 0 -> Row 1 (delta 300s, VALID)
        # Row 1 -> Row 2 (delta 3300s, GAP -> DROPPED!)
        # Row 2 -> Row 3 (delta 300s, VALID)
        # Row 3 -> Row 4 (delta 300s, VALID)
        # Row 4 -> None (LAST ROW -> DROPPED)
        # Total valid pairs = 3 (Row 0, Row 2, Row 3).
        total_pairs = meta["train_row_count"] + meta["test_row_count"] + meta["boundary_excluded_count"]
        assert total_pairs == 3


def test_duplicate_feature_key_detection(spark):
    """Asserts that duplicate (window_start, zone_id) tuples raise a DataQualityError."""
    df = create_synthetic_traffic_data(spark, num_windows=5, zones=["zone_1"])
    # Duplicate row 0
    duplicate_df = df.union(df.limit(1))

    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        duplicate_df.write.parquet(input_path)

        with pytest.raises(DataQualityError, match="Duplicate feature keys detected"):
            prepare_forecasting_dataset(
                spark,
                input_path=input_path,
                train_output_path=os.path.join(tmp_dir, "train"),
                test_output_path=os.path.join(tmp_dir, "test"),
                metadata_output_path=os.path.join(tmp_dir, "meta.json"),
            )


def test_target_excluded_from_features(spark):
    """Verifies that the target variable is NEVER included in the feature vector."""
    df = create_synthetic_traffic_data(spark, num_windows=6, zones=["zone_1"])
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        train_path = os.path.join(tmp_dir, "train")
        test_path = os.path.join(tmp_dir, "test")
        meta_path = os.path.join(tmp_dir, "meta.json")

        df.write.parquet(input_path)
        meta = prepare_forecasting_dataset(
            spark,
            input_path=input_path,
            train_output_path=train_path,
            test_output_path=test_path,
            metadata_output_path=meta_path,
        )

        assert "next_avg_speed_kmph" not in meta["feature_columns"]
        train_df = spark.read.parquet(train_path)
        assert "features" in train_df.columns
        assert isinstance(train_df.schema["features"].dataType, VectorUDT)


def test_boundary_crossing_exclusion_and_zero_leakage(spark):
    """Asserts that boundary-crossing rows are excluded and temporal leakage is zero."""
    df = create_synthetic_traffic_data(spark, num_windows=10, zones=["zone_1"])
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        train_path = os.path.join(tmp_dir, "train")
        test_path = os.path.join(tmp_dir, "test")
        meta_path = os.path.join(tmp_dir, "meta.json")

        df.write.parquet(input_path)
        meta = prepare_forecasting_dataset(
            spark,
            input_path=input_path,
            train_output_path=train_path,
            test_output_path=test_path,
            metadata_output_path=meta_path,
            train_ratio=0.8,
        )

        assert meta["boundary_excluded_count"] >= 1
        # Re-verify via verify_split
        success = verify_split(
            spark,
            train_path=train_path,
            test_path=test_path,
            metadata_path=meta_path,
        )
        assert success is True


def test_insufficient_timeline_error(spark):
    """Verifies that an input table with only 1 window raises an InsufficientCoverageError."""
    df = create_synthetic_traffic_data(spark, num_windows=1, zones=["zone_1"])
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        df.write.parquet(input_path)

        with pytest.raises(InsufficientCoverageError):
            prepare_forecasting_dataset(
                spark,
                input_path=input_path,
                train_output_path=os.path.join(tmp_dir, "train"),
                test_output_path=os.path.join(tmp_dir, "test"),
                metadata_output_path=os.path.join(tmp_dir, "meta.json"),
            )


def test_unseen_category_in_test_set(spark):
    """Verifies that StringIndexer handles an unseen zone in the test partition gracefully."""
    base = datetime(2012, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    # First 8 windows: zone_1 only (will land in training partition)
    # Windows 8, 9, 10: zone_new appears only in the future testing period
    schema = StructType([
        StructField("zone_id", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("traffic_record_count", LongType(), False),
        StructField("avg_speed_kmph", DoubleType(), False),
        StructField("min_speed_kmph", DoubleType(), False),
        StructField("max_speed_kmph", DoubleType(), False),
    ])

    rows = []
    for w in range(12):
        w_start = base + timedelta(seconds=w * 300)
        w_end = w_start + timedelta(seconds=300)
        rows.append(("zone_1", w_start, w_end, 50, 95.0, 80.0, 110.0))
        if w >= 9:
            # Unseen zone appears strictly in the test partition
            rows.append(("zone_new_unseen", w_start, w_end, 50, 80.0, 70.0, 90.0))

    df = spark.createDataFrame(rows, schema)
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        train_path = os.path.join(tmp_dir, "train")
        test_path = os.path.join(tmp_dir, "test")
        meta_path = os.path.join(tmp_dir, "meta.json")

        df.write.parquet(input_path)
        meta = prepare_forecasting_dataset(
            spark,
            input_path=input_path,
            train_output_path=train_path,
            test_output_path=test_path,
            metadata_output_path=meta_path,
            train_ratio=0.75,
        )

        assert meta["status"] == "PASS"
        test_df = spark.read.parquet(test_path)
        # Should contain zone_new_unseen encoded without throwing exception
        unseen_rows = test_df.filter("zone_id = 'zone_new_unseen'").count()
        assert unseen_rows > 0


def test_null_and_invalid_speed_filtering(spark):
    """Verifies that speeds > 250 km/h or <= 0.0 are quarantined before label construction."""
    schema = StructType([
        StructField("zone_id", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("traffic_record_count", LongType(), False),
        StructField("avg_speed_kmph", DoubleType(), True),
        StructField("min_speed_kmph", DoubleType(), True),
        StructField("max_speed_kmph", DoubleType(), True),
    ])

    base = datetime(2012, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    rows = [
        ("zone_1", base, base + timedelta(seconds=300), 50, 90.0, 80.0, 100.0),
        ("zone_1", base + timedelta(seconds=300), base + timedelta(seconds=600), 50, -5.0, -10.0, 0.0),  # INVALID
        ("zone_1", base + timedelta(seconds=600), base + timedelta(seconds=900), 50, 95.0, 80.0, 100.0),
        ("zone_1", base + timedelta(seconds=900), base + timedelta(seconds=1200), 50, 999.0, 80.0, 100.0), # INVALID
        ("zone_1", base + timedelta(seconds=1200), base + timedelta(seconds=1500), 50, 98.0, 80.0, 100.0),
        ("zone_1", base + timedelta(seconds=1500), base + timedelta(seconds=1800), 50, 92.0, 80.0, 100.0),
    ]
    df = spark.createDataFrame(rows, schema)

    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        train_path = os.path.join(tmp_dir, "train")
        test_path = os.path.join(tmp_dir, "test")
        meta_path = os.path.join(tmp_dir, "meta.json")

        df.write.parquet(input_path)
        # Clean rows: 0, 2, 4, 5. Gaps exist between 0 and 2, 2 and 4. Only 4 and 5 are consecutive!
        # Since only (4 -> 5) is consecutive, eligible count is 1, which triggers InsufficientCoverageError.
        with pytest.raises(InsufficientCoverageError):
            prepare_forecasting_dataset(
                spark,
                input_path=input_path,
                train_output_path=train_path,
                test_output_path=test_path,
                metadata_output_path=meta_path,
            )


def test_deterministic_split(spark):
    """Verifies that two runs on the same input produce identical train/test partitions."""
    df = create_synthetic_traffic_data(spark, num_windows=8, zones=["zone_1"])
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, "input")
        df.write.parquet(input_path)

        meta1 = prepare_forecasting_dataset(
            spark,
            input_path=input_path,
            train_output_path=os.path.join(tmp_dir, "train1"),
            test_output_path=os.path.join(tmp_dir, "test1"),
            metadata_output_path=os.path.join(tmp_dir, "meta1.json"),
            train_ratio=0.75,
        )

        meta2 = prepare_forecasting_dataset(
            spark,
            input_path=input_path,
            train_output_path=os.path.join(tmp_dir, "train2"),
            test_output_path=os.path.join(tmp_dir, "test2"),
            metadata_output_path=os.path.join(tmp_dir, "meta2.json"),
            train_ratio=0.75,
        )

        assert meta1["train_row_count"] == meta2["train_row_count"]
        assert meta1["test_row_count"] == meta2["test_row_count"]
        assert meta1["cutoff_timestamp_utc"] == meta2["cutoff_timestamp_utc"]
