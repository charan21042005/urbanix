"""
Urbanix — Day 3 Streaming Pipeline Test Suite
Deterministic unit and integration tests verifying:
- Schema adherence to Day 2 producer payloads
- Malformed JSON, missing fields, and timestamp validation
- Physical measurement boundaries and invalid value filtering
- Valid zero-value vs blackout handling
- 5-minute window assignment at boundaries (12:00, 12:04:59, 12:05:00)
- Aggregation semantics (speed, PM2.5, temperature, humidity, counts)
- Zone metadata uniqueness, relational join cardinality, and unmatched zone handling
- Absence of fabricated fields (no vehicle_count, no co2)
- Independent stream processing (streams operate without temporal coupling)
- Parquet schema and round-trip consistency
"""

import os
import json
import pytest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)
from pyspark.sql.functions import col, to_timestamp, date_format

# Schema imports
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

current_dir = os.path.dirname(os.path.abspath(__file__))
streaming_dir = os.path.dirname(current_dir)
if streaming_dir not in sys.path:
    sys.path.insert(0, streaming_dir)

from schemas import (
    TRAFFIC_SCHEMA,
    AQI_SCHEMA,
    WEATHER_SCHEMA,
    GRID_ZONES_SCHEMA,
    TIMESTAMP_FORMAT,
    DEFAULT_TIMEZONE,
)
from streaming_job import (
    load_grid_zones,
    parse_and_validate_traffic,
    parse_and_validate_aqi,
    parse_and_validate_weather,
    aggregate_traffic,
    aggregate_aqi,
    aggregate_weather,
)


@pytest.fixture(scope="session")
def spark():
    """
    Creates an isolated local SparkSession for deterministic pipeline testing.
    """
    session = (
        SparkSession.builder.appName("Urbanix-Streaming-Tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", DEFAULT_TIMEZONE)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_grid_zones(spark, tmp_path_factory):
    """
    Creates a validated static grid_zones.csv lookup dataframe.
    """
    tmp_dir = tmp_path_factory.mktemp("zones")
    zones_file = os.path.join(tmp_dir, "grid_zones.csv")
    with open(zones_file, "w", encoding="utf-8") as f:
        f.write("zone_id,zone_name,mapping_type,mapping_description,min_lat,max_lat,min_lon,max_lon\n")
        f.write("zone_1,Downtown Core,Operational,Downtown LA Core,,,, \n")
        f.write("zone_2,San Fernando Valley,Operational,San Fernando Corridor,,,, \n")
        f.write("zone_3,San Gabriel Foothills,Operational,East Foothills,,,, \n")
        f.write("zone_4,South Central Corridor,Operational,South Central Corridor,,,, \n")
        f.write("zone_5,Harbor & Outer Ring,Operational,Long Beach Harbor,,,, \n")

    return load_grid_zones(spark, zones_file)


# ---------------------------------------------------------------------------
# 1. Payload Parsing & Schema Validation Tests
# ---------------------------------------------------------------------------

def test_traffic_valid_json_parsing(spark):
    """
    Test 1: Confirms valid traffic JSON payload parses into expected columns and types.
    """
    valid_payload = json.dumps({
        "zone_id": "zone_1",
        "sensor_id": "773869",
        "timestamp": "2012-03-01T12:00:00",
        "speed_kmph": 96.56,
        "speed_mph": 60.0,
        "unit": "km/h",
        "source": "METR-LA (Caltrans PeMS)"
    })
    raw_df = spark.createDataFrame(
        [(b"key1", valid_payload.encode("utf-8"), datetime(2026, 1, 1, 12, 0, 0))],
        ["key", "value", "timestamp"],
    )

    valid_df = parse_and_validate_traffic(raw_df)
    rows = valid_df.collect()

    assert len(rows) == 1
    row = rows[0]
    assert row["zone_id"] == "zone_1"
    assert row["sensor_id"] == "773869"
    assert row["speed_kmph"] == 96.56
    assert row["speed_mph"] == 60.0
    assert row["unit"] == "km/h"
    event_time_str = valid_df.select(date_format("event_time", "yyyy-MM-dd HH:mm:ss").alias("et")).collect()[0]["et"]
    assert event_time_str == "2012-03-01 12:00:00"


def test_aqi_valid_json_parsing(spark):
    """
    Test 2: Confirms valid AQI JSON payload parses into expected columns and types.
    """
    valid_payload = json.dumps({
        "zone_id": "zone_2",
        "station_id": "1002",
        "station_name": "Burbank",
        "city": "Los Angeles",
        "timestamp": "2012-03-01T00:00:00",
        "pm25": 18.5,
        "aqi": 64,
        "unit": "ug/m3",
        "source": "US EPA AirData (AQS)"
    })
    raw_df = spark.createDataFrame(
        [(b"key2", valid_payload.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0))],
        ["key", "value", "timestamp"],
    )

    valid_df = parse_and_validate_aqi(raw_df)
    rows = valid_df.collect()

    assert len(rows) == 1
    row = rows[0]
    assert row["zone_id"] == "zone_2"
    assert row["station_id"] == "1002"
    assert row["pm25"] == 18.5
    assert row["aqi"] == 64
    event_time_str = valid_df.select(date_format("event_time", "yyyy-MM-dd HH:mm:ss").alias("et")).collect()[0]["et"]
    assert event_time_str == "2012-03-01 00:00:00"


def test_weather_valid_json_parsing(spark):
    """
    Test 3: Confirms valid weather JSON payload parses into expected columns and types.
    """
    valid_payload = json.dumps({
        "zone_id": "zone_3",
        "city": "Los Angeles",
        "timestamp": "2012-10-01T13:00:00",
        "temperature_c": 19.35,
        "temperature_k": 292.5,
        "humidity_pct": 77.0,
        "temperature_unit": "Celsius",
        "humidity_unit": "Percent",
        "source": "Historical Hourly Weather Data (OpenWeatherMap)"
    })
    raw_df = spark.createDataFrame(
        [(b"key3", valid_payload.encode("utf-8"), datetime(2026, 1, 1, 13, 0, 0))],
        ["key", "value", "timestamp"],
    )

    valid_df = parse_and_validate_weather(raw_df)
    rows = valid_df.collect()

    assert len(rows) == 1
    row = rows[0]
    assert row["zone_id"] == "zone_3"
    assert row["temperature_c"] == 19.35
    assert row["humidity_pct"] == 77.0
    event_time_str = valid_df.select(date_format("event_time", "yyyy-MM-dd HH:mm:ss").alias("et")).collect()[0]["et"]
    assert event_time_str == "2012-10-01 13:00:00"


# ---------------------------------------------------------------------------
# 2. Malformed, Missing, and Invalid Measurement Filtering
# ---------------------------------------------------------------------------

def test_malformed_json_handling(spark):
    """
    Test 4: Malformed JSON strings are safely handled and do not crash the pipeline.
    """
    corrupt_payload = "{ this is not valid json: 123"
    raw_df = spark.createDataFrame(
        [(b"key", corrupt_payload.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0))],
        ["key", "value", "timestamp"],
    )

    valid_traffic = parse_and_validate_traffic(raw_df)
    assert valid_traffic.count() == 0

    valid_aqi = parse_and_validate_aqi(raw_df)
    assert valid_aqi.count() == 0

    valid_weather = parse_and_validate_weather(raw_df)
    assert valid_weather.count() == 0


def test_missing_required_fields(spark):
    """
    Test 5: Payloads missing required keys (e.g. missing zone_id or missing timestamp) are filtered.
    """
    missing_zone = json.dumps({
        "sensor_id": "773869",
        "timestamp": "2012-03-01T12:00:00",
        "speed_kmph": 60.0
    })
    missing_timestamp = json.dumps({
        "zone_id": "zone_1",
        "sensor_id": "773869",
        "speed_kmph": 60.0
    })
    raw_df = spark.createDataFrame(
        [
            (b"k1", missing_zone.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)),
            (b"k2", missing_timestamp.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)),
        ],
        ["key", "value", "timestamp"],
    )

    valid_traffic = parse_and_validate_traffic(raw_df)
    assert valid_traffic.count() == 0


def test_invalid_timestamps(spark):
    """
    Test 6: Invalid timestamp strings (unparseable date formats) evaluate to null event_time and are filtered.
    """
    bad_time = json.dumps({
        "zone_id": "zone_1",
        "sensor_id": "773869",
        "timestamp": "not-a-timestamp",
        "speed_kmph": 55.0
    })
    raw_df = spark.createDataFrame(
        [(b"k", bad_time.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0))],
        ["key", "value", "timestamp"],
    )

    valid_traffic = parse_and_validate_traffic(raw_df)
    assert valid_traffic.count() == 0


def test_physical_boundary_filtering(spark):
    """
    Test 7: Unphysical measurements are quarantined/filtered out:
    - Traffic speed <= 0 or > 250 km/h
    - AQI pm25 < 0 or > 1000 ug/m3
    - Weather temp < -50 or > 60 C; humidity < 0 or > 100%
    """
    traffic_unphysical = [
        json.dumps({"zone_id": "zone_1", "sensor_id": "s1", "timestamp": "2012-03-01T12:00:00", "speed_kmph": -10.0}),
        json.dumps({"zone_id": "zone_1", "sensor_id": "s2", "timestamp": "2012-03-01T12:00:00", "speed_kmph": 500.0}),
    ]
    raw_traffic = spark.createDataFrame(
        [(b"k", p.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)) for p in traffic_unphysical],
        ["key", "value", "timestamp"],
    )
    assert parse_and_validate_traffic(raw_traffic).count() == 0

    aqi_unphysical = [
        json.dumps({"zone_id": "zone_1", "station_id": "1", "timestamp": "2012-03-01T00:00:00", "pm25": -5.0}),
        json.dumps({"zone_id": "zone_1", "station_id": "2", "timestamp": "2012-03-01T00:00:00", "pm25": 1500.0}),
    ]
    raw_aqi = spark.createDataFrame(
        [(b"k", p.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)) for p in aqi_unphysical],
        ["key", "value", "timestamp"],
    )
    assert parse_and_validate_aqi(raw_aqi).count() == 0

    weather_unphysical = [
        json.dumps({"zone_id": "zone_1", "timestamp": "2012-10-01T12:00:00", "temperature_c": -70.0, "humidity_pct": 50.0}),
        json.dumps({"zone_id": "zone_1", "timestamp": "2012-10-01T12:00:00", "temperature_c": 20.0, "humidity_pct": 120.0}),
    ]
    raw_weather = spark.createDataFrame(
        [(b"k", p.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)) for p in weather_unphysical],
        ["key", "value", "timestamp"],
    )
    assert parse_and_validate_weather(raw_weather).count() == 0


def test_valid_zero_measurements_where_applicable(spark):
    """
    Test 8: Distinguishes valid zeros from sensor blackout:
    - AQI PM2.5 = 0.0 is physically possible (clean air) and must be preserved.
    - Weather temperature_c = 0.0 (freezing point) is physically valid and preserved.
    - Traffic speed = 0.0 represents unobserved/blackout in METR-LA and is excluded.
    """
    aqi_zero = json.dumps({
        "zone_id": "zone_1",
        "station_id": "1103",
        "timestamp": "2012-03-01T00:00:00",
        "pm25": 0.0,
        "aqi": 0
    })
    raw_aqi = spark.createDataFrame(
        [(b"k", aqi_zero.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0))],
        ["key", "value", "timestamp"],
    )
    assert parse_and_validate_aqi(raw_aqi).count() == 1

    weather_zero = json.dumps({
        "zone_id": "zone_1",
        "timestamp": "2012-10-01T12:00:00",
        "temperature_c": 0.0,
        "humidity_pct": 50.0
    })
    raw_weather = spark.createDataFrame(
        [(b"k", weather_zero.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0))],
        ["key", "value", "timestamp"],
    )
    assert parse_and_validate_weather(raw_weather).count() == 1

    traffic_zero = json.dumps({
        "zone_id": "zone_1",
        "sensor_id": "773869",
        "timestamp": "2012-03-01T12:00:00",
        "speed_kmph": 0.0
    })
    raw_traffic = spark.createDataFrame(
        [(b"k", traffic_zero.encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0))],
        ["key", "value", "timestamp"],
    )
    assert parse_and_validate_traffic(raw_traffic).count() == 0


# ---------------------------------------------------------------------------
# 3. Window Boundary Assignment Tests
# ---------------------------------------------------------------------------

def test_five_minute_window_boundaries(spark, sample_grid_zones):
    """
    Test 9: Verifies 5-minute tumbling window assignment at boundaries:
    - 12:00:00 -> window [12:00:00, 12:05:00)
    - 12:04:59 -> window [12:00:00, 12:05:00)
    - 12:05:00 -> window [12:05:00, 12:10:00)
    """
    records = [
        {"zone_id": "zone_1", "sensor_id": "s1", "timestamp": "2012-03-01T12:00:00", "speed_kmph": 50.0},
        {"zone_id": "zone_1", "sensor_id": "s2", "timestamp": "2012-03-01T12:04:59", "speed_kmph": 60.0},
        {"zone_id": "zone_1", "sensor_id": "s3", "timestamp": "2012-03-01T12:05:00", "speed_kmph": 70.0},
    ]
    raw_df = spark.createDataFrame(
        [(b"k", json.dumps(r).encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)) for r in records],
        ["key", "value", "timestamp"],
    )
    valid_df = parse_and_validate_traffic(raw_df)
    aggregated = (
        aggregate_traffic(valid_df, sample_grid_zones, window_duration="5 minutes", watermark_delay="2 minutes")
        .withColumn("ws_str", date_format("window_start", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("we_str", date_format("window_end", "yyyy-MM-dd HH:mm:ss"))
    )
    results = aggregated.collect()

    assert len(results) == 2

    window_1 = [r for r in results if r["ws_str"] == "2012-03-01 12:00:00"][0]
    assert window_1["we_str"] == "2012-03-01 12:05:00"
    assert window_1["traffic_record_count"] == 2
    assert window_1["avg_speed_kmph"] == 55.0

    window_2 = [r for r in results if r["ws_str"] == "2012-03-01 12:05:00"][0]
    assert window_2["we_str"] == "2012-03-01 12:10:00"
    assert window_2["traffic_record_count"] == 1
    assert window_2["avg_speed_kmph"] == 70.0


# ---------------------------------------------------------------------------
# 4. Aggregation and Measurement Verification Tests
# ---------------------------------------------------------------------------

def test_aggregation_calculations(spark, sample_grid_zones):
    """
    Test 10 & 11: Validates min, max, average, and record counts for each stream.
    """
    # Traffic: speeds 40.0 and 80.0 -> avg 60.0, min 40.0, max 80.0, count 2
    traffic_recs = [
        {"zone_id": "zone_1", "sensor_id": "s1", "timestamp": "2012-03-01T10:00:00", "speed_kmph": 40.0},
        {"zone_id": "zone_1", "sensor_id": "s2", "timestamp": "2012-03-01T10:02:00", "speed_kmph": 80.0},
    ]
    t_raw = spark.createDataFrame(
        [(b"k", json.dumps(r).encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)) for r in traffic_recs],
        ["key", "value", "timestamp"],
    )
    t_agg = aggregate_traffic(parse_and_validate_traffic(t_raw), sample_grid_zones).collect()[0]
    assert t_agg["traffic_record_count"] == 2
    assert t_agg["avg_speed_kmph"] == 60.0
    assert t_agg["min_speed_kmph"] == 40.0
    assert t_agg["max_speed_kmph"] == 80.0

    # AQI: PM2.5 10.0 and 30.0 -> avg 20.0, min 10.0, max 30.0, count 2
    aqi_recs = [
        {"zone_id": "zone_2", "station_id": "1", "timestamp": "2012-03-01T00:00:00", "pm25": 10.0, "aqi": 40},
        {"zone_id": "zone_2", "station_id": "2", "timestamp": "2012-03-01T00:01:00", "pm25": 30.0, "aqi": 80},
    ]
    a_raw = spark.createDataFrame(
        [(b"k", json.dumps(r).encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)) for r in aqi_recs],
        ["key", "value", "timestamp"],
    )
    a_agg = aggregate_aqi(parse_and_validate_aqi(a_raw), sample_grid_zones).collect()[0]
    assert a_agg["aqi_record_count"] == 2
    assert a_agg["avg_pm25"] == 20.0
    assert a_agg["min_pm25"] == 10.0
    assert a_agg["max_pm25"] == 30.0
    assert a_agg["avg_aqi"] == 60.0

    # Weather: temp 15.0 and 25.0 -> avg 20.0; humidity 60.0 and 80.0 -> avg 70.0
    weather_recs = [
        {"zone_id": "zone_3", "timestamp": "2012-10-01T12:00:00", "temperature_c": 15.0, "humidity_pct": 60.0},
        {"zone_id": "zone_3", "timestamp": "2012-10-01T12:02:00", "temperature_c": 25.0, "humidity_pct": 80.0},
    ]
    w_raw = spark.createDataFrame(
        [(b"k", json.dumps(r).encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)) for r in weather_recs],
        ["key", "value", "timestamp"],
    )
    w_agg = aggregate_weather(parse_and_validate_weather(w_raw), sample_grid_zones).collect()[0]
    assert w_agg["weather_record_count"] == 2
    assert w_agg["avg_temperature_c"] == 20.0
    assert w_agg["min_temperature_c"] == 15.0
    assert w_agg["max_temperature_c"] == 25.0
    assert w_agg["avg_humidity_pct"] == 70.0


# ---------------------------------------------------------------------------
# 5. Zone Relational Join Integrity & Unmatched Zone Handling
# ---------------------------------------------------------------------------

def test_zone_metadata_uniqueness_and_join(spark, sample_grid_zones):
    """
    Test 12 & 13: Static zone join maintains 1-to-1 cardinality and handles unknown zones via left join.
    """
    recs = [
        {"zone_id": "zone_1", "sensor_id": "s1", "timestamp": "2012-03-01T12:00:00", "speed_kmph": 50.0},
        {"zone_id": "zone_99", "sensor_id": "s99", "timestamp": "2012-03-01T12:00:00", "speed_kmph": 55.0},
    ]
    raw_df = spark.createDataFrame(
        [(b"k", json.dumps(r).encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)) for r in recs],
        ["key", "value", "timestamp"],
    )
    agg = aggregate_traffic(parse_and_validate_traffic(raw_df), sample_grid_zones).collect()

    assert len(agg) == 2

    # Known zone has enriched metadata
    zone_1_row = [r for r in agg if r["zone_id"] == "zone_1"][0]
    assert zone_1_row["zone_name"] == "Downtown Core"

    # Unknown zone has null zone_name but record is preserved
    zone_99_row = [r for r in agg if r["zone_id"] == "zone_99"][0]
    assert zone_99_row["zone_name"] is None
    assert zone_99_row["traffic_record_count"] == 1


# ---------------------------------------------------------------------------
# 6. Absence of Fabricated Fields & Independent Stream Integrity
# ---------------------------------------------------------------------------

def test_absence_of_fabricated_fields(spark, sample_grid_zones):
    """
    Test 14: Confirms no synthetic vehicle_count or co2 columns exist in any feature table.
    """
    # Check traffic columns
    t_df = aggregate_traffic(
        spark.createDataFrame([], StructType([
            StructField("zone_id", StringType()),
            StructField("sensor_id", StringType()),
            StructField("event_time", TimestampType()),
            StructField("speed_kmph", DoubleType()),
            StructField("speed_mph", DoubleType()),
            StructField("unit", StringType()),
            StructField("source", StringType()),
            StructField("kafka_timestamp", TimestampType()),
        ])),
        sample_grid_zones,
    )
    assert "vehicle_count" not in t_df.columns
    assert "co2" not in t_df.columns

    # Check AQI columns
    a_df = aggregate_aqi(
        spark.createDataFrame([], StructType([
            StructField("zone_id", StringType()),
            StructField("station_id", StringType()),
            StructField("station_name", StringType()),
            StructField("city", StringType()),
            StructField("event_time", TimestampType()),
            StructField("pm25", DoubleType()),
            StructField("aqi", IntegerType()),
            StructField("unit", StringType()),
            StructField("source", StringType()),
            StructField("kafka_timestamp", TimestampType()),
        ])),
        sample_grid_zones,
    )
    assert "co2" not in a_df.columns

    # Check weather columns
    w_df = aggregate_weather(
        spark.createDataFrame([], StructType([
            StructField("zone_id", StringType()),
            StructField("city", StringType()),
            StructField("event_time", TimestampType()),
            StructField("temperature_c", DoubleType()),
            StructField("temperature_k", DoubleType()),
            StructField("humidity_pct", DoubleType()),
            StructField("temperature_unit", StringType()),
            StructField("humidity_unit", StringType()),
            StructField("source", StringType()),
            StructField("kafka_timestamp", TimestampType()),
        ])),
        sample_grid_zones,
    )
    assert "vehicle_count" not in w_df.columns
    assert "co2" not in w_df.columns


def test_independent_stream_processing(spark, sample_grid_zones):
    """
    Test 15: Confirms each stream produces valid aggregates independently,
    even when event timestamps belong to entirely different calendar months (March vs October 2012).
    """
    traffic_event = {"zone_id": "zone_1", "sensor_id": "s1", "timestamp": "2012-03-01T12:00:00", "speed_kmph": 65.0}
    weather_event = {"zone_id": "zone_1", "timestamp": "2012-10-01T12:00:00", "temperature_c": 22.0, "humidity_pct": 55.0}

    t_raw = spark.createDataFrame([(b"k", json.dumps(traffic_event).encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0))], ["key", "value", "timestamp"])
    w_raw = spark.createDataFrame([(b"k", json.dumps(weather_event).encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0))], ["key", "value", "timestamp"])

    t_agg = aggregate_traffic(parse_and_validate_traffic(t_raw), sample_grid_zones).withColumn("ws_str", date_format("window_start", "yyyy-MM-dd HH:mm:ss")).collect()
    w_agg = aggregate_weather(parse_and_validate_weather(w_raw), sample_grid_zones).withColumn("ws_str", date_format("window_start", "yyyy-MM-dd HH:mm:ss")).collect()

    assert len(t_agg) == 1
    assert t_agg[0]["ws_str"] == "2012-03-01 12:00:00"

    assert len(w_agg) == 1
    assert w_agg[0]["ws_str"] == "2012-10-01 12:00:00"


# ---------------------------------------------------------------------------
# 7. Parquet Round-Trip Persistence Test
# ---------------------------------------------------------------------------

def test_parquet_schema_and_roundtrip(spark, sample_grid_zones, tmp_path):
    """
    Test 16: Confirms aggregated dataframe writes to Parquet and reloads with intact schema and values.
    """
    recs = [
        {"zone_id": "zone_1", "sensor_id": "s1", "timestamp": "2012-03-01T12:00:00", "speed_kmph": 75.0},
    ]
    raw_df = spark.createDataFrame(
        [(b"k", json.dumps(r).encode("utf-8"), datetime(2026, 1, 1, 0, 0, 0)) for r in recs],
        ["key", "value", "timestamp"],
    )
    agg_df = aggregate_traffic(parse_and_validate_traffic(raw_df), sample_grid_zones)

    parquet_dir = str(tmp_path / "traffic_parquet")
    agg_df.write.parquet(parquet_dir)

    reloaded_df = spark.read.parquet(parquet_dir)
    assert reloaded_df.count() == 1
    row = reloaded_df.collect()[0]
    assert row["zone_id"] == "zone_1"
    assert row["zone_name"] == "Downtown Core"
    assert row["traffic_record_count"] == 1
    assert row["avg_speed_kmph"] == 75.0
