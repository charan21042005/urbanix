#!/usr/bin/env python3
"""
Urbanix — Day 3 Spark Structured Streaming Job
Processes real-time IoT telemetry from three independent Kafka topics:
  - traffic-topic  -> 5-min tumbling window -> data/feature_table/traffic/
  - aqi-topic      -> 5-min tumbling window -> data/feature_table/aqi/
  - weather-topic  -> 5-min tumbling window -> data/feature_table/weather/

Key Architectural Principles:
1. Three Independent Streams:
   Preserves valid observations independently across streams. No forced inner join
   across streams with disjoint historical calendar ranges (March vs October 2012).
2. Event-Time Watermarking:
   Uses genuine event-time timestamps from sensor payloads with a 2-minute watermark
   and 5-minute tumbling windows in Append output mode.
3. Static Zone Metadata Join:
   Relational enrichment with data/grid_zones.csv without multiplying observations.
4. Separate Checkpoints:
   Independent checkpoint directories for each query under infra/checkpoints/.
5. Honest Schema Semantics:
   No fabricated fields (no synthetic vehicle_count, no artificial co2).
"""

import os
import sys
import logging
import argparse
import signal
from typing import Dict, Any, Optional

# Ensure worker python matches driver python runtime
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    avg,
    min as spark_min,
    max as spark_max,
    round as spark_round,
    when,
    lit,
)

# Relative or package import for schemas
try:
    from schemas import (
        TRAFFIC_SCHEMA,
        AQI_SCHEMA,
        WEATHER_SCHEMA,
        GRID_ZONES_SCHEMA,
        TIMESTAMP_FORMAT,
        DEFAULT_TIMEZONE,
    )
except ImportError:
    from services.streaming.schemas import (
        TRAFFIC_SCHEMA,
        AQI_SCHEMA,
        WEATHER_SCHEMA,
        GRID_ZONES_SCHEMA,
        TIMESTAMP_FORMAT,
        DEFAULT_TIMEZONE,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Urbanix-Streaming] %(message)s",
)
logger = logging.getLogger("UrbanixStreaming")


def build_spark_session(
    app_name: str = "Urbanix-StreamingJob-Day3",
    master: Optional[str] = None,
    kafka_package: Optional[str] = None,
) -> SparkSession:
    """
    Initializes and configures the SparkSession for Structured Streaming with Kafka.
    Defaults to Scala 2.13 Kafka connector matching host Spark 3.5.1 installation.
    """
    if kafka_package is None:
        kafka_package = os.environ.get(
            "SPARK_KAFKA_PACKAGE",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1",
        )

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", DEFAULT_TIMEZONE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", kafka_package)
    )

    if master:
        builder = builder.master(master)
    elif "SPARK_MASTER_URL" in os.environ:
        builder = builder.master(os.environ["SPARK_MASTER_URL"])

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Initialized SparkSession: version={spark.version}, appName={app_name}")
    return spark


def load_grid_zones(spark: SparkSession, zones_path: str) -> DataFrame:
    """
    Loads and validates the static grid zone metadata table.
    Enforces uniqueness on zone_id to ensure relational joins do not multiply observations.
    """
    if not os.path.exists(zones_path):
        raise FileNotFoundError(f"Static grid zones file not found: {zones_path}")

    zones_df = (
        spark.read.option("header", "true")
        .schema(GRID_ZONES_SCHEMA)
        .csv(zones_path)
    )

    total_count = zones_df.count()
    distinct_count = zones_df.select("zone_id").distinct().count()

    if total_count != distinct_count:
        raise ValueError(
            f"Integrity error: Duplicate zone_ids detected in {zones_path} "
            f"({total_count} rows vs {distinct_count} distinct zones)"
        )

    logger.info(f"Loaded {total_count} validated grid zones from {zones_path}")
    return zones_df


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "latest",
) -> DataFrame:
    """
    Reads a raw streaming DataFrame from Kafka with source metadata.
    """
    logger.info(f"Connecting to Kafka stream: topic='{topic}', bootstrap='{bootstrap_servers}', offsets='{starting_offsets}'")
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    return raw_stream


# ---------------------------------------------------------------------------
# Stream Parsers & Validations
# ---------------------------------------------------------------------------

def parse_and_validate_traffic(raw_stream: DataFrame) -> DataFrame:
    """
    Parses JSON payload for traffic stream, enforces physical bounds,
    and converts timestamps to event_time.
    Physical rule: 0.0 < speed_kmph <= 250.0 (filters sensor blackouts and unphysical spikes).
    """
    parsed = (
        raw_stream.selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as payload_json",
            "timestamp as kafka_timestamp",
        )
        .withColumn("data", from_json(col("payload_json"), TRAFFIC_SCHEMA))
        .select(
            col("data.zone_id").alias("zone_id"),
            col("data.sensor_id").alias("sensor_id"),
            to_timestamp(col("data.timestamp"), TIMESTAMP_FORMAT).alias("event_time"),
            col("data.speed_kmph").alias("speed_kmph"),
            col("data.speed_mph").alias("speed_mph"),
            col("data.unit").alias("unit"),
            col("data.source").alias("source"),
            col("kafka_timestamp"),
        )
    )

    # Filter malformed records and enforce physical validity
    valid_stream = parsed.filter(
        col("event_time").isNotNull()
        & col("zone_id").isNotNull()
        & col("speed_kmph").isNotNull()
        & (col("speed_kmph") > 0.0)
        & (col("speed_kmph") <= 250.0)
    )
    return valid_stream


def parse_and_validate_aqi(raw_stream: DataFrame) -> DataFrame:
    """
    Parses JSON payload for air quality stream, enforces physical bounds,
    and converts timestamps to event_time.
    Physical rule: 0.0 <= pm25 <= 1000.0 (EPA bounds).
    """
    parsed = (
        raw_stream.selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as payload_json",
            "timestamp as kafka_timestamp",
        )
        .withColumn("data", from_json(col("payload_json"), AQI_SCHEMA))
        .select(
            col("data.zone_id").alias("zone_id"),
            col("data.station_id").alias("station_id"),
            col("data.station_name").alias("station_name"),
            col("data.city").alias("city"),
            to_timestamp(col("data.timestamp"), TIMESTAMP_FORMAT).alias("event_time"),
            col("data.pm25").alias("pm25"),
            col("data.aqi").alias("aqi"),
            col("data.unit").alias("unit"),
            col("data.source").alias("source"),
            col("kafka_timestamp"),
        )
    )

    valid_stream = parsed.filter(
        col("event_time").isNotNull()
        & col("zone_id").isNotNull()
        & col("pm25").isNotNull()
        & (col("pm25") >= 0.0)
        & (col("pm25") <= 1000.0)
    )
    return valid_stream


def parse_and_validate_weather(raw_stream: DataFrame) -> DataFrame:
    """
    Parses JSON payload for weather stream, enforces terrestrial bounds,
    and converts timestamps to event_time.
    Physical rule: -50.0 <= temperature_c <= 60.0 and 0.0 <= humidity_pct <= 100.0.
    """
    parsed = (
        raw_stream.selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as payload_json",
            "timestamp as kafka_timestamp",
        )
        .withColumn("data", from_json(col("payload_json"), WEATHER_SCHEMA))
        .select(
            col("data.zone_id").alias("zone_id"),
            col("data.city").alias("city"),
            to_timestamp(col("data.timestamp"), TIMESTAMP_FORMAT).alias("event_time"),
            col("data.temperature_c").alias("temperature_c"),
            col("data.temperature_k").alias("temperature_k"),
            col("data.humidity_pct").alias("humidity_pct"),
            col("data.temperature_unit").alias("temperature_unit"),
            col("data.humidity_unit").alias("humidity_unit"),
            col("data.source").alias("source"),
            col("kafka_timestamp"),
        )
    )

    valid_stream = parsed.filter(
        col("event_time").isNotNull()
        & col("zone_id").isNotNull()
        & col("temperature_c").isNotNull()
        & (col("temperature_c") >= -50.0)
        & (col("temperature_c") <= 60.0)
        & col("humidity_pct").isNotNull()
        & (col("humidity_pct") >= 0.0)
        & (col("humidity_pct") <= 100.0)
    )
    return valid_stream


# ---------------------------------------------------------------------------
# Aggregation & Enrichment Pipelines
# ---------------------------------------------------------------------------

def aggregate_traffic(
    traffic_stream: DataFrame,
    grid_zones: DataFrame,
    window_duration: str = "5 minutes",
    watermark_delay: str = "2 minutes",
) -> DataFrame:
    """
    Applies event-time watermark, tumbling window aggregation, and zone metadata enrichment.
    Output metrics:
      - traffic_record_count: total valid sensor readings in window
      - avg_speed_kmph, min_speed_kmph, max_speed_kmph
    """
    watermarked = traffic_stream.withWatermark("event_time", watermark_delay)

    aggregated = (
        watermarked.groupBy(
            window(col("event_time"), window_duration),
            col("zone_id"),
        )
        .agg(
            count("*").alias("traffic_record_count"),
            spark_round(avg("speed_kmph"), 2).alias("avg_speed_kmph"),
            spark_round(spark_min("speed_kmph"), 2).alias("min_speed_kmph"),
            spark_round(spark_max("speed_kmph"), 2).alias("max_speed_kmph"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone_id"),
            col("traffic_record_count"),
            col("avg_speed_kmph"),
            col("min_speed_kmph"),
            col("max_speed_kmph"),
        )
    )

    # Relational join with static zone metadata
    enriched = aggregated.join(grid_zones, on="zone_id", how="left")
    return enriched


def aggregate_aqi(
    aqi_stream: DataFrame,
    grid_zones: DataFrame,
    window_duration: str = "5 minutes",
    watermark_delay: str = "2 minutes",
) -> DataFrame:
    """
    Applies event-time watermark, tumbling window aggregation, and zone metadata enrichment.
    Output metrics:
      - aqi_record_count: total valid PM2.5 observations in window
      - avg_pm25, min_pm25, max_pm25, avg_aqi
    """
    watermarked = aqi_stream.withWatermark("event_time", watermark_delay)

    aggregated = (
        watermarked.groupBy(
            window(col("event_time"), window_duration),
            col("zone_id"),
        )
        .agg(
            count("*").alias("aqi_record_count"),
            spark_round(avg("pm25"), 2).alias("avg_pm25"),
            spark_round(spark_min("pm25"), 2).alias("min_pm25"),
            spark_round(spark_max("pm25"), 2).alias("max_pm25"),
            spark_round(avg("aqi"), 1).alias("avg_aqi"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone_id"),
            col("aqi_record_count"),
            col("avg_pm25"),
            col("min_pm25"),
            col("max_pm25"),
            col("avg_aqi"),
        )
    )

    enriched = aggregated.join(grid_zones, on="zone_id", how="left")
    return enriched


def aggregate_weather(
    weather_stream: DataFrame,
    grid_zones: DataFrame,
    window_duration: str = "5 minutes",
    watermark_delay: str = "2 minutes",
) -> DataFrame:
    """
    Applies event-time watermark, tumbling window aggregation, and zone metadata enrichment.
    Output metrics:
      - weather_record_count: total valid meteorological observations in window
      - avg_temperature_c, min_temperature_c, max_temperature_c, avg_humidity_pct
    """
    watermarked = weather_stream.withWatermark("event_time", watermark_delay)

    aggregated = (
        watermarked.groupBy(
            window(col("event_time"), window_duration),
            col("zone_id"),
        )
        .agg(
            count("*").alias("weather_record_count"),
            spark_round(avg("temperature_c"), 2).alias("avg_temperature_c"),
            spark_round(spark_min("temperature_c"), 2).alias("min_temperature_c"),
            spark_round(spark_max("temperature_c"), 2).alias("max_temperature_c"),
            spark_round(avg("humidity_pct"), 1).alias("avg_humidity_pct"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone_id"),
            col("weather_record_count"),
            col("avg_temperature_c"),
            col("min_temperature_c"),
            col("max_temperature_c"),
            col("avg_humidity_pct"),
        )
    )

    enriched = aggregated.join(grid_zones, on="zone_id", how="left")
    return enriched


# ---------------------------------------------------------------------------
# Query Sinks & Execution
# ---------------------------------------------------------------------------

def start_parquet_query(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    query_name: str,
    trigger_interval: str = "10 seconds",
    available_now: bool = False,
):
    """
    Starts a Structured Streaming query writing windowed aggregates to a Parquet directory
    in Append mode with an independent checkpoint directory.
    """
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)

    logger.info(
        f"Starting query '{query_name}': output='{output_path}', checkpoint='{checkpoint_path}', "
        f"trigger={'availableNow' if available_now else trigger_interval}"
    )

    writer = (
        df.writeStream.queryName(query_name)
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
    )

    if available_now:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime=trigger_interval).start()

    return query


def run_pipeline(
    bootstrap_servers: str,
    traffic_topic: str,
    aqi_topic: str,
    weather_topic: str,
    zones_path: str,
    output_root: str,
    checkpoint_root: str,
    window_duration: str = "5 minutes",
    watermark_delay: str = "2 minutes",
    trigger_interval: str = "10 seconds",
    starting_offsets: str = "latest",
    spark_master: Optional[str] = None,
    await_termination: bool = True,
    available_now: bool = False,
    kafka_package: Optional[str] = None,
):
    """
    Main orchestration function: initializes Spark, reads 3 Kafka topics, applies
    validation, windowing, watermarking, zone joining, and starts 3 Parquet queries.
    """
    spark = build_spark_session(master=spark_master, kafka_package=kafka_package)
    grid_zones = load_grid_zones(spark, zones_path)

    # 1. Traffic Pipeline
    raw_traffic = read_kafka_stream(spark, bootstrap_servers, traffic_topic, starting_offsets)
    valid_traffic = parse_and_validate_traffic(raw_traffic)
    traffic_features = aggregate_traffic(valid_traffic, grid_zones, window_duration, watermark_delay)

    # 2. AQI Pipeline
    raw_aqi = read_kafka_stream(spark, bootstrap_servers, aqi_topic, starting_offsets)
    valid_aqi = parse_and_validate_aqi(raw_aqi)
    aqi_features = aggregate_aqi(valid_aqi, grid_zones, window_duration, watermark_delay)

    # 3. Weather Pipeline
    raw_weather = read_kafka_stream(spark, bootstrap_servers, weather_topic, starting_offsets)
    valid_weather = parse_and_validate_weather(raw_weather)
    weather_features = aggregate_weather(valid_weather, grid_zones, window_duration, watermark_delay)

    # Start 3 Independent Streaming Queries
    traffic_query = start_parquet_query(
        traffic_features,
        os.path.join(output_root, "traffic"),
        os.path.join(checkpoint_root, "traffic"),
        query_name="TrafficFeatureQuery",
        trigger_interval=trigger_interval,
        available_now=available_now,
    )

    aqi_query = start_parquet_query(
        aqi_features,
        os.path.join(output_root, "aqi"),
        os.path.join(checkpoint_root, "aqi"),
        query_name="AqiFeatureQuery",
        trigger_interval=trigger_interval,
        available_now=available_now,
    )

    weather_query = start_parquet_query(
        weather_features,
        os.path.join(output_root, "weather"),
        os.path.join(checkpoint_root, "weather"),
        query_name="WeatherFeatureQuery",
        trigger_interval=trigger_interval,
        available_now=available_now,
    )

    active_queries = [traffic_query, aqi_query, weather_query]

    def handle_shutdown(signum, frame):
        logger.info(f"Received shutdown signal ({signum}). Stopping active streaming queries...")
        for q in active_queries:
            try:
                if q.isActive:
                    q.stop()
                    logger.info(f"Query {q.name} stopped.")
            except Exception as e:
                logger.warning(f"Error stopping query {q.name}: {e}")
        spark.stop()
        logger.info("Spark session stopped. Exiting cleanly.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info("All 3 Urbanix streaming queries successfully started!")

    if await_termination:
        logger.info("Awaiting termination of streaming queries...")
        for q in active_queries:
            q.awaitTermination()

    return active_queries, spark


def main():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    parser = argparse.ArgumentParser(description="Urbanix Day 3 Spark Structured Streaming Job")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument("--traffic-topic", default="traffic-topic", help="Kafka traffic topic")
    parser.add_argument("--aqi-topic", default="aqi-topic", help="Kafka AQI topic")
    parser.add_argument("--weather-topic", default="weather-topic", help="Kafka weather topic")
    parser.add_argument(
        "--zones-path",
        default=os.path.join(repo_root, "data", "grid_zones.csv"),
        help="Path to static grid_zones.csv",
    )
    parser.add_argument(
        "--output-root",
        default=os.path.join(repo_root, "data", "feature_table"),
        help="Root directory for Parquet feature tables",
    )
    parser.add_argument(
        "--checkpoint-root",
        default=os.path.join(repo_root, "infra", "checkpoints"),
        help="Root directory for query checkpoints",
    )
    parser.add_argument("--window-duration", default="5 minutes", help="Tumbling window duration")
    parser.add_argument("--watermark-delay", default="2 minutes", help="Event-time watermark delay")
    parser.add_argument("--trigger-interval", default="10 seconds", help="Micro-batch trigger interval")
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["latest", "earliest"],
        help="Kafka starting offsets policy (default: latest)",
    )
    parser.add_argument("--master", default=None, help="Spark master URL (optional)")
    parser.add_argument("--no-await", action="store_true", help="Do not block on awaitTermination")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Use trigger(availableNow=True) to process all available Kafka data and exit cleanly",
    )
    parser.add_argument(
        "--kafka-package",
        default=None,
        help="Custom maven artifact coordinate for spark-sql-kafka connector",
    )

    args = parser.parse_args()

    run_pipeline(
        bootstrap_servers=args.bootstrap_servers,
        traffic_topic=args.traffic_topic,
        aqi_topic=args.aqi_topic,
        weather_topic=args.weather_topic,
        zones_path=args.zones_path,
        output_root=args.output_root,
        checkpoint_root=args.checkpoint_root,
        window_duration=args.window_duration,
        watermark_delay=args.watermark_delay,
        trigger_interval=args.trigger_interval,
        starting_offsets=args.starting_offsets,
        spark_master=args.master,
        await_termination=not args.no_await,
        available_now=args.available_now,
        kafka_package=args.kafka_package,
    )


if __name__ == "__main__":
    main()
