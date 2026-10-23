#!/usr/bin/env python3
# ==============================================================================
# Urbanix — Day 3 & Day 6 Spark Structured Streaming Production Job
# ==============================================================================
# Processes real-time IoT telemetry from three independent Kafka topics:
#   - traffic-topic  -> 5-min tumbling window -> data/feature_table/traffic/
#   - aqi-topic      -> 5-min tumbling window -> data/feature_table/aqi/
#   - weather-topic  -> 5-min tumbling window -> data/feature_table/weather/
#
# Educational Guide for Beginners:
# --------------------------------
# What is Apache Spark Structured Streaming?
# Structured Streaming is a scalable and fault-tolerant stream processing engine built
# on the Spark SQL engine. It allows developers to express streaming computations
# the same way they would express batch computations on static DataFrames.
#
# Key Architectural Concepts in this Job:
# 1. Three Independent Streams:
#    In real-world data engineering, different sensors publish data at different rates
#    and time periods (e.g., METR-LA traffic from March 2012 vs EPA AQI data). Attempting
#    an inner-join between raw streams with disjoint calendar times would drop 100% of rows!
#    Instead, Urbanix ingests and windows each stream independently into its own feature sink.
# 2. Event-Time Watermarking (withWatermark):
#    Network packets and edge sensors can experience latency or arrive out of order.
#    A "watermark" is a threshold that tells Spark: "Only wait up to 2 minutes for late-arriving
#    data. Any data older than (max_event_time - 2 minutes) will be safely discarded."
# 3. 5-Minute Tumbling Windows:
#    A tumbling window is a non-overlapping, fixed-duration time bucket (e.g., 07:00-07:05,
#    07:05-07:10). All sensor readings falling into that bucket are aggregated together.
# 4. Static-to-Stream Join:
#    Each micro-batch of windowed aggregates is enriched with static geographic metadata
#    from 'data/grid_zones.csv' (latitude, longitude, zone name).
# 5. MLlib Model Scoring via foreachBatch:
#    Machine Learning algorithms (like LinearRegressionModel) require static DataFrames.
#    Structured Streaming's 'foreachBatch' mechanism passes each materialized micro-batch
#    to a custom scoring function that adds predicted traffic speed before saving to Parquet.
# ==============================================================================

# Standard Python system and operating system libraries
import os          # Operating system environment variables and file paths
import sys         # Python runtime information and sys.path manipulation
import logging     # Formatted logging output for monitoring streaming progress
import argparse    # Command-line interface argument parsing
import signal      # POSIX and Windows process signal handlers (SIGINT, SIGTERM)
from typing import Dict, Any, Optional  # Type annotations for code clarity

# ------------------------------------------------------------------------------
# PySpark Environment Configuration
# ------------------------------------------------------------------------------
# In distributed environments and Windows local installations, worker processes
# need to run the exact same Python binary as the driver to avoid serialization errors.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# PySpark SQL imports: core engine, DataFrames, and functional expressions
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,           # References a column by string name
    from_json,     # Decodes raw JSON text strings into structured Spark rows
    to_timestamp,  # Parses raw timestamp strings into Spark SQL TimestampType
    window,        # Generates tumbling or sliding time windows on event_time
    count,         # Aggregation: counts non-null rows in each window
    avg,           # Aggregation: computes arithmetic average
    min as spark_min,  # Aggregation: finds the minimum value in each window
    max as spark_max,  # Aggregation: finds the maximum value in each window
    round as spark_round,  # Rounds numeric values to specified decimal precision
    when,          # Conditional SQL CASE/WHEN branching
    lit,           # Injects literal constant values into columns
)

# Relative or package import for schemas and inference engine:
# Depending on how the script is invoked (from root or from within services/streaming),
# we attempt local relative import first, falling back to absolute package import.
try:
    from schemas import (
        TRAFFIC_SCHEMA,
        AQI_SCHEMA,
        WEATHER_SCHEMA,
        GRID_ZONES_SCHEMA,
        TIMESTAMP_FORMAT,
        DEFAULT_TIMEZONE,
    )
    from streaming_inference import (
        load_forecasting_model,
        make_traffic_batch_processor,
        prepare_traffic_features,
        predict_traffic_speed,
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
    from services.streaming.streaming_inference import (
        load_forecasting_model,
        make_traffic_batch_processor,
        prepare_traffic_features,
        predict_traffic_speed,
    )

# Configure structured logging format
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
    # If no custom Kafka connector maven coordinate was provided, default to Spark 3.5.1 connector
    if kafka_package is None:
        kafka_package = os.environ.get(
            "SPARK_KAFKA_PACKAGE",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1",
        )

    # Initialize the SparkSession builder with UTC timezone and optimized shuffle partition count
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", DEFAULT_TIMEZONE)
        .config("spark.sql.shuffle.partitions", "4")  # Optimize shuffle partitions for local dev/testing
        .config("spark.jars.packages", kafka_package)  # Automatically download and include Kafka connector jar
    )

    # Configure master URL if explicitly provided or found in environment
    if master:
        builder = builder.master(master)
    elif "SPARK_MASTER_URL" in os.environ:
        builder = builder.master(os.environ["SPARK_MASTER_URL"])

    # Create or retrieve existing SparkSession
    spark = builder.getOrCreate()
    # Suppress verbose internal Spark info messages to keep logs actionable
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Initialized SparkSession: version={spark.version}, appName={app_name}")
    return spark


def load_grid_zones(spark: SparkSession, zones_path: str) -> DataFrame:
    """
    Loads and validates the static grid zone metadata table.
    Enforces uniqueness on zone_id to ensure relational joins do not multiply observations.
    """
    # Verify the CSV file exists on disk
    if not os.path.exists(zones_path):
        raise FileNotFoundError(f"Static grid zones file not found: {zones_path}")

    # Read the CSV file enforcing the strict grid zones schema
    zones_df = (
        spark.read.option("header", "true")
        .schema(GRID_ZONES_SCHEMA)
        .csv(zones_path)
    )

    # Verify primary key uniqueness to protect against fan-out join multiplication bugs
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
    
    Parameters:
      spark: Active SparkSession
      bootstrap_servers: Host and port of the Kafka cluster (e.g. 'localhost:9092')
      topic: Name of the Kafka topic to subscribe to
      starting_offsets: Offset strategy ('latest' for real-time, 'earliest' for historical backfill)
    """
    logger.info(f"Connecting to Kafka stream: topic='{topic}', bootstrap='{bootstrap_servers}', offsets='{starting_offsets}'")
    # Spark Structured Streaming Kafka source connector
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")  # Prevent crash if topic partitions are rebalanced or trimmed
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
    # Kafka messages have binary key, value, and timestamp; we cast to string and parse JSON
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

    # Filter out corrupted payloads and enforce strict physical velocity boundaries
    valid_stream = parsed.filter(
        col("event_time").isNotNull()
        & col("zone_id").isNotNull()
        & col("speed_kmph").isNotNull()
        & (col("speed_kmph") > 0.0)      # Eliminate 0 or negative velocities (blackout / sensor error)
        & (col("speed_kmph") <= 250.0)  # Eliminate impossible urban velocities > 250 km/h
    )
    return valid_stream


def parse_and_validate_aqi(raw_stream: DataFrame) -> DataFrame:
    """
    Parses JSON payload for air quality stream, enforces physical bounds,
    and converts timestamps to event_time.
    Physical rule: 0.0 <= pm25 <= 1000.0 (EPA bounds).
    """
    # Parse Kafka message value into typed AQI columns
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

    # Validate against EPA environmental sensor bounds for PM2.5
    valid_stream = parsed.filter(
        col("event_time").isNotNull()
        & col("zone_id").isNotNull()
        & col("pm25").isNotNull()
        & (col("pm25") >= 0.0)      # Particulate matter concentration cannot be negative
        & (col("pm25") <= 1000.0)   # Values above 1000 ug/m3 represent sensor malfunction
    )
    return valid_stream


def parse_and_validate_weather(raw_stream: DataFrame) -> DataFrame:
    """
    Parses JSON payload for weather stream, enforces terrestrial bounds,
    and converts timestamps to event_time.
    Physical rule: -50.0 <= temperature_c <= 60.0 and 0.0 <= humidity_pct <= 100.0.
    """
    # Parse Kafka weather JSON payload
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

    # Validate against terrestrial climatic temperature and relative humidity bounds
    valid_stream = parsed.filter(
        col("event_time").isNotNull()
        & col("zone_id").isNotNull()
        & col("temperature_c").isNotNull()
        & (col("temperature_c") >= -50.0)  # Filter temperatures colder than -50 C
        & (col("temperature_c") <= 60.0)   # Filter temperatures hotter than +60 C
        & col("humidity_pct").isNotNull()
        & (col("humidity_pct") >= 0.0)     # Humidity percentage lower bound
        & (col("humidity_pct") <= 100.0)   # Humidity percentage upper bound
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
    # Apply watermark threshold on event_time to limit state buffer size
    watermarked = traffic_stream.withWatermark("event_time", watermark_delay)

    # Group by tumbling window and zone identifier
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

    # Relational join with static zone metadata table (lat, lon, zone_name)
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
    # Apply 2-minute watermark on air quality readings
    watermarked = aqi_stream.withWatermark("event_time", watermark_delay)

    # Group by 5-minute tumbling window and zone
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

    # Enrich with static geographic details
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
    # Apply watermark to manage state store eviction for weather readings
    watermarked = weather_stream.withWatermark("event_time", watermark_delay)

    # Aggregate meteorological metrics over 5-minute intervals
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

    # Enrich with static zone metadata
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
    # Ensure physical destination directories exist on disk
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)

    logger.info(
        f"Starting query '{query_name}': output='{output_path}', checkpoint='{checkpoint_path}', "
        f"trigger={'availableNow' if available_now else trigger_interval}"
    )

    # Configure the streaming writer
    writer = (
        df.writeStream.queryName(query_name)
        .format("parquet")
        .outputMode("append")                            # Append finalized window records only
        .option("path", output_path)                     # Parquet storage root
        .option("checkpointLocation", checkpoint_path)   # Independent checkpoint directory for WAL
    )

    # Start the streaming query based on trigger mode
    if available_now:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime=trigger_interval).start()

    return query


def start_traffic_inference_query(
    df: DataFrame,
    model: Any,
    output_path: str,
    checkpoint_path: str,
    query_name: str = "TrafficInferenceQuery",
    trigger_interval: str = "10 seconds",
    available_now: bool = False,
):
    """
    Starts a Structured Streaming query that performs live MLlib model inference on each
    finalized micro-batch using foreachBatch and writes predictions to Parquet.

    Architectural Rationale:
    ------------------------
    1. Why foreachBatch instead of direct .transform() on the streaming DataFrame?
       In Apache Spark, MLlib's .transform() method requires a static DataFrame and cannot be
       applied directly to an unbounded streaming DataFrame because MLlib algorithms are
       not implemented as streaming relational expressions. Structured Streaming's
       `foreachBatch` sink delivers each finalized micro-batch as an ordinary static DataFrame,
       enabling native MLlib VectorAssembler and model scoring (.transform()) without schema errors.

    2. Why load the model ONCE at job startup outside foreachBatch?
       The model is loaded once on the driver node before starting the streaming query.
       If the model were loaded inside the per-batch callback function, Spark would re-read
       the model metadata and weight matrices from disk on every single micro-batch trigger,
       causing severe disk I/O bottlenecks and high processing latency.

    3. Checkpointing & At-Least-Once Delivery Guarantees:
       Structured Streaming records source Kafka offsets and micro-batch commit states in the
       specified checkpoint directory. If a worker fails or the driver process is terminated,
       restarting the query with the identical checkpointLocation ensures that processing
       resumes exactly from the last committed offset, avoiding unhandled data loss.
    """
    # Ensure destination output and checkpoint directories exist
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)

    logger.info(
        f"Starting inference query '{query_name}': output='{output_path}', "
        f"checkpoint='{checkpoint_path}', trigger={'availableNow' if available_now else trigger_interval}"
    )

    # Build the closure function that accepts (batch_df, batch_id)
    batch_processor = make_traffic_batch_processor(model, output_path)

    # Configure streaming writer to route micro-batches to the batch processor callback
    writer = (
        df.writeStream.queryName(query_name)
        .foreachBatch(batch_processor)
        .option("checkpointLocation", checkpoint_path)
    )

    # Start the streaming execution
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
    model_path: Optional[str] = None,
    enable_inference: bool = True,
):
    """
    Main orchestration function: initializes Spark, reads 3 Kafka topics, applies
    validation, windowing, watermarking, zone joining, loads the MLlib model once,
    and starts 3 streaming queries (traffic with live inference, aqi, and weather).
    """
    # Step 1: Initialize SparkSession with Kafka packages and configurations
    spark = build_spark_session(master=spark_master, kafka_package=kafka_package)
    # Step 2: Load static grid zones CSV for spatial enrichment
    grid_zones = load_grid_zones(spark, zones_path)

    # Step 3: Load MLlib forecasting model ONCE at job startup (outside micro-batch loop)
    forecasting_model = None
    if enable_inference:
        if not model_path:
            raise ValueError(
                "Streaming inference is enabled (enable_inference=True), but 'model_path' is None or empty. "
                "Specify a valid model directory or pass enable_inference=False to run feature aggregation only."
            )
        if not os.path.exists(model_path):
            raise FileNotFoundError(
                f"Streaming inference is enabled, but the required model directory was not found: '{model_path}'. "
                f"Ensure Day 5 model training has been completed, or pass --disable-inference (enable_inference=False) "
                f"to run feature aggregation without live inference."
            )
        try:
            forecasting_model = load_forecasting_model(model_path)
            logger.info(
                f"Successfully loaded primary MLlib forecasting model from '{model_path}' "
                f"for live streaming inference."
            )
        except Exception as e:
            logger.error(
                f"Fatal startup error: Failed to load required forecasting model from '{model_path}': {e}"
            )
            raise RuntimeError(
                f"Fatal startup error: Failed to load required forecasting model from '{model_path}': {e}"
            ) from e
    else:
        logger.info(
            "Live MLlib streaming inference is explicitly disabled (enable_inference=False). "
            "Running standard feature aggregation only."
        )

    # Step 4: Build Traffic Pipeline (read -> parse -> window -> enrich)
    raw_traffic = read_kafka_stream(spark, bootstrap_servers, traffic_topic, starting_offsets)
    valid_traffic = parse_and_validate_traffic(raw_traffic)
    traffic_features = aggregate_traffic(valid_traffic, grid_zones, window_duration, watermark_delay)

    # Step 5: Build AQI Pipeline
    raw_aqi = read_kafka_stream(spark, bootstrap_servers, aqi_topic, starting_offsets)
    valid_aqi = parse_and_validate_aqi(raw_aqi)
    aqi_features = aggregate_aqi(valid_aqi, grid_zones, window_duration, watermark_delay)

    # Step 6: Build Weather Pipeline
    raw_weather = read_kafka_stream(spark, bootstrap_servers, weather_topic, starting_offsets)
    valid_weather = parse_and_validate_weather(raw_weather)
    weather_features = aggregate_weather(valid_weather, grid_zones, window_duration, watermark_delay)

    # Step 7: Launch 3 Independent Streaming Queries with Dedicated Checkpoint Directories
    if forecasting_model is not None:
        traffic_query = start_traffic_inference_query(
            traffic_features,
            model=forecasting_model,
            output_path=os.path.join(output_root, "traffic"),
            checkpoint_path=os.path.join(checkpoint_root, "traffic"),
            query_name="TrafficInferenceQuery",
            trigger_interval=trigger_interval,
            available_now=available_now,
        )
    else:
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

    # Step 8: Register Graceful Shutdown Handler for OS Signals
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

    # Bind interrupt (Ctrl+C) and termination signals to the graceful shutdown handler
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info("All 3 Urbanix streaming queries successfully started!")

    # If requested, block main thread until streaming queries terminate
    if await_termination:
        logger.info("Awaiting termination of streaming queries...")
        for q in active_queries:
            q.awaitTermination()

    return active_queries, spark


def main():
    """
    Command-line interface entry point for launching the Urbanix Streaming Job.
    Parses CLI flags for Kafka brokers, topics, time windows, and model configurations.
    """
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
    parser.add_argument(
        "--model-path",
        default=os.path.join(repo_root, "models", "linear_regression"),
        help="Path to trained MLlib LinearRegression model for live inference (default: models/linear_regression)",
    )
    parser.add_argument(
        "--disable-inference",
        action="store_true",
        help="Disable live model scoring and run raw feature aggregation only",
    )
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

    # Invoke pipeline orchestration with parsed arguments
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
        model_path=args.model_path,
        enable_inference=not args.disable_inference,
    )


# Standard Python module guard
if __name__ == "__main__":
    main()
