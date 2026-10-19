"""
Urbanix — Streaming Schemas Module (Day 3)
Defines explicit PySpark StructType schemas for streaming Kafka payloads:
- traffic-topic
- aqi-topic
- weather-topic
and static zone metadata (grid_zones.csv).

All schema definitions reflect the verified Day 2 producer JSON contract.
No fabricated fields (such as synthetic vehicle_count or co2) are included.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# 1. Traffic Telemetry Schema (Topic: traffic-topic)
# Source: METR-LA loop detectors (Caltrans PeMS)
TRAFFIC_SCHEMA = StructType([
    StructField("zone_id", StringType(), nullable=False),
    StructField("sensor_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("speed_kmph", DoubleType(), nullable=True),
    StructField("speed_mph", DoubleType(), nullable=True),
    StructField("unit", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
])

# 2. Air Quality Telemetry Schema (Topic: aqi-topic)
# Source: US EPA AirData (AQS)
AQI_SCHEMA = StructType([
    StructField("zone_id", StringType(), nullable=False),
    StructField("station_id", StringType(), nullable=False),
    StructField("station_name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),
    StructField("pm25", DoubleType(), nullable=True),
    StructField("aqi", IntegerType(), nullable=True),
    StructField("unit", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
])

# 3. Weather Telemetry Schema (Topic: weather-topic)
# Source: Historical Hourly Weather (OpenWeatherMap)
WEATHER_SCHEMA = StructType([
    StructField("zone_id", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),
    StructField("temperature_c", DoubleType(), nullable=True),
    StructField("temperature_k", DoubleType(), nullable=True),
    StructField("humidity_pct", DoubleType(), nullable=True),
    StructField("temperature_unit", StringType(), nullable=True),
    StructField("humidity_unit", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
])

# 4. Static Grid Zones Metadata Schema (data/grid_zones.csv)
# Lookup table for relational zone enrichment
GRID_ZONES_SCHEMA = StructType([
    StructField("zone_id", StringType(), nullable=False),
    StructField("zone_name", StringType(), nullable=False),
    StructField("mapping_type", StringType(), nullable=True),
    StructField("mapping_description", StringType(), nullable=True),
    StructField("min_lat", DoubleType(), nullable=True),
    StructField("max_lat", DoubleType(), nullable=True),
    StructField("min_lon", DoubleType(), nullable=True),
    StructField("max_lon", DoubleType(), nullable=True),
])

# Standard ISO-8601 timestamp format produced by Day 2 producers
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

# Documented baseline timezone for Urbanix processing
# Day 2 datasets originate from Los Angeles County sensors without explicit UTC offsets.
# Spark session is configured to UTC to prevent unexpected daylight savings shifts,
# while documenting that input timestamps represent local Pacific Standard/Daylight event time.
DEFAULT_TIMEZONE = "UTC"
