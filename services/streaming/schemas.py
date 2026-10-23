"""
Urbanix — Streaming Schemas Module (Day 3)
Defines explicit PySpark StructType schemas for streaming Kafka payloads:
- traffic-topic
- aqi-topic
- weather-topic
and static zone metadata (grid_zones.csv).

All schema definitions reflect the verified Day 2 producer JSON contract.
No fabricated fields (such as synthetic vehicle_count or co2) are included.

BEGINNER'S GUIDE TO PYSPARK SCHEMAS:
In Apache Spark, a `StructType` represents the overall structure (schema) of a table or JSON object,
analogous to a `CREATE TABLE` definition in SQL.
Inside a `StructType`, a list of `StructField` definitions specifies:
1. Field name (string, e.g., "zone_id")
2. Data type (e.g., StringType(), DoubleType(), IntegerType())
3. Nullability (boolean: True if values can be None/null, False if required)
Defining explicit schemas prevents PySpark from performing expensive schema inference scans over
distributed streams, guaranteeing deterministic typing and fast deserialization.
"""

# Import fundamental PySpark SQL data type classes
from pyspark.sql.types import (
    StructType,     # Represents a complete schema containing multiple fields
    StructField,    # Represents a single column/field definition (name, type, nullable)
    StringType,     # Text string data type
    DoubleType,     # 64-bit floating-point numeric data type
    IntegerType,    # 32-bit signed integer data type
    TimestampType,  # SQL timestamp data type with date and time
)

# ==============================================================================
# 1. Traffic Telemetry Schema (Topic: traffic-topic)
# ==============================================================================
# Source: METR-LA highway loop detectors (Caltrans PeMS freeway sensors)
# Each record represents a single speed observation from a physical loop detector.
TRAFFIC_SCHEMA = StructType([
    # Identifier of the smart city operational zone (e.g., "zone_1" through "zone_5")
    StructField("zone_id", StringType(), nullable=False),
    # Unique physical sensor station ID from Caltrans PeMS (e.g., "773869")
    StructField("sensor_id", StringType(), nullable=False),
    # Observation time in ISO-8601 string format (e.g., "2012-03-01T00:00:00")
    StructField("timestamp", StringType(), nullable=False),
    # Vehicle speed converted to kilometers per hour (km/h)
    StructField("speed_kmph", DoubleType(), nullable=True),
    # Original vehicle speed in miles per hour (mph) from raw source
    StructField("speed_mph", DoubleType(), nullable=True),
    # Measurement unit designation string (always "km/h")
    StructField("unit", StringType(), nullable=True),
    # Data provenance label (always "METR-LA (Caltrans PeMS)")
    StructField("source", StringType(), nullable=True),
])

# ==============================================================================
# 2. Air Quality Telemetry Schema (Topic: aqi-topic)
# ==============================================================================
# Source: U.S. EPA AirData system (Air Quality System - AQS)
# Each record represents an ambient particulate matter measurement from a monitoring station.
AQI_SCHEMA = StructType([
    # Identifier of the smart city operational zone assigned to this monitoring station
    StructField("zone_id", StringType(), nullable=False),
    # EPA physical monitoring site number (e.g., "1103" for Downtown LA)
    StructField("station_id", StringType(), nullable=False),
    # Official name of the EPA monitoring facility (e.g., "N. Main St")
    StructField("station_name", StringType(), nullable=True),
    # Municipal city location (e.g., "Los Angeles", "Burbank", "Pasadena")
    StructField("city", StringType(), nullable=True),
    # Observation date/time in ISO-8601 string format
    StructField("timestamp", StringType(), nullable=False),
    # Fine particulate matter concentration in micrograms per cubic meter (µg/m³)
    StructField("pm25", DoubleType(), nullable=True),
    # EPA Air Quality Index calculation (integer value, e.g., 55 for moderate air)
    StructField("aqi", IntegerType(), nullable=True),
    # Measurement unit designation string (always "ug/m3")
    StructField("unit", StringType(), nullable=True),
    # Data provenance label (always "US EPA AirData (AQS)")
    StructField("source", StringType(), nullable=True),
])

# ==============================================================================
# 3. Weather Telemetry Schema (Topic: weather-topic)
# ==============================================================================
# Source: Historical Hourly Meteorological Data (OpenWeatherMap collection)
# Each record represents ambient atmospheric conditions broadcast across the metropolitan basin.
WEATHER_SCHEMA = StructType([
    # Identifier of the smart city operational zone receiving this regional weather reading
    StructField("zone_id", StringType(), nullable=False),
    # City name for regional context (e.g., "Los Angeles")
    StructField("city", StringType(), nullable=True),
    # Observation timestamp in ISO-8601 string format
    StructField("timestamp", StringType(), nullable=False),
    # Ambient temperature converted to degrees Celsius (°C)
    StructField("temperature_c", DoubleType(), nullable=True),
    # Original ambient temperature in thermodynamic Kelvin (K)
    StructField("temperature_k", DoubleType(), nullable=True),
    # Atmospheric relative humidity percentage (0.0% to 100.0%)
    StructField("humidity_pct", DoubleType(), nullable=True),
    # Temperature unit designation string (always "Celsius")
    StructField("temperature_unit", StringType(), nullable=True),
    # Humidity unit designation string (always "Percent")
    StructField("humidity_unit", StringType(), nullable=True),
    # Data provenance label (always "Historical Hourly Weather Data (OpenWeatherMap)")
    StructField("source", StringType(), nullable=True),
])

# ==============================================================================
# 4. Static Grid Zones Metadata Schema (data/grid_zones.csv)
# ==============================================================================
# Lookup table used for spatial relational enrichment during stream joins.
GRID_ZONES_SCHEMA = StructType([
    # Primary key identifier for the zone ("zone_1" through "zone_5")
    StructField("zone_id", StringType(), nullable=False),
    # Human-readable descriptive name (e.g., "Downtown Core / CBD")
    StructField("zone_name", StringType(), nullable=False),
    # Spatial partitioning methodology description (e.g., "Sub-regional corridor partition")
    StructField("mapping_type", StringType(), nullable=True),
    # Long descriptive notes regarding physical highway and neighborhood coverage
    StructField("mapping_description", StringType(), nullable=True),
    # Minimum latitude bounding coordinate (South boundary)
    StructField("min_lat", DoubleType(), nullable=True),
    # Maximum latitude bounding coordinate (North boundary)
    StructField("max_lat", DoubleType(), nullable=True),
    # Minimum longitude bounding coordinate (West boundary)
    StructField("min_lon", DoubleType(), nullable=True),
    # Maximum longitude bounding coordinate (East boundary)
    StructField("max_lon", DoubleType(), nullable=True),
])

# Standard ISO-8601 timestamp parsing format string recognized by PySpark SQL:
# yyyy = 4-digit year, MM = 2-digit month, dd = 2-digit day,
# 'T' = literal delimiter, HH = 24-hour hour, mm = minute, ss = second
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

# Documented baseline timezone for Urbanix distributed processing:
# All SparkSessions standardise internal timestamp calculations to UTC to eliminate daylight savings shifts.
DEFAULT_TIMEZONE = "UTC"
