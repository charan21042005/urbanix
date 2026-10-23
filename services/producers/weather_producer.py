#!/usr/bin/env python3
"""
Urbanix — Weather Telemetry Simulation Producer (Day 2)
Replays verified historical hourly meteorological observations from OpenWeatherMap
(Kaggle selfishgene collection) for Los Angeles into the Kafka topic 'weather-topic'.

Contract:
- Topic: weather-topic
- Metric: Temperature (Celsius, converted from Kelvin via T_K - 273.15) & Relative Humidity (%)
- Spatial Assumption: Regional atmospheric readings apply across all grid zones without
  manufacturing artificial per-zone variations.
- Delivery: Producer confirms delivery via Kafka callbacks and flushes before reporting success
"""

# --- Standard Library Imports ---
import os        # File system paths and environment variable retrieval
import sys       # System arguments, runtime parameters, and output stream control
import csv       # Reading and parsing Comma-Separated Values files
import json      # Encoding Python dictionary structures into JSON byte payloads
import time      # Time delays to throttle replay speed (time.sleep)
import argparse  # Command-line interface definition and flag parsing
import signal    # Intercepting operating system interrupt signals for graceful cleanup
from typing import Generator, Dict, Any, Optional  # Type annotations for static typing

# Try importing KafkaProducer from kafka-python-ng; allow None for dry-run testing
try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

# Import our helper returning the active zones governed by regional weather observations
from zone_mapping import get_weather_zones

# --- Default Application Constants ---
# Default Kafka broker connection string (defaults to localhost:9092)
DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Default Kafka topic designated for meteorological telemetry events
DEFAULT_TOPIC = "weather-topic"
# Default path to the historical OpenWeather sample CSV dataset
DEFAULT_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "weather_sample.csv")
)
# Default replay pause between consecutive timestamp batches in seconds
DEFAULT_INTERVAL_SEC = 0.1

# Thermodynamic constant: absolute zero in Celsius is -273.15 °C (0 Kelvin)
KELVIN_OFFSET = 273.15


def parse_weather_rows(filepath: str) -> Generator[Dict[str, Any], None, None]:
    """
    Parses historical hourly weather CSV and yields verified observation events.
    Applies confirmed unit conversion from Kelvin to Celsius and broadcasts identical
    ambient observations across Urbanix zones without synthetic distortion.

    Parameters:
        filepath (str): Physical path on disk to the weather CSV dataset.

    Yields:
        Dict[str, Any]: Standardized meteorological telemetry event dictionaries.
    """
    # Guard clause: ensure the dataset file exists before attempting to open it
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Weather dataset not found: {filepath}")

    # Retrieve the list of all Urbanix zones that share this regional weather reading
    zones = get_weather_zones()

    # Open CSV file with standard UTF-8 encoding
    with open(filepath, "r", encoding="utf-8") as f:
        # csv.DictReader maps column headers to dictionary keys for each row
        reader = csv.DictReader(f)
        for row in reader:
            # Extract raw temperature in Kelvin and relative humidity percentage strings
            raw_temp = row.get("temperature_kelvin", "").strip()
            raw_hum = row.get("humidity_percent", "").strip()

            # Skip rows where either critical meteorological measurement is absent
            if not raw_temp or not raw_hum:
                continue

            try:
                # Convert raw text strings into floating-point numbers
                temp_k = float(raw_temp)
                hum_pct = float(raw_hum)
            except ValueError:
                # Skip corrupt non-numeric data entries
                continue

            # Physical boundary validation:
            # Atmospheric surface temperatures on Earth fall strictly between 150 K (-123 °C) and 350 K (+77 °C)
            if temp_k < 150.0 or temp_k > 350.0:
                continue
            # Relative humidity is physically bounded between 0% and 100%
            if hum_pct < 0.0 or hum_pct > 100.0:
                continue

            # Convert thermodynamic Kelvin temperature into Celsius and round to 2 decimal places
            temp_c = round(temp_k - KELVIN_OFFSET, 2)
            city_name = row.get("city", "Los Angeles").strip()
            raw_dt = row.get("datetime", "").strip()
            # Standardize datetime string to ISO-8601 ('YYYY-MM-DDTHH:MM:SS')
            iso_timestamp = raw_dt.replace(" ", "T")

            # Broadcast genuine regional observation across all 5 zones without manufacturing artificial variations
            for zone_id in zones:
                # Construct standardized JSON-serializable event payload
                event = {
                    "zone_id": zone_id,
                    "city": city_name,
                    "timestamp": iso_timestamp,
                    "temperature_c": temp_c,
                    "temperature_k": round(temp_k, 2),
                    "humidity_pct": round(hum_pct, 1),
                    "temperature_unit": "Celsius",
                    "humidity_unit": "Percent",
                    "source": "Historical Hourly Weather Data (OpenWeatherMap)",
                }
                # Yield observation to the caller
                yield event


def main():
    """
    Main application entry point.
    Parses CLI flags, establishes Kafka connection, reads weather CSV telemetry,
    and publishes structured events with delivery confirmation callbacks.
    """
    # Configure command-line argument parser
    parser = argparse.ArgumentParser(description="Urbanix Weather Sensor Replay Producer")
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Target Kafka topic")
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH, help="Path to Weather CSV")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SEC, help="Delay between timestamp batches in seconds")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to publish")
    parser.add_argument("--dry-run", action="store_true", help="Print sample events without sending to Kafka")
    args = parser.parse_args()

    # Input validation checks
    if args.interval < 0:
        parser.error("--interval cannot be negative")
    if args.max_events is not None and args.max_events <= 0:
        parser.error("--max-events must be a positive integer")

    # Force standard output to UTF-8 on Windows environments
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    # Print startup banner and active configuration
    print("=" * 60)
    print("[WEATHER] Urbanix Weather Producer -- OpenWeather Historical Simulation")
    print(f" - Target Topic:    {args.topic}")
    print(f" - Bootstrap:       {args.bootstrap_server}")
    print(f" - Data Source:     {args.data_path}")
    print(f" - Replay Interval: {args.interval}s")
    if args.max_events:
        print(f" - Max Events:      {args.max_events}")
    print("=" * 60)

    # Counters to track asynchronous message delivery status
    confirmed_count = 0
    failed_count = 0

    # Kafka delivery success callback: called asynchronously by Kafka background thread
    def on_send_success(record_metadata):
        nonlocal confirmed_count
        confirmed_count += 1

    # Kafka delivery error callback: called asynchronously if broker fails to acknowledge
    def on_send_error(exception):
        nonlocal failed_count
        failed_count += 1
        print(f"[-] Kafka delivery error on {args.topic}: {exception}", file=sys.stderr)

    producer = None
    # Initialize KafkaProducer if not executing in dry-run mode
    if not args.dry_run:
        if KafkaProducer is None:
            sys.exit("Error: kafka-python-ng is not installed. Install requirements first.")
        try:
            # Instantiate KafkaProducer with robust delivery guarantees
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                retries=3,
                retry_backoff_ms=250,
                request_timeout_ms=10000,
            )
            print("[+] Successfully connected to Kafka broker.")
        except Exception as e:
            sys.exit(f"[-] Failed to connect to Kafka broker at {args.bootstrap_server}: {e}")

    # Control flag for clean loop termination on OS signals
    running = True

    # Signal handler for graceful termination (Ctrl+C or Docker stop)
    def sig_handler(sig, frame):
        nonlocal running
        print("\n[!] Graceful shutdown signal received. Stopping weather producer...")
        running = False

    # Register OS signal hooks
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    emitted_count = 0
    current_timestamp = None

    try:
        # Replay meteorological observations row-by-row
        for event in parse_weather_rows(args.data_path):
            if not running:
                break

            if args.dry_run:
                # Dry run mode: print JSON directly to console instead of Kafka
                print(f"[DRY-RUN] {json.dumps(event)}")
                confirmed_count += 1
            else:
                # Asynchronously send message to Kafka topic and attach tracking callbacks
                future = producer.send(args.topic, value=event)
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)

            emitted_count += 1

            # Throttle between timestamp slices to simulate temporal progression
            if current_timestamp != event["timestamp"]:
                current_timestamp = event["timestamp"]
                if args.interval > 0:
                    time.sleep(args.interval)

            # Periodic status logging every 50 events
            if emitted_count % 50 == 0:
                print(f"[+] Replayed {emitted_count} weather events (Current: {event['timestamp']}, Temp: {event['temperature_c']} deg C)...")

            # Check if user-configured event limit has been reached
            if args.max_events and emitted_count >= args.max_events:
                print(f"[OK] Reached max events limit ({args.max_events}). Stopping.")
                break

    except Exception as e:
        print(f"[-] Producer runtime error: {e}", file=sys.stderr)
        failed_count += 1
    finally:
        # Ensure producer flushes all in-flight buffers to broker before exiting
        if producer:
            producer.flush()
            producer.close()
            print("[+] Kafka producer flushed and closed cleanly.")
        print(f"[OK] Weather producer finished. Confirmed published: {confirmed_count} events (Failed: {failed_count}).")
        # Exit with non-zero error code if any delivery failures occurred
        if failed_count > 0:
            sys.exit(1)


# Standard Python boilerplate to execute main() when run directly from command line
if __name__ == "__main__":
    main()
