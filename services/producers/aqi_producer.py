#!/usr/bin/env python3
"""
Urbanix — Air Quality Telemetry Simulation Producer (Day 2)
Replays verified historical air quality monitoring observations from the
U.S. EPA AirData system for Los Angeles County into the Kafka topic 'aqi-topic'.

Contract:
- Topic: aqi-topic
- Metric: PM2.5 (Particulate Matter < 2.5 µm in µg/m³)
- Observations: Genuine historical EPA monitoring station observations (no synthetic CO2)
- Delivery: Producer confirms delivery via Kafka callbacks and flushes before reporting success
"""

# --- Standard Library Imports ---
import os        # File system paths and environment variable inspection
import sys       # System parameters, exit codes, and stdout/stderr handling
import csv       # CSV parsing utilities (DictReader for header-based row lookups)
import json      # JSON serialization for message payload encoding
import time      # Time delays to throttle event replay speed (time.sleep)
import argparse  # Command-line option parsing for configurable execution
import signal    # Intercepting termination signals (SIGINT, SIGTERM) for graceful cleanup
from typing import Generator, Dict, Any, Optional  # Type hinting for static code analysis

# Try importing KafkaProducer from kafka-python-ng; allow None for dry-run testing
try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

# Import our deterministic spatial mapper for EPA air quality monitoring sites
from zone_mapping import map_aqi_station_to_zone

# --- Default Application Constants ---
# Default Kafka broker endpoint (defaults to localhost:9092)
DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Default Kafka topic designated for ambient air quality telemetry
DEFAULT_TOPIC = "aqi-topic"
# Default path to the EPA air quality sample CSV dataset
DEFAULT_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "aqi_sample.csv")
)
# Default replay pause between consecutive air quality observations in seconds
DEFAULT_INTERVAL_SEC = 0.1


def parse_aqi_rows(filepath: str) -> Generator[Dict[str, Any], None, None]:
    """
    Parses EPA AirData CSV file and yields verified PM2.5 observation dictionaries.
    Preserves original timestamps and physical station identifiers.

    Parameters:
        filepath (str): Physical path on disk to the AQI CSV dataset.

    Yields:
        Dict[str, Any]: Standardized air quality telemetry event dictionaries.
    """
    # Guard clause: ensure the dataset file exists before attempting to open it
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"AQI dataset not found: {filepath}")

    # Open CSV file with UTF-8 encoding
    with open(filepath, "r", encoding="utf-8") as f:
        # csv.DictReader maps column headers to dictionary keys for each row
        reader = csv.DictReader(f)
        for row in reader:
            # Extract raw PM2.5 mean concentration string
            raw_pm25 = row.get("pm25_mean", "").strip()
            # Skip rows where PM2.5 reading is missing
            if not raw_pm25:
                continue

            try:
                # Convert reading to floating point number
                pm25_val = float(raw_pm25)
            except ValueError:
                # Skip corrupt non-numeric values
                continue

            # Skip unphysical / sensor electrical error spikes if negative
            if pm25_val < 0.0:
                continue

            # Extract station and geographic metadata from the CSV columns
            site_num = row.get("site_num", "").strip()
            site_name = row.get("site_name", "Unknown Station").strip()
            city_name = row.get("city_name", "Los Angeles").strip()
            date_local = row.get("date_local", "").strip()

            # Date must be present to establish a valid observation timestamp
            if not date_local:
                continue

            # Standardize date string to ISO-8601 format ('YYYY-MM-DDT00:00:00')
            iso_timestamp = f"{date_local}T00:00:00" if "T" not in date_local else date_local

            # Extract raw calculated Air Quality Index (AQI) value if present
            raw_aqi = row.get("aqi", "").strip()
            aqi_val = None
            if raw_aqi:
                try:
                    aqi_val = int(float(raw_aqi))
                except ValueError:
                    aqi_val = None

            # Map the EPA station site number to its corresponding Urbanix operational zone
            zone_id = map_aqi_station_to_zone(site_num)

            # Construct standardized JSON-serializable event payload
            event = {
                "zone_id": zone_id,
                "station_id": site_num,
                "station_name": site_name,
                "city": city_name,
                "timestamp": iso_timestamp,
                "pm25": round(pm25_val, 2),
                "aqi": aqi_val,
                "unit": "ug/m3",
                "source": "US EPA AirData (AQS)",
            }
            # Yield observation to the caller
            yield event


def main():
    """
    Main application entry point.
    Parses CLI flags, establishes Kafka connection, reads EPA CSV telemetry,
    and publishes structured events with delivery confirmation callbacks.
    """
    # Configure command-line argument parser
    parser = argparse.ArgumentParser(description="Urbanix Air Quality Sensor Replay Producer")
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Target Kafka topic")
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH, help="Path to AQI CSV")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SEC, help="Delay between events in seconds")
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
    print("[AQI] Urbanix AQI Producer -- U.S. EPA AirData Simulation")
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
        print("\n[!] Graceful shutdown signal received. Stopping AQI producer...")
        running = False

    # Register OS signal hooks
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    emitted_count = 0

    try:
        # Replay air quality observations row-by-row
        for event in parse_aqi_rows(args.data_path):
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

            # Throttle between events according to user-configured interval
            if args.interval > 0:
                time.sleep(args.interval)

            # Periodic status logging every 50 events
            if emitted_count % 50 == 0:
                print(f"[+] Replayed {emitted_count} AQI telemetry events (Current: {event['timestamp']}, Station: {event['station_name']})...")

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
        print(f"[OK] AQI producer finished. Confirmed published: {confirmed_count} events (Failed: {failed_count}).")
        # Exit with non-zero error code if any delivery failures occurred
        if failed_count > 0:
            sys.exit(1)


# Standard Python boilerplate to execute main() when run directly from command line
if __name__ == "__main__":
    main()
