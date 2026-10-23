#!/usr/bin/env python3
"""
Urbanix — Traffic Telemetry Simulation Producer (Day 2)
Replays verified historical traffic loop-detector telemetry from METR-LA (Caltrans PeMS)
into the Kafka topic 'traffic-topic' at a configurable accelerated replay rate.

Contract:
- Topic: traffic-topic
- Unit: km/h (explicitly converted from raw source mph via speed_mph * 1.60934)
- Observations: Genuine historical sensor velocity measurements (no invented vehicle count)
- Delivery: Producer confirms delivery via Kafka callbacks and flushes before reporting success
"""

# --- Standard Library Imports ---
import os        # Provides operating system path and environment variable utilities
import sys       # Access to system-specific parameters and standard I/O streams
import csv       # Built-in reader/writer for parsing Comma-Separated Values files
import json      # Encoder and decoder for serializing dictionaries into JSON strings
import time      # Time utilities for introducing replay intervals and delays (time.sleep)
import argparse  # Command-line argument parser for flexible runtime flags
import signal    # Allows intercepting OS termination signals (SIGINT, SIGTERM) for graceful shutdowns
from typing import Generator, Dict, Any, Optional  # Type annotations for static verification

# Attempt to import KafkaProducer from kafka-python-ng; gracefully handle absence for dry-run testing
try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

# Import our deterministic spatial zone mapper for traffic loop detectors
from zone_mapping import map_traffic_sensor_to_zone

# --- Default Application Constants ---
# Default Kafka broker connection string (overridable via environment variable or CLI flag)
DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Default Kafka topic designated for traffic telemetry events
DEFAULT_TOPIC = "traffic-topic"
# Default path to the historical sample CSV dataset located in the project's data directory
DEFAULT_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "traffic_sample.csv")
)
# Default replay pause between distinct timestamp slices (in seconds)
DEFAULT_INTERVAL_SEC = 0.05

# Conversion factor: 1 mile per hour is exactly 1.60934 kilometers per hour
MPH_TO_KMPH = 1.60934


def parse_traffic_rows(filepath: str) -> Generator[Dict[str, Any], None, None]:
    """
    Parses wide-matrix METR-LA CSV file and yields discrete sensor observation dictionaries.
    Preserves original timestamps and sensor IDs with confirmed unit conversions.

    Parameters:
        filepath (str): Physical path on disk to the traffic CSV dataset.

    Yields:
        Dict[str, Any]: Standardized telemetry event dictionaries.
    """
    # Guard clause: ensure the dataset file exists before attempting to open it
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Traffic dataset not found: {filepath}")

    # Open file with standard UTF-8 encoding
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            # Read the header row: first column is timestamp, remaining columns are sensor IDs
            header = next(reader)
        except StopIteration:
            # File is completely empty; return immediately
            return

        # Extract sensor IDs from the header (ignoring the first 'timestamp' column)
        sensor_ids = [s.strip() for s in header[1:] if s.strip()]

        # Iterate over each row in the CSV file
        for row_idx, row in enumerate(reader):
            # Skip empty lines or malformed rows that lack data
            if not row or len(row) < 2:
                continue

            # First column is the observation timestamp string (e.g., '2012-03-01 00:00:00')
            raw_timestamp = row[0].strip()
            # Standardize timestamp to ISO-8601 format ('YYYY-MM-DDTHH:MM:SS')
            iso_timestamp = raw_timestamp.replace(" ", "T")

            # Iterate through each sensor column in this timestamp row
            for col_idx, sensor_id in enumerate(sensor_ids, start=1):
                # Guard against ragged rows where row length is shorter than header length
                if col_idx >= len(row):
                    break

                val_str = row[col_idx].strip()
                # Skip empty/missing reading cells
                if not val_str:
                    continue

                try:
                    # Parse velocity reading in miles per hour
                    speed_mph = float(val_str)
                except ValueError:
                    # Skip corrupt non-numeric cells
                    continue

                # Skip zero / sensor electrical blackout if unobserved (PeMS indicates missing as 0.0)
                if speed_mph <= 0.0:
                    continue

                # Convert to metric speed in km/h and round to 2 decimal places
                speed_kmph = round(speed_mph * MPH_TO_KMPH, 2)
                # Map physical sensor ID to its assigned Urbanix zone (zone_1 to zone_5)
                zone_id = map_traffic_sensor_to_zone(sensor_id)

                # Construct standardized JSON-serializable event payload
                event = {
                    "zone_id": zone_id,
                    "sensor_id": sensor_id,
                    "timestamp": iso_timestamp,
                    "speed_kmph": speed_kmph,
                    "speed_mph": round(speed_mph, 2),
                    "unit": "km/h",
                    "source": "METR-LA (Caltrans PeMS)",
                }
                # Yield observation to the caller (generator pipeline)
                yield event


def main():
    """
    Main application entry point.
    Parses CLI flags, establishes Kafka connection, reads CSV telemetry,
    and publishes structured events with delivery confirmation callbacks.
    """
    # Configure command-line argument parser
    parser = argparse.ArgumentParser(description="Urbanix Traffic Sensor Replay Producer")
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Target Kafka topic")
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH, help="Path to traffic CSV")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SEC, help="Delay between timestamp batches in seconds")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to publish (useful for test runs)")
    parser.add_argument("--dry-run", action="store_true", help="Print sample events without sending to Kafka")
    args = parser.parse_args()

    # Input validation checks
    if args.interval < 0:
        parser.error("--interval cannot be negative")
    if args.max_events is not None and args.max_events <= 0:
        parser.error("--max-events must be a positive integer")

    # Force standard output to UTF-8 on Windows environments to prevent character encoding errors
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    # Print startup banner and active configuration
    print("=" * 60)
    print("[TRAFFIC] Urbanix Traffic Producer -- METR-LA Sensor Simulation")
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
    # If not running in dry-run mode, initialize the real KafkaProducer
    if not args.dry_run:
        if KafkaProducer is None:
            sys.exit("Error: kafka-python-ng is not installed. Install requirements first.")
        try:
            # Instantiate KafkaProducer with robust delivery guarantees:
            # - value_serializer: automatically encodes Python dicts to UTF-8 JSON bytes
            # - acks=1: leader broker must write to local log before acknowledging
            # - retries=3: automatically retry transient connection glitches
            # - retry_backoff_ms=250: wait 250ms between retries
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
        print("\n[!] Graceful shutdown signal received. Stopping traffic producer...")
        running = False

    # Register OS signal hooks
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    emitted_count = 0
    current_timestamp = None

    try:
        # Replay telemetry stream row-by-row
        for event in parse_traffic_rows(args.data_path):
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

            # Periodic status logging every 500 events
            if emitted_count % 500 == 0:
                print(f"[+] Replayed {emitted_count} traffic telemetry events (Current: {event['timestamp']})...")

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
        print(f"[OK] Traffic producer finished. Confirmed published: {confirmed_count} events (Failed: {failed_count}).")
        # Exit with non-zero error code if any delivery failures occurred
        if failed_count > 0:
            sys.exit(1)


# Standard Python boilerplate to execute main() when run directly from command line
if __name__ == "__main__":
    main()
