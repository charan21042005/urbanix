#!/usr/bin/env python3
"""
Urbanix — Traffic Telemetry Simulation Producer (Day 2)
Replays verified historical traffic loop-detector telemetry from METR-LA (Caltrans PeMS)
into the Kafka topic 'traffic-topic' at a configurable accelerated replay rate.

Contract:
- Topic: traffic-topic
- Unit: km/h (explicitly converted from raw source mph via speed_mph * 1.60934)
- Observations: Genuine historical sensor velocity measurements (no invented vehicle count)
"""

import os
import sys
import csv
import json
import time
import argparse
import signal
from typing import Generator, Dict, Any, Optional

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

from zone_mapping import map_traffic_sensor_to_zone

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = "traffic-topic"
DEFAULT_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "traffic_sample.csv")
)
DEFAULT_INTERVAL_SEC = 0.05  # Replay delay between timestamp slices

MPH_TO_KMPH = 1.60934


def parse_traffic_rows(filepath: str) -> Generator[Dict[str, Any], None, None]:
    """
    Parses wide-matrix METR-LA CSV file and yields discrete sensor observation dictionaries.
    Preserves original timestamps and sensor IDs with confirmed unit conversions.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Traffic dataset not found: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return

        sensor_ids = [s.strip() for s in header[1:] if s.strip()]

        for row_idx, row in enumerate(reader):
            if not row or len(row) < 2:
                continue

            raw_timestamp = row[0].strip()
            # Standardize timestamp to ISO-8601 without inventing timezones
            iso_timestamp = raw_timestamp.replace(" ", "T")

            for col_idx, sensor_id in enumerate(sensor_ids, start=1):
                if col_idx >= len(row):
                    break

                val_str = row[col_idx].strip()
                if not val_str:
                    continue

                try:
                    speed_mph = float(val_str)
                except ValueError:
                    continue

                # Skip zero / sensor electrical blackout if unobserved
                if speed_mph <= 0.0:
                    continue

                speed_kmph = round(speed_mph * MPH_TO_KMPH, 2)
                zone_id = map_traffic_sensor_to_zone(sensor_id)

                event = {
                    "zone_id": zone_id,
                    "sensor_id": sensor_id,
                    "timestamp": iso_timestamp,
                    "speed_kmph": speed_kmph,
                    "speed_mph": round(speed_mph, 2),
                    "unit": "km/h",
                    "source": "METR-LA (Caltrans PeMS)",
                }
                yield event


def main():
    parser = argparse.ArgumentParser(description="Urbanix Traffic Sensor Replay Producer")
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Target Kafka topic")
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH, help="Path to traffic CSV")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SEC, help="Delay between timestamp batches in seconds")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to publish (useful for test runs)")
    parser.add_argument("--dry-run", action="store_true", help="Print sample events without sending to Kafka")
    args = parser.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("=" * 60)
    print("[TRAFFIC] Urbanix Traffic Producer -- METR-LA Sensor Simulation")
    print(f" - Target Topic:    {args.topic}")
    print(f" - Bootstrap:       {args.bootstrap_server}")
    print(f" - Data Source:     {args.data_path}")
    print(f" - Replay Interval: {args.interval}s")
    if args.max_events:
        print(f" - Max Events:      {args.max_events}")
    print("=" * 60)

    producer = None
    if not args.dry_run:
        if KafkaProducer is None:
            sys.exit("Error: kafka-python-ng is not installed. Install requirements first.")
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                request_timeout_ms=10000,
            )
            print("[+] Successfully connected to Kafka broker.")
        except Exception as e:
            sys.exit(f"[-] Failed to connect to Kafka broker at {args.bootstrap_server}: {e}")

    running = True

    def sig_handler(sig, frame):
        nonlocal running
        print("\n[!] Graceful shutdown signal received. Stopping traffic producer...")
        running = False

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    published_count = 0
    current_timestamp = None

    try:
        for event in parse_traffic_rows(args.data_path):
            if not running:
                break

            if args.dry_run:
                print(f"[DRY-RUN] {json.dumps(event)}")
            else:
                producer.send(args.topic, value=event)

            published_count += 1

            # Throttle between timestamp slices to simulate temporal progression
            if current_timestamp != event["timestamp"]:
                current_timestamp = event["timestamp"]
                if args.interval > 0:
                    time.sleep(args.interval)

            if published_count % 500 == 0:
                print(f"[+] Replayed {published_count} traffic telemetry events (Current: {event['timestamp']})...")

            if args.max_events and published_count >= args.max_events:
                print(f"[OK] Reached max events limit ({args.max_events}). Stopping.")
                break

    except Exception as e:
        print(f"[-] Producer runtime error: {e}")
    finally:
        if producer:
            producer.flush()
            producer.close()
            print("[+] Kafka producer flushed and closed cleanly.")
        print(f"[OK] Traffic producer finished. Total published: {published_count} events.")


if __name__ == "__main__":
    main()
