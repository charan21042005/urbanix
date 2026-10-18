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

from zone_mapping import map_aqi_station_to_zone

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = "aqi-topic"
DEFAULT_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "aqi_sample.csv")
)
DEFAULT_INTERVAL_SEC = 0.1


def parse_aqi_rows(filepath: str) -> Generator[Dict[str, Any], None, None]:
    """
    Parses EPA AirData CSV file and yields verified PM2.5 observation dictionaries.
    Preserves original timestamps and physical station identifiers.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"AQI dataset not found: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_pm25 = row.get("pm25_mean", "").strip()
            if not raw_pm25:
                continue

            try:
                pm25_val = float(raw_pm25)
            except ValueError:
                continue

            # Skip unphysical / error sensor spikes if negative
            if pm25_val < 0.0:
                continue

            site_num = row.get("site_num", "").strip()
            site_name = row.get("site_name", "Unknown Station").strip()
            city_name = row.get("city_name", "Los Angeles").strip()
            date_local = row.get("date_local", "").strip()

            if not date_local:
                continue

            iso_timestamp = f"{date_local}T00:00:00" if "T" not in date_local else date_local

            raw_aqi = row.get("aqi", "").strip()
            aqi_val = None
            if raw_aqi:
                try:
                    aqi_val = int(float(raw_aqi))
                except ValueError:
                    aqi_val = None

            zone_id = map_aqi_station_to_zone(site_num)

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
            yield event


def main():
    parser = argparse.ArgumentParser(description="Urbanix Air Quality Sensor Replay Producer")
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Target Kafka topic")
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH, help="Path to AQI CSV")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SEC, help="Delay between events in seconds")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to publish")
    parser.add_argument("--dry-run", action="store_true", help="Print sample events without sending to Kafka")
    args = parser.parse_args()

    # Input validation
    if args.interval < 0:
        parser.error("--interval cannot be negative")
    if args.max_events is not None and args.max_events <= 0:
        parser.error("--max-events must be a positive integer")

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("=" * 60)
    print("[AQI] Urbanix AQI Producer -- U.S. EPA AirData Simulation")
    print(f" - Target Topic:    {args.topic}")
    print(f" - Bootstrap:       {args.bootstrap_server}")
    print(f" - Data Source:     {args.data_path}")
    print(f" - Replay Interval: {args.interval}s")
    if args.max_events:
        print(f" - Max Events:      {args.max_events}")
    print("=" * 60)

    confirmed_count = 0
    failed_count = 0

    def on_send_success(record_metadata):
        nonlocal confirmed_count
        confirmed_count += 1

    def on_send_error(exception):
        nonlocal failed_count
        failed_count += 1
        print(f"[-] Kafka delivery error on {args.topic}: {exception}", file=sys.stderr)

    producer = None
    if not args.dry_run:
        if KafkaProducer is None:
            sys.exit("Error: kafka-python-ng is not installed. Install requirements first.")
        try:
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

    running = True

    def sig_handler(sig, frame):
        nonlocal running
        print("\n[!] Graceful shutdown signal received. Stopping AQI producer...")
        running = False

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    emitted_count = 0

    try:
        for event in parse_aqi_rows(args.data_path):
            if not running:
                break

            if args.dry_run:
                print(f"[DRY-RUN] {json.dumps(event)}")
                confirmed_count += 1
            else:
                future = producer.send(args.topic, value=event)
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)

            emitted_count += 1

            if args.interval > 0:
                time.sleep(args.interval)

            if emitted_count % 50 == 0:
                print(f"[+] Replayed {emitted_count} AQI telemetry events (Current: {event['timestamp']}, Station: {event['station_name']})...")

            if args.max_events and emitted_count >= args.max_events:
                print(f"[OK] Reached max events limit ({args.max_events}). Stopping.")
                break

    except Exception as e:
        print(f"[-] Producer runtime error: {e}", file=sys.stderr)
        failed_count += 1
    finally:
        if producer:
            producer.flush()
            producer.close()
            print("[+] Kafka producer flushed and closed cleanly.")
        print(f"[OK] AQI producer finished. Confirmed published: {confirmed_count} events (Failed: {failed_count}).")
        if failed_count > 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
