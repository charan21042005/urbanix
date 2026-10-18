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

from zone_mapping import get_weather_zones

DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = "weather-topic"
DEFAULT_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "weather_sample.csv")
)
DEFAULT_INTERVAL_SEC = 0.1

KELVIN_OFFSET = 273.15


def parse_weather_rows(filepath: str) -> Generator[Dict[str, Any], None, None]:
    """
    Parses historical hourly weather CSV and yields verified observation events.
    Applies confirmed unit conversion from Kelvin to Celsius and broadcasts identical
    ambient observations across Urbanix zones without synthetic distortion.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Weather dataset not found: {filepath}")

    zones = get_weather_zones()

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_temp = row.get("temperature_kelvin", "").strip()
            raw_hum = row.get("humidity_percent", "").strip()

            if not raw_temp or not raw_hum:
                continue

            try:
                temp_k = float(raw_temp)
                hum_pct = float(raw_hum)
            except ValueError:
                continue

            # Physical boundary validation
            if temp_k < 150.0 or temp_k > 350.0:  # Outside terrestrial bounds
                continue
            if hum_pct < 0.0 or hum_pct > 100.0:
                continue

            temp_c = round(temp_k - KELVIN_OFFSET, 2)
            city_name = row.get("city", "Los Angeles").strip()
            raw_dt = row.get("datetime", "").strip()
            iso_timestamp = raw_dt.replace(" ", "T")

            # Broadcast genuine regional observation across zones without modifying values
            for zone_id in zones:
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
                yield event


def main():
    parser = argparse.ArgumentParser(description="Urbanix Weather Sensor Replay Producer")
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Target Kafka topic")
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH, help="Path to Weather CSV")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SEC, help="Delay between timestamp batches in seconds")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to publish")
    parser.add_argument("--dry-run", action="store_true", help="Print sample events without sending to Kafka")
    args = parser.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("=" * 60)
    print("[WEATHER] Urbanix Weather Producer -- OpenWeather Historical Simulation")
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
        print("\n[!] Graceful shutdown signal received. Stopping weather producer...")
        running = False

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    published_count = 0
    current_timestamp = None

    try:
        for event in parse_weather_rows(args.data_path):
            if not running:
                break

            if args.dry_run:
                print(f"[DRY-RUN] {json.dumps(event)}")
            else:
                producer.send(args.topic, value=event)

            published_count += 1

            # Throttle between timestamp slices
            if current_timestamp != event["timestamp"]:
                current_timestamp = event["timestamp"]
                if args.interval > 0:
                    time.sleep(args.interval)

            if published_count % 50 == 0:
                print(f"[+] Replayed {published_count} weather events (Current: {event['timestamp']}, Temp: {event['temperature_c']} deg C)...")

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
        print(f"[OK] Weather producer finished. Total published: {published_count} events.")


if __name__ == "__main__":
    main()
