# 📡 Sensor Simulation Layer (Producers)
![Status](https://img.shields.io/badge/Status-Planned%20(Day%202)-orange)

## Purpose
This module simulates and replays telemetry data, acting as the primary ingestion source for the Urbanix platform. It generates synthetic but realistic data mimicking a city grid's sensors.

## Sensor Types & Kafka Topics
- **Traffic Sensors** -> `traffic-topic`
- **Air Quality (AQI)** -> `aqi-topic`
- **Weather Conditions** -> `weather-topic`

## Expected Event Concept
Data will be serialized as JSON events before being published to Kafka.
```json
{
  "sensor_id": "T-104",
  "timestamp": "2026-09-19T10:00:00Z",
  "location": {"lat": 34.05, "lon": -118.24},
  "speed_kmh": 42.5
}
```

## Future Folder Structure
Will contain isolated Python producer scripts, schemas, and a `requirements.txt`.

**Current Status:** Planned for Day 2. No code implemented yet.
