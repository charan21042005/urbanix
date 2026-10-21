# Urbanix — Day 2: Sensor Simulation Layer & Data Provenance

Welcome to the **Day 2 Module Documentation** for Project Urbanix.

## Milestone Summary
- **Module:** Weekend 1 — Core Foundation & Ingestion (Day 2 of 12)
- **Primary Language:** Python 3.10+
- **Core Technologies:** Apache Kafka Producer API (`confluent-kafka` / `kafka-python`), Historical Benchmark Replay
- **Data Sources:** Caltrans PeMS METR-LA (Traffic), U.S. EPA AirData (AQI), OpenWeatherMap Archives (Weather)
- **Status:** **VERIFIED & FORMALLY CLOSED** (All Acceptance Gates Passed)
- **Total Tests:** 19 Passing Automated Unit & Integration Tests in `services/producers/tests/`

## Directory Contents
- [DAY_02_SENSOR_SIMULATION_REPORT.md](DAY_02_SENSOR_SIMULATION_REPORT.md) — Comprehensive technical report detailing data provenance, licensing, wide-to-stream matrix transformations, delivery callback mechanics, process supervision, and test execution logs.
- [../../services/producers/](../../services/producers/) — Python producer scripts (`traffic_producer.py`, `aqi_producer.py`, `weather_producer.py`, `run_all.py`).
- [../../services/producers/tests/](../../services/producers/tests/) — 19-test automated verification suite for sensor simulation.
- [../../data/](../../data/) — Sample benchmark datasets (`traffic_sample.csv`, `aqi_sample.csv`, `weather_sample.csv`, `grid_zones.csv`).

## Quick Execution Commands
```powershell
# Navigate to producers directory
cd services/producers

# 1. Run automated producer test suite (19 tests)
python -m pytest tests/ -v

# 2. Run sensor simulation replay supervisor
python run_all.py
```
