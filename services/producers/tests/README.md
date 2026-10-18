# 🧪 Urbanix Producer Test Suite (`services/producers/tests/`)

![Tests](https://img.shields.io/badge/Unit_Tests-19%2F19_Passing-brightgreen)
![Framework](https://img.shields.io/badge/Framework-pytest_9.1.1-blue)
![Execution Time](https://img.shields.io/badge/Runtime-%3C_0.5s-orange)
![Kafka Mock](https://img.shields.io/badge/Kafka-In--Memory_Mock-purple)

This directory contains the automated test suite for the **Urbanix Sensor Simulation Layer**. It verifies unit conversion accuracy, row parsing resilience, zone mapping determinism, Kafka delivery confirmation mechanics, and raw sample dataset integrity.

---

## 🏛️ 1. Test Architecture & Design Principles

The test suite is structured around three core principles:
1. **Zero External Infrastructure Dependency:** All 16 unit tests execute completely in memory using self-contained fixtures and an in-memory `MockKafkaProducer`. They do not require a live Docker Kafka broker or Zookeeper instance.
2. **Deterministic & Fast:** The entire 19-test suite executes in **less than 0.5 seconds** (`0.36s` recorded on Python 3.10).
3. **Explicit Data Integrity Assertions:** 3 dedicated integration tests assert that committed sample files in `data/` strictly conform to project schemas, row counts, and date boundaries.

---

## 📋 2. Comprehensive Test Catalog (19 Tests)

| # | Test Function Name | Category | What It Verifies |
| :---: | :--- | :--- | :--- |
| **1** | `test_traffic_zone_mapping_determinism` | Spatial Mapping | Validates that METR-LA sensor IDs map deterministically via modulo arithmetic `(sensor_id % 5) + 1` across `zone_1` to `zone_5`. |
| **2** | `test_traffic_zone_mapping_fallback` | Spatial Mapping | Ensures non-numeric sensor IDs trigger character-sum deterministic fallback rather than throwing uncaught exceptions. |
| **3** | `test_aqi_zone_mapping` | Spatial Mapping | Validates that EPA AQS station numbers map to documented sub-regional LA County zones (`1103` $\to$ `zone_1`, `0002` $\to$ `zone_3`). |
| **4** | `test_aqi_zone_mapping_fallback` | Spatial Mapping | Verifies deterministic fallback on unknown or newly deployed station identifiers. |
| **5** | `test_weather_zones` | Spatial Mapping | Asserts that regional atmospheric weather returns all 5 zones (`zone_1` through `zone_5`) for uniform broadcast. |
| **6** | `test_traffic_unit_conversion` | Unit Conversion | Verifies conversion formula $\text{km/h} = \text{round}(\text{mph} \times 1.60934, 2)$ ($60.0 \text{ mph} \to 96.56 \text{ km/h}$). |
| **7** | `test_traffic_parsing_valid_fixture` | Ingestion Parsing | Tests wide-to-stream parsing of 207 sensors across multiple time slices using an in-memory CSV fixture. |
| **8** | `test_traffic_parsing_malformed_rows` | Error Resilience | Verifies that corrupted rows (missing values, unparseable strings) are logged and skipped without crashing the stream. |
| **9** | `test_traffic_missing_file_raises` | Error Handling | Asserts that non-existent file paths raise `FileNotFoundError` immediately with descriptive errors. |
| **10** | `test_aqi_parsing_and_validation` | Ingestion Parsing | Validates EPA CSV row parsing, PM2.5 float extraction, and Air Quality Index bounds ($0 \le \text{AQI} \le 500$). |
| **11** | `test_aqi_missing_file_raises` | Error Handling | Asserts that missing EPA input files raise `FileNotFoundError`. |
| **12** | `test_weather_unit_conversion` | Unit Conversion | Verifies thermodynamic conversion formula $T_C = \text{round}(T_K - 273.15, 2)$ ($300.0 K \to 26.85^\circ C$). |
| **13** | `test_weather_parsing_and_regional_broadcast` | Ingestion Parsing | Asserts that a single hourly weather row emits exactly 5 events—one for each of the 5 municipal zones. |
| **14** | `test_weather_parsing_out_of_bounds` | Physical Sanity | Asserts that physically impossible readings ($500K$ or $-50\%$ humidity) are rejected as sensor anomalies. |
| **15** | `test_weather_missing_file_raises` | Error Handling | Asserts that missing weather sample files raise `FileNotFoundError`. |
| **16** | `test_mock_kafka_delivery_callbacks` | Delivery Guarantees | Mocks `KafkaProducer`, asserting that asynchronous `on_send_success` and `on_send_error` callbacks fire accurately. |
| **17** | `test_aqi_sample_file_window` | Data Integrity | Asserts that `data/aqi_sample.csv` contains observations strictly between `2012-03-01` and `2012-06-30` (712 records). |
| **18** | `test_traffic_sample_file_structure` | Data Integrity | Asserts that `data/traffic_sample.csv` contains exactly 207 unique loop detector sensor columns. |
| **19** | `test_weather_sample_file_pairing` | Data Integrity | Asserts that `data/weather_sample.csv` contains valid paired temperature ($^\circ C$) and humidity ($\%$) readings. |

---

## ⌨️ 3. Execution Instructions (PowerShell)

Execute tests from the project root or the `services/producers` directory with the virtual environment activated:

```powershell
# Activate producers virtual environment
.\services\producers\venv\Scripts\Activate.ps1

# Run the complete test suite with verbose output
pytest services/producers/tests -v

# Run only spatial zone mapping tests
pytest services/producers/tests -k "zone"

# Run with test duration profiling
pytest services/producers/tests --durations=5
```
