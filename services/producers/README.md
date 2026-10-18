# 📡 Urbanix Sensor Simulation Layer (Producers)

![Status](https://img.shields.io/badge/Status-Audited%20%26%20Complete%20(Day%202)-brightgreen)
![Kafka](https://img.shields.io/badge/Kafka-localhost%3A9092-blue)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Tests](https://img.shields.io/badge/Unit_Tests-19%2F19_Passing-brightgreen)

This module implements the **Sensor Simulation Layer** for Urbanix. It continuously replays verified public historical telemetry through Kafka to simulate real-time municipal sensor streams across the 5 Urbanix smart-city zones.

> **Important Note:** These producers replay historical/public observations (METR-LA highway loop detectors, U.S. EPA AirData monitors, and OpenWeather meteorological records) at configurable accelerated rates. They do not represent live feeds collected from physical devices.

---

## 1. Producer Architecture & Topics

```
[ METR-LA (Zenodo) ]       --> traffic_producer.py --> [ traffic-topic ]
[ U.S. EPA AirData ]       --> aqi_producer.py     --> [ aqi-topic     ]
[ OpenWeather (Kaggle) ]   --> weather_producer.py --> [ weather-topic ]
```

| Producer | Script | Kafka Topic | Primary Observation | Unit | Source Origin |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Traffic** | `traffic_producer.py` | `traffic-topic` | Vehicle Speed | `km/h` (converted from `mph`) | METR-LA (Caltrans PeMS) |
| **Air Quality** | `aqi_producer.py` | `aqi-topic` | PM2.5 Concentration | `ug/m3` | U.S. EPA AirData (AQS) |
| **Weather** | `weather_producer.py` | `weather-topic` | Temperature & Humidity | `Celsius` & `%` | OpenWeatherMap Historical Data |

---

## 2. Event Contracts & Actual Sample JSON

### 2.1 Traffic Event (`traffic-topic`)
```json
{
  "zone_id": "zone_5",
  "sensor_id": "773869",
  "timestamp": "2012-03-01T00:00:00",
  "speed_kmph": 103.6,
  "speed_mph": 64.38,
  "unit": "km/h",
  "source": "METR-LA (Caltrans PeMS)"
}
```
* **Units:** Raw speed is recorded in `mph` by Caltrans loop detectors. The producer converts it explicitly: $\text{speed\_kmph} = \text{round}(\text{speed\_mph} \times 1.60934, 2)$.
* **Vehicle Count:** Omitted (METR-LA does not contain vehicle count; no fabricated data).

### 2.2 Air Quality Event (`aqi-topic`)
```json
{
  "zone_id": "zone_3",
  "station_id": "0002",
  "station_name": "Azusa",
  "city": "Azusa",
  "timestamp": "2012-03-01T00:00:00",
  "pm25": 6.3,
  "aqi": 35,
  "unit": "ug/m3",
  "source": "US EPA AirData (AQS)"
}
```
* **Units:** PM2.5 is measured in $\mu g/m^3$.
* **Temporal Alignment:** Verified to start on `2012-03-01T00:00:00`, strictly aligned with the METR-LA observation window.
* **CO2:** Omitted (not universally recorded at standard EPA criteria stations).

### 2.3 Weather Event (`weather-topic`)
```json
{
  "zone_id": "zone_1",
  "city": "Los Angeles",
  "timestamp": "2012-10-01T13:00:00",
  "temperature_c": 18.72,
  "temperature_k": 291.87,
  "humidity_pct": 88.0,
  "temperature_unit": "Celsius",
  "humidity_unit": "Percent",
  "source": "Historical Hourly Weather Data (OpenWeatherMap)"
}
```
* **Units:** Raw temperature is recorded in Kelvin ($K$). The producer converts it explicitly: $T_{\text{Celsius}} = \text{round}(T_{\text{Kelvin}} - 273.15, 2)$.
* **Spatial Mapping:** Broadcasts the identical genuine observation across all 5 zones without manufacturing artificial per-zone variations.

---

## 3. Setup & Environment (PowerShell)

### Step 1: Create and Activate Virtual Environment
```powershell
# From project root:
cd services\producers
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### Step 2: Install Required Dependencies
```powershell
pip install -r requirements.txt
```
*(Dependencies: `kafka-python-ng>=2.2.3`, `pytest>=8.0.0`)*

---

## 4. Execution Instructions

### Run Producers Individually

#### Traffic Producer
```powershell
python traffic_producer.py --interval 0.05
```

#### Air Quality Producer
```powershell
python aqi_producer.py --interval 0.1
```

#### Weather Producer
```powershell
python weather_producer.py --interval 0.1
```

### Common CLI Options
* `--bootstrap-server`: Kafka address (default: `localhost:9092` for Windows host, `kafka:29092` inside Docker).
* `--interval`: Delay between timestamp batches in seconds (default: `0.05` to `0.1`s for accelerated replay). Must be $\ge 0$.
* `--max-events`: Stop after $N$ events (useful for bounded verification). Must be $\ge 1$.
* `--dry-run`: Output JSON events to terminal without publishing to Kafka.

### Delivery Confirmation & Reliability
* Each producer registers asynchronous Kafka delivery callbacks (`on_send_success`, `on_send_error`) to prevent silent message drops.
* Configured with 3 automatic retries (`retries=3`, `retry_backoff_ms=250`).
* Flushes all pending buffers prior to clean exit and logs final confirmed vs. failed publication counts.

---

## 5. Concurrent Replay Execution

To launch all three simulation streams simultaneously:

```powershell
python run_all.py
```

To run a bounded test of 50 events per stream (150 total events across all three producers):
```powershell
python run_all.py 50
```

> **Process Management:** The runner tracks each child process PID, waits for completion, intercepts Ctrl+C for clean termination of all children, and returns a non-zero exit code if any child fails.

---

## 6. Verification with Kafka Console Consumer

Read live messages from any topic using the Docker Kafka container:

```powershell
# Inspect Traffic Topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic traffic-topic --from-beginning --max-messages 5

# Inspect AQI Topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic aqi-topic --from-beginning --max-messages 5

# Inspect Weather Topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic weather-topic --from-beginning --max-messages 5
```

---

## 7. Automated Unit Testing

Run the automated test suite verifying row parsing, unit conversions, zone mapping, date window integrity, missing value handling, and mock Kafka delivery confirmations:

```powershell
pytest tests -v
```

All 19 tests (unit tests and sample integration assertions) execute in < 1 second.
