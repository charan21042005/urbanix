# 💾 Urbanix Datasets & Data Provenance

![Status](https://img.shields.io/badge/Status-Validated%20(Day%202)-brightgreen)
![License-Traffic](https://img.shields.io/badge/Traffic_License-CC--BY%204.0-blue)
![License-AQI](https://img.shields.io/badge/AQI_License-US_Public_Domain-green)
![License-Weather](https://img.shields.io/badge/Weather_License-ODbL%201.0-lightgrey)

This directory houses verified historical benchmark datasets replayed through the Urbanix sensor simulation layer. Every dataset was selected to meet strict engineering standards: real-world provenance, open licenses, reproducible download mechanisms, and geographic consistency across the **Los Angeles Metropolitan Area**.

---

## 1. Provenance & Dataset Dossiers

### 1.1 Traffic Stream: METR-LA (Los Angeles County Highways)
* **Dataset Name:** METR-LA (Metropolitan Los Angeles Traffic Speed Benchmark)
* **Original Data Provider:** California Department of Transportation (Caltrans) Performance Measurement System (PeMS).
* **Research Provenance:** Curated by USC Information Sciences Institute for graph neural network traffic forecasting (Li et al., *Diffusion Convolutional Recurrent Neural Network: Data-Driven Traffic Forecasting*, ICLR 2018).
* **Exact Working Source URL:** [https://zenodo.org/records/5724362](https://zenodo.org/records/5724362)
* **DOI:** `10.5281/zenodo.5724362` (Semin Kwak, EPFL)
* **License:** **Creative Commons Attribution 4.0 International (CC-BY 4.0)** / Caltrans Open Public Data.
* **Actual Raw Format & File Name:** `METR-LA.csv` (Wide matrix format, ~72.4 MB uncompressed).
* **Time Range:** `2012-03-01 00:00:00` to `2012-06-30 23:55:00` (4 months, 34,272 timestamps at 5-minute intervals).
* **Geographic Coverage:** 207 highway loop-detector sensor stations across Los Angeles County, California.
* **Schema:**
  * Column 0 (Unnamed Index): Observation timestamp (`YYYY-MM-DD HH:MM:SS`).
  * Columns 1 to 207: Caltrans detector station IDs (e.g., `773869`, `767541`, `767542`, `717447`, ...).
* **Measurement Units:** **Miles Per Hour (mph)**.
* **Vehicle Count:** **None**. Strictly verified that METR-LA records speed only. No vehicle counts are fabricated.
* **Unit Conversion Formula:**
  $$\text{speed\_kmph} = \text{round}(\text{speed\_mph} \times 1.60934, 2)$$
* **Zone Assignment Strategy:** Deterministic modulo mapping of physical sensor station IDs across 5 Urbanix zones:
  $$\text{zone\_id} = \text{zone\_}(( \text{sensor\_id} \pmod 5 ) + 1)$$
* **Sample File:** `data/traffic_sample.csv` (239 timestamps across all 207 sensors = 49,473 real velocity records, spanning `2012-03-01 00:00:00` to `2012-03-01 19:50:00`).

---

### 1.2 Air Quality Stream: U.S. EPA AirData (Los Angeles County)
* **Dataset Name:** U.S. EPA Air Quality System (AQS) Criteria Pollutants — PM2.5 Local Conditions
* **Original Data Provider:** United States Environmental Protection Agency (EPA), AirData System.
* **Exact Working Source URL:** [https://aqs.epa.gov/aqsweb/airdata/daily_88101_2012.zip](https://aqs.epa.gov/aqsweb/airdata/daily_88101_2012.zip)
* **Dataset Catalog / Documentation:** [https://aqs.epa.gov/aqsweb/airdata/download_files.html](https://aqs.epa.gov/aqsweb/airdata/download_files.html)
* **License:** **U.S. Federal Government Work / Public Domain** (17 U.S.C. § 105).
* **Actual Raw File:** `daily_88101_2012.csv` (extracted from `daily_88101_2012.zip`, 3.1 MB compressed).
* **Time Range:** Calendar year 2012.
* **Preprocessed Filtering:** Strictly constrained to the METR-LA observation window: **March 1, 2012 to June 30, 2012** (`2012-03-01 <= date_local <= 2012-06-30`).
* **Geographic Coverage:** Los Angeles County, California (State FIPS `06`, County FIPS `037`), encompassing 10 active physical monitoring stations:
  * Site `1103`: Los Angeles - North Main Street (Downtown Core) -> `zone_1`
  * Site `1002`: Burbank / Site `1201`: Reseda (San Fernando Valley) -> `zone_2`
  * Site `2005`: Pasadena / Site `0002`: Azusa (San Gabriel Valley) -> `zone_3`
  * Site `1302`: Compton / Site `1602`: Pico Rivera (South Central Corridor) -> `zone_4`
  * Site `4002`: Long Beach / Site `4004`: Long Beach South / Site `9033`: Lancaster -> `zone_5`
* **Schema:**
  * `state_code`, `county_code`, `site_num`: Geographic station identifiers.
  * `date_local`: Observation date (`YYYY-MM-DD`).
  * `pm25_mean`: Arithmetic mean PM2.5 concentration in **$\mu g/m^3$**.
  * `aqi`: Official calculated EPA Air Quality Index value.
  * `site_name`, `city_name`: Human-readable location metadata.
* **CO2 Availability:** OpenAQ and EPA criteria pollutant stations track criteria aerosols (PM2.5, PM10, Ozone, NO2, CO). $CO_2$ is not measured at standard EPA criteria stations and is omitted from the contract.
* **Sample File:** `data/aqi_sample.csv` (712 real EPA PM2.5 observations across all 10 Los Angeles County stations for the 122 calendar days between `2012-03-01` and `2012-06-30`).

---

### 1.3 Weather Stream: Historical Hourly Weather Data (OpenWeatherMap)
* **Dataset Name:** Historical Hourly Weather Data 2012–2017
* **Original Data Provider:** OpenWeatherMap API / Curated by Eli Shammah (`selfishgene`).
* **Kaggle Landing Page:** [https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data](https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data)
* **Open Repository Mirror:** [https://github.com/hex-inc/datasets/tree/master/historical-hourly-weather-data](https://github.com/hex-inc/datasets/tree/master/historical-hourly-weather-data)
* **License:** **Open Database License (ODbL) v1.0**.
* **Actual Raw Files:** `temperature.csv` (~13.9 MB), `humidity.csv` (~9.0 MB), `city_attributes.csv`.
* **Geographic Coverage:** 36 international cities, specifically extracting column `Los Angeles` (Latitude: 34.0522, Longitude: -118.2437).
* **Schema:**
  * `datetime`: Timestamp (`YYYY-MM-DD HH:MM:SS`).
  * `Los Angeles`: Column recording ambient meteorological conditions.
* **Observed Measurement Units:**
  * Temperature: **Kelvin ($K$)** (e.g., `291.87 K`).
  * Relative Humidity: **Percentage (%)** (e.g., `88.0%`).
* **Unit Conversion Formula:**
  $$\text{temperature\_celsius} = \text{round}(\text{temperature\_kelvin} - 273.15, 2)$$
* **Regional Spatial Assumption:** A single regional weather observation applies across the Los Angeles grid zones without manufacturing artificial variations between zones.
* **Sample File:** `data/weather_sample.csv` (162 continuous hourly observations for Los Angeles from `2012-10-01 13:00:00` to `2012-10-08 06:00:00`). The initial row at `12:00:00` contained missing values and was dropped, yielding 162 matching pairs.
* **Temporal Offset Limitation:** The Kaggle OpenWeather dataset begins on October 1, 2012, whereas METR-LA loop data begins in March 2012. Each producer replays its stream sequentially with its authentic observation timestamp preserved.

---

## 2. Data Segregation Architecture

To maintain strict data integrity and prevent repository bloat, Urbanix uses a 4-tier segregation model:

```
[ Tier 1: Raw Public Archives ]  --> (Zenodo METR-LA 72MB, EPA Zip 3MB, Kaggle CSVs)
              ↓
[ Tier 2: Preprocessing Script ]  --> (data/prepare_data.py filters & formats)
              ↓
[ Tier 3: Local Samples ]        --> (data/*_sample.csv, kept local & git-ignored)
              ↓
[ Tier 4: Streaming Kafka Events ] --> (JSON serialized events emitted to broker)
```

1. **Raw Archives:** Never committed to Git. Downloaded on demand or stored in temporary scratch caches.
2. **Preprocessing Script (`prepare_data.py`):** Deterministic, reproducible Python script that creates trimmed, validated CSV samples.
3. **Local Samples:** Stored in `data/*.csv`. Explicitly ignored by `.gitignore` to keep the repository lightweight and clone-friendly.
4. **Kafka Events:** Real-time JSON telemetry generated on the fly during accelerated replay.

---

## 3. Data Preparation Script

To extract, clean, and regenerate the sample datasets from verified sources, run:

```powershell
python data/prepare_data.py
```

This script:
1. Validates raw source files in local scratch/download paths.
2. Trims and validates the METR-LA 207-sensor matrix.
3. Filters EPA AirData to Los Angeles County criteria monitors strictly within the March–June 2012 window.
4. Joins Kelvin temperatures and humidity percentages for Los Angeles.
5. Saves clean development samples in `data/`.

---

## 4. Report Evidence Checklist (For Semester Report)

Capture and preserve the following items for the final project report:
- [ ] **Zenodo METR-LA Page:** Screenshot of Record `5724362` showing DOI `10.5281/zenodo.5724362` and CC-BY 4.0 license.
- [ ] **EPA AirData Catalog:** Screenshot of `aqs.epa.gov/aqsweb/airdata` listing `daily_88101_2012.zip`.
- [ ] **Kaggle Weather Page:** Screenshot of `selfishgene/historical-hourly-weather-data` showing ODbL license badge.
- [ ] **File Headers:** Console output of `data/traffic_sample.csv`, `data/aqi_sample.csv`, and `data/weather_sample.csv`.
- [ ] **Console Consumers:** Screenshot of `docker exec kafka kafka-console-consumer` reading verified JSON from all 3 topics.
