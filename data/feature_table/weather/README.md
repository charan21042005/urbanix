# 🌦️ Weather Streaming Feature Table (`data/feature_table/weather/`)

![Status](https://img.shields.io/badge/Status-Active%20%26%20Streaming%20(Day%203)-brightgreen)
![Topic](https://img.shields.io/badge/Topic-weather--topic-blue)
![Format](https://img.shields.io/badge/Format-Snappy_Parquet-orange)
![Metrics](https://img.shields.io/badge/Metrics-Temp_%E2%80%A2_Humidity_%E2%80%A2_Wind-purple)

This directory stores the streaming feature table produced by the `weather_query` structured streaming pipeline in `services/streaming/streaming_job.py`. It aggregates surface weather telemetry into 5-minute tumbling windows.

---

## 📊 1. Provenance & Source Telemetry
- **Source Topic:** `weather-topic` (replaying historical meteorological records from OpenWeather archives).
- **Spatial Coverage:** Los Angeles County surface observation points mapped to 5 zones.
- **Physical Bounds:**
  - Temperature: $-20.0^\circ C \le T \le 60.0^\circ C$ (converted from Kelvin).
  - Humidity: $0.0\% \le H \le 100.0\%$.
  - Wind Speed: $0.0 \le W \le 150.0\text{ km/h}$.
- **Window Strategy:** 5-minute tumbling windows governed by a 2-minute event-time watermark.

---

## 🏛️ 2. Schema Definition

```
root
 |-- window_start: timestamp (nullable = true)           # Window start timestamp (UTC)
 |-- window_end: timestamp (nullable = true)             # Window end timestamp (UTC)
 |-- zone_id: string (nullable = true)                   # Municipal Zone (zone_1 to zone_5)
 |-- avg_temperature_c: double (nullable = true)         # Mean surface temperature in Celsius
 |-- avg_humidity_pct: double (nullable = true)          # Mean relative humidity percentage
 |-- avg_wind_speed_kmph: double (nullable = true)       # Mean wind speed in km/h
 |-- weather_record_count: long (nullable = true)        # Total individual weather events in window
```

---

## 🚀 3. Role in Downstream Analytics

1. **Adverse Weather Risk Adjustments (Day 7):**
   - High humidity combined with sudden temperature drops triggers localized fog/slick-road risk multipliers.
2. **Dynamic Road Capacity Penalties (Weekend 3):**
   - Rain and wind reduce the theoretical free-flow speed on highway edges in the GraphX shortest-path engine.
