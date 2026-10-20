# 🌫️ Air Quality Streaming Feature Table (`data/feature_table/aqi/`)

![Status](https://img.shields.io/badge/Status-Active%20%26%20Streaming%20(Day%203)-brightgreen)
![Topic](https://img.shields.io/badge/Topic-aqi--topic-red)
![Metric](https://img.shields.io/badge/Metric-PM2.5_%26_AQI-blue)
![Format](https://img.shields.io/badge/Format-Snappy_Parquet-orange)

This directory houses the streaming feature table produced by the `aqi_query` structured streaming pipeline in `services/streaming/streaming_job.py`. It aggregates high-frequency ambient particulate matter ($PM_{2.5}$) telemetry into 5-minute tumbling windows.

---

## 📊 1. Provenance & Source Telemetry
- **Source Topic:** `aqi-topic` (ingesting real U.S. EPA Air Quality System records from `data/aqi_sample.csv`).
- **Physical Sensor Stations:** 10 monitoring sites across Los Angeles County (e.g., North Main St, Burbank, Reseda, Pasadena, Azusa, Compton, Long Beach).
- **Physical Bounds:** $0.0 \le PM_{2.5} \le 500.0\ \mu g/m^3$.
- **Window Strategy:** 5-minute tumbling event-time windows with 2-minute watermarking.

---

## 🏛️ 2. Schema Definition

```
root
 |-- window_start: timestamp (nullable = true)           # Window start timestamp (UTC)
 |-- window_end: timestamp (nullable = true)             # Window end timestamp (UTC)
 |-- zone_id: string (nullable = true)                   # Municipal Zone (zone_1 to zone_5)
 |-- avg_pm25: double (nullable = true)                  # Mean PM2.5 concentration (ug/m3) in window
 |-- min_pm25: double (nullable = true)                  # Minimum instantaneous PM2.5 in window
 |-- max_pm25: double (nullable = true)                  # Maximum instantaneous PM2.5 in window
 |-- avg_aqi: double (nullable = true)                   # Mean calculated EPA Air Quality Index
 |-- aqi_record_count: long (nullable = true)            # Number of individual sensor events aggregated
```

---

## 🚀 3. Role in Downstream Analytics

1. **Multimodal Environmental Risk Scoring (Day 7):**
   - In Day 7, the PM2.5 feature table is joined with traffic congestion forecasts to compute composite roadway health risk indices.
2. **Pedestrian & Cyclist Route Weighting (Weekend 3):**
   - High PM2.5 levels trigger environmental edge penalties on GraphX road segments, rerouting sensitive populations away from high-pollution corridors.
