# 📅 Urbanix Daily Engineering Journal & Milestone Tracker (`_day-notes/`)

![Roadmap Progress](https://img.shields.io/badge/Progress-Day_2_of_12_Complete_(16.7%25)-brightgreen)
![Day 1 Status](https://img.shields.io/badge/Day_1-Verified_&_Closed-brightgreen)
![Day 2 Status](https://img.shields.io/badge/Day_2-Verified_&_Closed-brightgreen)
![Day 3 Status](https://img.shields.io/badge/Day_3-Active_Next-blue)

> [!WARNING]
> **DEVELOPMENT & VIVA ARTIFACT NOTICE**
> The documents inside `_day-notes/` are detailed daily engineering retrospectives and atomic implementation logs. They serve as the **authoritative source of truth** for project recaps, technical interview preparation, and external capstone viva defense.

---

## 🗺️ 1. 12-Day Capstone Master Roadmap & Tracking Matrix

```mermaid
gantt
    title Urbanix 12-Day Engineering Roadmap
    dateFormat  YYYY-MM-DD
    section Phase 1: Core Foundation
    Day 1 - Infrastructure & Cluster Setup      :done,    d1, 2026-10-17, 1d
    Day 2 - Sensor Simulation & Kafka Producers :done,    d2, 2026-10-18, 1d
    section Phase 2: Distributed Analytics
    Day 3 - Spark Structured Streaming Engine  :active,  d3, 2026-10-19, 1d
    Day 4 - Real-Time ML Risk Forecasting       :         d4, 2026-10-20, 1d
    Day 5 - GraphX Road Network Modeling        :         d5, 2026-10-21, 1d
    Day 6 - Dynamic Graph Rerouting Engine      :         d6, 2026-10-22, 1d
    section Phase 3: Storage & Presentation
    Day 7 - Analytical Data Sinks (Parquet/SQL) :         d7, 2026-10-23, 1d
    Day 8 - FastAPI Gateway & WebSockets        :         d8, 2026-10-24, 1d
    Day 9 - React & deck.gl Geospatial Frontend :         d9, 2026-10-25, 1d
    Day 10 - Interactive Operator Dashboard     :         d10, 2026-10-26, 1d
    section Phase 4: Finalization
    Day 11 - End-to-End Verification & Benchmarks:       d11, 2026-10-27, 1d
    Day 12 - Capstone Defense & Packaging       :         d12, 2026-10-28, 1d
```

| Day | Milestone / Module | Primary Tech Stack | Status | Deep-Dive Report Link |
| :---: | :--- | :--- | :---: | :--- |
| **01** | **Distributed Infrastructure Foundation** | Docker Compose, Zookeeper, Kafka, Apache Spark Master & Worker | **COMPLETE ✅** | [`day-01/DAY_01_INFRASTRUCTURE_REPORT.md`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-01/DAY_01_INFRASTRUCTURE_REPORT.md) |
| **02** | **Sensor Simulation Layer & Data Provenance** | Python 3.10, `kafka-python-ng`, pytest, METR-LA, EPA AirData, OpenWeather | **COMPLETE ✅** | [`day-02/DAY_02_SENSOR_SIMULATION_REPORT.md`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-02/DAY_02_SENSOR_SIMULATION_REPORT.md) |
| **03** | **Spark Structured Streaming Engine** | PySpark 3.5.1, Kafka Connector, Watermarking, Multi-Stream Window Joins | **UPCOMING ⏳** | *Planned: `day-03/DAY_03_STREAMING_REPORT.md`* |
| **04** | **Real-Time ML Risk Forecasting** | Spark MLlib, Linear Regression, Decision Trees, Feature Engineering | **PLANNED 📅** | *Planned: `day-04/DAY_04_ML_FORECASTING_REPORT.md`* |
| **05** | **Road Network Graph Modeling** | Scala 2.12, Apache Spark GraphX, Los Angeles Topological Network | **PLANNED 📅** | *Planned: `day-05/DAY_05_GRAPHX_MODELING_REPORT.md`* |
| **06** | **Congestion-Aware Graph Rerouting** | Scala GraphX, Dijkstra's Algorithm, Pregel API, Dynamic Edge Recalibration | **PLANNED 📅** | *Planned: `day-06/DAY_06_REROUTING_REPORT.md`* |
| **07** | **Data Storage Layer & Analytical Sinks** | Partitioned Parquet, Data Lake Checkpointing, PostgreSQL Sinks | **PLANNED 📅** | *Planned: `day-07/DAY_07_STORAGE_SINKS_REPORT.md`* |
| **08** | **API Gateway & Telemetry Streaming** | FastAPI, Uvicorn, Asynchronous WebSockets, Pydantic | **PLANNED 📅** | *Planned: `day-08/DAY_08_FASTAPI_GATEWAY_REPORT.md`* |
| **09** | **Geospatial Dashboard Core** | React 18, Vite, MapLibre GL, deck.gl 3D Hexagonal Heatmaps | **PLANNED 📅** | *Planned: `day-09/DAY_09_DASHBOARD_CORE_REPORT.md`* |
| **10** | **Interactive Rerouting & Trend UI** | React Components, Recharts, Origin-Destination Path Comparator | **PLANNED 📅** | *Planned: `day-10/DAY_10_DASHBOARD_INTEGRATION_REPORT.md`* |
| **11** | **End-to-End Stress Testing & Chaos Verification** | Load Testing, Ingestion Latency Benchmarks, Broker Crash Recovery | **PLANNED 📅** | *Planned: `day-11/DAY_11_TESTING_VERIFICATION_REPORT.md`* |
| **12** | **Capstone Packaging, Documentation & Viva Defense** | Final Academic Report, Slide Deck, Code Archival, Defense Rehearsal | **PLANNED 📅** | *Planned: `day-12/DAY_12_FINALIZATION_VIVA_REPORT.md`* |

---

## 📑 2. Completed Milestone Retrospectives

### Day 1: Distributed Infrastructure Foundation
* **Report:** [`day-01/DAY_01_INFRASTRUCTURE_REPORT.md`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-01/DAY_01_INFRASTRUCTURE_REPORT.md)
* **Key Achievements:**
  * Created clean monorepo architecture and `.gitignore` safety boundaries.
  * Verified local Windows environment and JDK 25 session isolation while preserving Java 8 for legacy projects.
  * Authored `infra/docker-compose.yml` deploying Zookeeper, Kafka, Spark Master, and Spark Worker on an isolated bridge network (`urbanix-network`).
  * Resolved the Windows host port 8080 conflict by cleanly mapping host `8081` $\to$ container `8080`.
  * Configured dual-listener Kafka networking (`kafka:29092` internal, `localhost:9092` external).
  * Provisioned and verified topics: `traffic-topic`, `aqi-topic`, `weather-topic` (1 partition, replication factor 1).
  * Validated Spark Worker registration with Spark Master (`http://localhost:8081`).

### Day 2: Sensor Simulation Layer & Provenance
* **Report:** [`day-02/DAY_02_SENSOR_SIMULATION_REPORT.md`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-02/DAY_02_SENSOR_SIMULATION_REPORT.md)
* **Key Achievements:**
  * Established isolated Python 3.10 virtualenv using modern `kafka-python-ng` and `pytest`.
  * Vetted public real-world datasets: METR-LA (Caltrans PeMS), U.S. EPA AirData (AQS PM2.5), and OpenWeather historical records.
  * Created reproducible preprocessing pipeline in `data/prepare_data.py`.
  * Enforced strict calendar filtering on EPA AQI (March 1 – June 30, 2012, 712 observations across 10 stations), synchronizing with METR-LA.
  * Preserved authentic historical timestamps; documented the OpenWeather October 2012 gap as an event-time contract for Day 3.
  * Formulated SI unit conversions ($mph \to km/h$, $Kelvin \to ^\circ C$) and deterministic 5-zone mappings.
  * Developed three Kafka simulation producers with asynchronous delivery callbacks, 3 retries, and buffer flushes.
  * Built `run_all.py` concurrent runner tracking child PIDs and handling clean `SIGINT` termination.
  * Authored 19 automated tests (unit tests with mock Kafka + real sample assertions) running in $< 0.4s$.
  * Verified end-to-end replay and console consumer JSON outputs on all 3 topics.

---

## 🏛️ 3. Purpose & Conventions of Daily Notes

Every daily report inside `_day-notes/` follows a standardized structure:
1. **Executive Objective & Scope Boundary:** What was built vs. what was deliberately postponed.
2. **Deep Theory & Atomic Definitions:** Theoretical fundamentals of every library, protocol, and algorithm used.
3. **Architectural Blueprints:** High-contrast Mermaid diagrams and ASCII dataflows.
4. **Step-by-Step Implementation Logs:** Line-by-line breakdowns of scripts, configs, and formulas.
5. **Real-World Troubleshooting:** Documentation of real bugs encountered, root causes, and fixes.
6. **Comprehensive Viva Playbook:** 20+ examiner questions and senior-level model answers.
7. **Exhaustive Command Reference:** Step-by-step reproduction command cheat sheets.
