# 📅 Urbanix Daily Engineering Journal & Milestone Tracker (`_day-notes/`)

![Roadmap Progress](https://img.shields.io/badge/Progress-Day_4_of_12_Complete_(33.3%25)-brightgreen)
![Day 1 Status](https://img.shields.io/badge/Day_1-Verified_&_Closed-brightgreen)
![Day 2 Status](https://img.shields.io/badge/Day_2-Verified_&_Closed-brightgreen)
![Day 3 Status](https://img.shields.io/badge/Day_3-Verified_&_Closed-brightgreen)
![Day 4 Status](https://img.shields.io/badge/Day_4-Verified_&_Closed-brightgreen)
![Day 5 Status](https://img.shields.io/badge/Day_5-Active_Next-blue)

> [!WARNING]
> **DEVELOPMENT & VIVA ARTIFACT NOTICE**
> The documents inside `_day-notes/` are detailed daily engineering retrospectives and atomic implementation logs. They serve as the **authoritative source of truth** for project recaps, technical interview preparation, and external capstone viva defense.

---

## 🗺️ 1. 12-Day Capstone Master Roadmap & Tracking Matrix

```mermaid
graph TD
    %% Phase 1: Core Foundation & Ingestion
    subgraph P1["<b>PHASE 1: Core Foundation & Ingestion (Days 1–2)</b> — [100% COMPLETE]"]
        D1["<b>Day 1: Distributed Infrastructure Foundation</b><br>Zookeeper • Kafka Broker • Spark Master & Worker<br><b>Status: COMPLETE</b>"]
        D2["<b>Day 2: Sensor Simulation Layer & Provenance</b><br>METR-LA • EPA AirData • OpenWeather • 3 Kafka Producers<br><b>Status: COMPLETE</b>"]
        D1 --> D2
    end

    %% Phase 2: Distributed Analytics & Graph Engine
    subgraph P2["<b>PHASE 2: Distributed Analytics & Graph Engine (Days 3–6)</b> — [ACTIVE / NEXT]"]
        D3["<b>Day 3: Spark Structured Streaming Engine</b><br>Kafka Ingestion • Watermarking • Multi-Stream Window Joins<br><b>Status: UPCOMING NEXT</b>"]
        D4["<b>Day 4: Real-Time ML Risk Forecasting</b><br>Spark MLlib • Feature Engineering • Municipal Risk Index<br><b>Status: PLANNED</b>"]
        D5["<b>Day 5: Road Network Graph Modeling</b><br>Scala 2.12 • Apache Spark GraphX • LA Topological Graph<br><b>Status: PLANNED</b>"]
        D6["<b>Day 6: Dynamic Congestion Rerouting</b><br>GraphX Dijkstra • Pregel API • Dynamic Edge Weights<br><b>Status: PLANNED</b>"]
        D3 --> D4 --> D5 --> D6
    end

    %% Phase 3: Analytical Sinks & Operator Dashboard
    subgraph P3["<b>PHASE 3: Analytical Sinks & Operator Dashboard (Days 7–10)</b> — [PLANNED]"]
        D7["<b>Day 7: Analytical Data Sinks</b><br>Parquet Checkpoint Lake • PostgreSQL Risk Sinks<br><b>Status: PLANNED</b>"]
        D8["<b>Day 8: API Gateway & WebSocket Server</b><br>FastAPI Async Server • Real-Time Telemetry WebSockets<br><b>Status: PLANNED</b>"]
        D9["<b>Day 9: Geospatial Dashboard Core</b><br>React 18 • Vite • MapLibre GL • deck.gl 3D Hex Heatmaps<br><b>Status: PLANNED</b>"]
        D10["<b>Day 10: Interactive Operator Dashboard</b><br>Route Comparator • Live Gauges • Recharts Time-Series<br><b>Status: PLANNED</b>"]
        D7 --> D8 --> D9 --> D10
    end

    %% Phase 4: Benchmarks & Defense
    subgraph P4["<b>PHASE 4: Benchmarks & Capstone Defense (Days 11–12)</b> — [PLANNED]"]
        D11["<b>Day 11: End-to-End Testing & Chaos Benchmarks</b><br>Ingestion Latency • Fault Recovery • Stress Benchmarks<br><b>Status: PLANNED</b>"]
        D12["<b>Day 12: Capstone Packaging & Viva Defense</b><br>Academic Defense • Final Report • Slide Deck Archival<br><b>Status: PLANNED</b>"]
        D11 --> D12
    end

    %% Inter-Phase Transitions
    D2 ==>|Kafka Stream Topics| D3
    D6 ==>|Dynamic Edge Weights| D7
    D10 ==>|Full Platform Integration| D11

    %% Explicit High-Contrast Styling (Guaranteed Readability in Light & Dark Themes)
    style D1 fill:#d4edda,stroke:#28a745,stroke-width:2px,color:#155724
    style D2 fill:#d4edda,stroke:#28a745,stroke-width:2px,color:#155724
    style D3 fill:#cce5ff,stroke:#007bff,stroke-width:2px,color:#004085
    style D4 fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529
    style D5 fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529
    style D6 fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529
    style D7 fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529
    style D8 fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529
    style D9 fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529
    style D10 fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529
    style D11 fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529
    style D12 fill:#f8f9fa,stroke:#6c757d,stroke-width:1px,color:#212529
```

### 🛣️ Visual Phase-by-Phase Timeline
```text
Phase 1: Foundation (COMPLETE)        Phase 2: Analytics & Graph (NEXT)           Phase 3: Sinks & Dashboard (PLANNED)        Phase 4: Finalization
┌──────────────────────────────────┐  ┌────────────────────────────────────────┐  ┌────────────────────────────────────────┐  ┌───────────────────────────────┐
│ [Day 01] Docker & Spark Cluster✅│  │ [Day 03] Spark Structured Streaming ⏳ │  │ [Day 07] Parquet Data Sinks         📅 │  │ [Day 11] E2E Stress Testing 📅 │
│ [Day 02] Sensors & Kafka Replay✅│──▶ [Day 04] MLlib Risk Forecasting      📅 │──▶ [Day 08] FastAPI & WebSockets       📅 │─▶│ [Day 12] Capstone Defense   📅 │
└──────────────────────────────────┘  │ [Day 05] GraphX Road Modeling        📅 │  │ [Day 09] React & deck.gl 3D Maps    📅 │  └───────────────────────────────┘
                                      │ [Day 06] Congestion Rerouting Engine 📅 │  │ [Day 10] Operator Command Console   📅 │
                                      └────────────────────────────────────────┘  └────────────────────────────────────────┘
```

| Day | Milestone / Module | Primary Tech Stack | Status | Deep-Dive Report Link |
| :---: | :--- | :--- | :---: | :--- |
| **01** | **Distributed Infrastructure Foundation** | Docker Compose, Zookeeper, Kafka, Apache Spark Master & Worker | **COMPLETE ✅** | [`day-01/DAY_01_INFRASTRUCTURE_REPORT.md`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-01/DAY_01_INFRASTRUCTURE_REPORT.md) |
| **02** | **Sensor Simulation Layer & Data Provenance** | Python 3.10, `kafka-python-ng`, pytest, METR-LA, EPA AirData, OpenWeather | **COMPLETE ✅** | [`day-02/DAY_02_SENSOR_SIMULATION_REPORT.md`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-02/DAY_02_SENSOR_SIMULATION_REPORT.md) |
| **03** | **Spark Structured Streaming Engine** | PySpark 3.5.1, Kafka Connector, Watermarking, Independent Parquet Feature Tables | **COMPLETE ✅** | [`day-03/DAY_03_STREAMING_REPORT.md`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-03/DAY_03_STREAMING_REPORT.md) |
| **04** | **Feature Engineering & Chronological Split** | Spark MLlib, VectorAssembler, StringIndexer, Zero-Leakage Split | **COMPLETE ✅** | [`day-04/DAY_04_FEATURE_ENGINEERING_REPORT.md`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-04/DAY_04_FEATURE_ENGINEERING_REPORT.md) |
| **05** | **MLlib Regression & Forecasting Evaluation** | Spark MLlib, Linear Regression, Decision Tree Regressor, Evaluator | **ACTIVE NEXT ⏳** | *Planned: `day-05/DAY_05_ML_FORECASTING_REPORT.md`* |
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
