# Urbanix Platform — Autonomous Execution & Evidence Manifest

**Execution Date:** 2026-10-24  
**Host Platform:** Microsoft Windows 11 Home Single Language (Build 26300)  
**Shell Environment:** Windows PowerShell 5.1 / PowerShell Core  
**Repository Branch:** `main` (Commit: `9b0ecd4`)  
**Auditor / Engineer:** Autonomous Distributed Systems Verification Agent  

---

## 1. Execution Summary Table

| Phase | Description | Command Executed | Exit Code | Outcome | Evidence Log / Artifact |
| :---: | :--- | :--- | :---: | :---: | :--- |
| **A** | Environment & Runtimes Audit | `java -version`, PySpark, Kafka-Python, FastAPI version checks | `0` | **PASS** | `logs/phase_a_environment.log` |
| **B** | Infrastructure & Kafka Topics | `docker compose up -d`, `docker exec kafka ... --list` | `0` | **PASS** | `logs/phase_b_infrastructure.log` |
| **B** | Multi-Stream Producer Ingestion | `python services/producers/run_all.py 30` | `0` | **PASS** | `logs/phase_b_producer.log` |
| **D** | Feature Table & Schema Check | `python services/streaming/verify_features.py` | `0` | **PASS** | `logs/phase_d_features.log` |
| **D** | Chronological Split & Leakage | `python services/streaming/verify_split.py` | `0` | **PASS** | `reports/day-04/` & stdout |
| **E** | MLlib Model Evaluation Audit | `python services/streaming/verify_models.py` | `0` | **PASS** | `reports/day-05/metrics.json` |
| **F** | Fault Tolerance Crash Recovery | `python services/streaming/test_checkpoint_recovery.py` | `0` | **PASS** | `logs/phase_f_recovery.log` |
| **G** | Scala GraphX Pregel Engine | `sbt.bat --batch test` (43 tests) | `0` | **PASS** | `logs/phase_g_graphx.log` |
| **H** | FastAPI REST Endpoints | `uvicorn main:app --port 8001`, `/health`, `/api/predictions` | `0` | **PASS** | `logs/phase_h_api.log` |
| **H** | Browser Dashboard Rendering | Interactive browser subagent navigation to `http://localhost:8001` | `0` | **PASS** | `screenshots/fig_10_*.png` |
| **I** | Producer Unit Tests | `pytest services/producers/tests -v` (19 tests) | `0` | **PASS** | `test-results/producers_tests.log` |
| **I** | Streaming Unit Tests | `pytest services/streaming/tests -q` (44 tests) | `0` | **PASS** | `test-results/streaming_tests.log` |
| **I** | Dashboard API Tests | `pytest dashboard/tests -v` (47 tests) | `0` | **PASS** | `test-results/dashboard_tests.log` |

**Total Automated Tests Passed:** **153 / 153 Tests Passing (100% Success Rate)**

---

## 2. Evidence Artifacts Catalog

### Screenshots Directory (`reports/execution-evidence/screenshots/`)
1. **`fig_10_01_dashboard_overview.png`** (1.27 MB): High-resolution browser capture of the full Leaflet dashboard displaying the 5 Los Angeles municipal zone heat circles, Level of Service color indicators, traffic forecast list, and dynamic Pregel routes.
2. **`fig_10_02_zone_forecast_popup.png`** (1.19 MB): Close-up browser capture of the interactive popup for Zone 4 (South Central Corridor) displaying forecast velocity (72.8 km/h), traffic state (Moderate), and timestamp window.
3. **`fig_10_03_downtown_popup.png`** (1.27 MB): Close-up browser capture of the interactive popup for Downtown Los Angeles (Zone 1) displaying forecast velocity (98.6 km/h) and free-flow traffic state.

### Logs Directory (`reports/execution-evidence/logs/`)
- `phase_a_environment.log`: System specifications, Windows OS build, Java runtime, and Python virtual environment package versions.
- `phase_b_infrastructure.log`: Docker Compose service startup logs and verified Kafka topics (`traffic-topic`, `aqi-topic`, `weather-topic`, `__consumer_offsets`).
- `phase_b_producer.log`: Concurrency logs from `run_all.py` showing 3 parallel producer threads publishing to Kafka.
- `phase_d_features.log`: Verification logs proving 5-minute tumbling windows, zero duplicate keys, and static metadata joins for Traffic, AQI, and Weather feature tables.
- `phase_f_recovery.log`: 5-phase crash recovery execution log verifying SIGKILL hard process kill, checkpoint WAL durability, process resumption, and 0-duplicate reconciliation.
- `phase_g_graphx.log`: Complete ScalaTest execution log showing 43 passing tests across property graph construction, congestion weights, and Pregel shortest-path routing.
- `phase_h_api.log`: REST API response logs confirming HTTP 200 OK responses for `/health`, `/api/predictions`, and `/api/route`.

### Test Results Directory (`reports/execution-evidence/test-results/`)
- `producers_tests.log`: 19 passed tests in 0.67s.
- `streaming_tests.log`: 44 passed tests in 294.10s.
- `dashboard_tests.log`: 47 passed tests in 11.60s.
