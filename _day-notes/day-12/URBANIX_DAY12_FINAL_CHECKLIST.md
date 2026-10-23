# 📋 URBANIX — DAY 12 FINAL READINESS CHECKLIST & CAPSTONE AUDIT GATE
## Engineering Completion Gate, Physical Artifact Lineage, and Evaluation Sign-Off

**Project:** Urbanix — Smart City IoT Traffic & Environmental Risk Forecasting Platform  
**Course:** INT315 — Cluster Computing Capstone  
**Milestone:** Day 12 of 12 (Final Capstone Closure)  
**Date:** October 23, 2026  
**Auditor / Lead Engineer:** Senior Distributed Systems & Academic Capstone Lead  
**Overall Readiness Verdict:** **READY WITH DISCLOSED LIMITATIONS**  

---

## 📑 1. Physical Verification Matrix (Checklist with Evidence References)

| Verification Item | Status | Primary Physical Artifact / Command Reference | Notes / Remaining Limitations |
| :--- | :---: | :--- | :--- |
| **1. Repository Baseline Recorded** | ✅ PASS | `git status`, `git branch -vv`, HEAD `109d862` | Clean working tree; 3 local commits ahead of origin/main. |
| **2. Git Hygiene & Artifact Safety** | ✅ PASS | `.gitignore` excludes Parquet part files and checkpoints | Protected production data and checkpoint Write-Ahead Logs. |
| **3. Original SRS Preserved** | ✅ PASS | `1.Project1_SmartCityIoT_SRS.docx` (Unmodified) | Original planning file untouched in Planning directory. |
| **4. Revised SRS Authored** | ✅ PASS | `_day-notes/day-12/URBANIX_SRS_REVISED.md` | Complete revision with all real metrics and Pregel details. |
| **5. Model Metrics Reconciled** | ✅ PASS | `models/linear_regression/`, `models/decision_tree/` | Linear Regression: $R^2 = 0.7757$, RMSE 2.93 km/h; Decision Tree $R^2 = -0.2064$. |
| **6. Dataset Provenance Documented** | ✅ PASS | `data/sample_traffic.csv`, `data/sample_aqi.csv` | Caltrans PeMS METR-LA (2012), EPA AirData, OpenWeather. |
| **7. Functional Requirements Verified** | ✅ PASS | FR-1 through FR-10 mapped in Claim-Evidence Matrix | Speed conversion, windowing, GraphX routing, APIs verified. |
| **8. Architecture & Data Flow** | ✅ PASS | Section 3 in `URBANIX_DAY12_FINAL_AUDIT_REPORT.md` | Decoupled Kafka -> Spark -> MLlib -> GraphX -> FastAPI flow. |
| **9. GraphX Topology & Routing** | ✅ PASS | `RoadGraphBuilder.scala`, `ShortestPathPregel.scala` | 20 vertices, 40 directed edges; travel time in hours/minutes. |
| **10. Edge Cost Formula & Floor** | ✅ PASS | `CongestionWeight.scala` lines 50–93 | Formula: $\text{dist} / v_{\text{eff}}$; $1.0\text{ km/h}$ floor prevents division by zero. |
| **11. Shared Parquet Route Store** | ✅ PASS | `data/recommended_routes/` (20 rows, snapshot) | Destination 15 baseline ETA: 19.02 min; OVERWRITE semantics. |
| **12. API Contracts & Freshness** | ✅ PASS | `dashboard/main.py` lines 400–500 | 180s route stale threshold, 900s prediction stale threshold. |
| **13. Dashboard Regression Tests** | ✅ PASS | `dashboard/tests/test_dashboard.py` (47/47 passed) | Pytest suite passes in 9.41s; verified PolyLine and errors. |
| **14. Live HTTP Smoke Tests** | ✅ PASS | `dashboard/tests/smoke_test.py` (9/9 passed) | All 9 live HTTP endpoints verified against Uvicorn server. |
| **15. Producer Unit Tests** | ✅ PASS | `services/producers/tests/test_producers.py` | 19/19 passed in 0.42s; unit conversion and schemas verified. |
| **16. MLlib Model Training Tests** | ✅ PASS | `services/streaming/tests/test_model_training.py` | 9/9 passed in 115.35s; numerical tolerance verified. |
| **17. Checkpoint Write-Ahead Logs** | ⚠️ PARTIAL | `infra/checkpoints/traffic/` (Commits 0–1, deltas 1–6) | Checkpoints physically present; live recovery paused in audit. |
| **18. Safe Dry Run Executed** | ✅ PASS | Level 1 & Level 2 verified; Level 3 safely blocked | Port 8000 conflict identified; alternate port 8085 tested. |
| **19. Three-Minute Walkthrough** | ✅ PASS | `_day-notes/day-12/URBANIX_DAY12_THREE_MINUTE_WALKTHROUGH.md` | Word-for-word timed script (0:00–3:00) with screen actions. |
| **20. Master Viva Question Bank** | ✅ PASS | `_day-notes/day-12/URBANIX_MASTER_VIVA_BANK.md` | Consolidated Q&A across 9 thematic groups + 5 hard probes. |
| **21. Contingency Failure Plan** | ✅ PASS | Section 3 of Three-Minute Walkthrough | Zero-panic offline fallback protocol using cached evidence. |
| **22. Rehearsal Plan Tracking** | ⚠️ PENDING | Section 5 of Three-Minute Walkthrough | Requires student to personally speak script out loud 3 times. |
| **23. Final Git Tag Marking** | ⚠️ PENDING | Pending student authorization (Safety Rule 3) | Recommended: `git tag -a v1.0-submission -m "..."`. |
| **24. Disclosure of Limitations** | ✅ PASS | Executive Summary of Final Audit Report | AQI forecasting deferred; live cluster unverified in audit. |

---

## 2. Unchecked Items & Exactly What is Required

### Item 17: Live Checkpoint Recovery
- **Current State:** Checkpoints exist on disk in `infra/checkpoints/traffic/`, but live failure injection against an active streaming query was not re-executed during Day 12 to prevent corrupting state.
- **Action Required for Level 3:** If required by evaluators, execute Section K.3 of `FULL_SYSTEM_TEST.md` using a dedicated temporary checkpoint directory (`infra/checkpoints/traffic_test/`).

### Item 22: Personal Spoken Rehearsal
- **Current State:** Script is fully drafted, timed, and structured with examiner interruptions.
- **Action Required for Student:** Speak the script out loud using a real stopwatch at least 3 times. Verify delivery fits within 180 seconds.

### Item 23: Final Git Tag & Remote Push
- **Current State:** Local `main` is ahead of `origin/main` by 3 commits. Working tree is clean.
- **Action Required for Student:** Explicitly authorize Git push and tag creation:
  ```powershell
  git tag -a v1.0-submission -m "Final Urbanix Capstone Submission: All 12 Days Complete"
  # Optional: git push origin main --tags (Only when authorized)
  ```

---

## 3. Capstone Deliverables Index

All master deliverables for Day 12 are persisted locally in `_day-notes/day-12/`:
1. [URBANIX_DAY12_FINAL_AUDIT_REPORT.md](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-12/URBANIX_DAY12_FINAL_AUDIT_REPORT.md)
2. [URBANIX_SRS_REVISED.md](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-12/URBANIX_SRS_REVISED.md)
3. [URBANIX_DAY12_THREE_MINUTE_WALKTHROUGH.md](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-12/URBANIX_DAY12_THREE_MINUTE_WALKTHROUGH.md)
4. [URBANIX_MASTER_VIVA_BANK.md](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-12/URBANIX_MASTER_VIVA_BANK.md)
5. [URBANIX_DAY12_FINAL_CHECKLIST.md](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/_day-notes/day-12/URBANIX_DAY12_FINAL_CHECKLIST.md)
