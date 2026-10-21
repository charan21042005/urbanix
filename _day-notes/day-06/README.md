# Urbanix — Day 6: Live MLlib Streaming Inference & Checkpoint Recovery

Welcome to the **Day 6 Module Documentation** for Project Urbanix.

## Milestone Summary
- **Module:** Weekend 2 — Distributed Analytics & Graph Engine (Day 6 of 12)
- **Primary Language:** Python (PySpark)
- **Core Engine:** Apache Spark Structured Streaming 3.5.0 + Spark MLlib 3.5.0
- **Key Deliverables:** Startup driver model deserialization (`models/linear_regression/`), dynamic 9-dimensional vector feature parity, `foreachBatch` streaming micro-batch scoring, physical velocity bounding ($0 < v \le 250\text{ km/h}$), durable checkpoint offsets under `infra/checkpoints/traffic/`, verified OS-level hard process crash recovery (`taskkill /F`)
- **Status:** **VERIFIED & FORMALLY CLOSED** (All Acceptance Gates Passed)
- **Total Tests:** 10 Passing Automated Tests in `services/streaming/tests/test_streaming_inference.py`

## Directory Contents
- [DAY_06_LIVE_INFERENCE_REPORT.md](DAY_06_LIVE_INFERENCE_REPORT.md) — Exhaustive 1,151-line pedagogical compendium and academic defense manual covering static vs unbounded execution plans, `foreachBatch` micro-batch architecture, Write-Ahead Logs (WAL), commit offset tracking, and 35 viva questions.
- [CRASH_TEST.md](CRASH_TEST.md) — Comprehensive runbook and empirical evidence log of the genuine OS-level hard crash-and-recovery test.
- [../../services/streaming/streaming_inference.py](../../services/streaming/streaming_inference.py) — Live streaming model scoring engine.
- [../../services/streaming/test_checkpoint_recovery.py](../../services/streaming/test_checkpoint_recovery.py) — Multi-batch crash recovery test harness.
- [../../infra/checkpoints/traffic/](../../infra/checkpoints/traffic/) — Durable streaming checkpoint directory (`offsets/`, `commits/`, `metadata`).

## Quick Execution Commands
```powershell
# Navigate to streaming directory
cd services/streaming

# 1. Run automated live streaming inference test suite (10 tests)
python -m pytest tests/test_streaming_inference.py -v

# 2. Run multi-batch checkpoint crash-and-recovery harness
python test_checkpoint_recovery.py
```
