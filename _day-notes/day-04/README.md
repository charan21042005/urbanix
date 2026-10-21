# Urbanix — Day 4: Feature Engineering & Chronological Train/Test Split

Welcome to the **Day 4 Module Documentation** for Project Urbanix.

## Milestone Summary
- **Module:** Weekend 2 — Distributed Analytics & Graph Engine (Day 4 of 12)
- **Primary Language:** Python (PySpark)
- **Core Engine:** Apache Spark MLlib 3.5.0 (`VectorAssembler`, `StringIndexer`)
- **Key Deliverables:** 300-second Continuity Guard, 9-dimensional `VectorUDT` assembly, future velocity target formulation ($y = \text{next\_avg\_speed\_kmph}$), 80/20 elapsed-time chronological split (`2012-03-01 06:24:00 UTC`), mathematically proven zero temporal data leakage
- **Status:** **VERIFIED & FORMALLY CLOSED** (All Acceptance Gates Passed)
- **Total Tests:** 11 Passing Automated Tests in `services/streaming/tests/test_training_data.py`

## Directory Contents
- [DAY_04_FEATURE_ENGINEERING_REPORT.md](DAY_04_FEATURE_ENGINEERING_REPORT.md) — Comprehensive technical report detailing continuity guard logic, cyclic diurnal harmonic encodings ($\sin(2\pi h/24), \cos(2\pi h/24)$), chronological boundary enforcement, leakage verification proofs, and test execution logs.
- [../../services/streaming/prepare_training_data.py](../../services/streaming/prepare_training_data.py) — Feature preparation and chronological split script.
- [../../services/streaming/verify_split.py](../../services/streaming/verify_split.py) — Independent chronological split and leakage verification utility.
- [../../data/train/](../../data/train/) — 365-row chronological training dataset.
- [../../data/test/](../../data/test/) — 100-row held-out chronological test dataset.

## Quick Execution Commands
```powershell
# Navigate to streaming directory
cd services/streaming

# 1. Run automated feature engineering test suite (11 tests)
python -m pytest tests/test_training_data.py -v

# 2. Run independent split and zero-leakage verifier
python verify_split.py
```
