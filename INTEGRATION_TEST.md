# 🧪 Urbanix Day 9 — End-to-End Forecast-to-Reroute Integration Test Runbook

This runbook has been placed in the canonical documentation directory at:
[`_day-notes/day-09/INTEGRATION_TEST.md`](_day-notes/day-09/INTEGRATION_TEST.md)

Please refer to that file for the complete, step-by-step Windows PowerShell execution procedure, synthetic injection scripts, troubleshooting guide, automated regression summary, and the viva defense reference.

### Quick Start (Single-Pass Verification)
```powershell
# 1. Run full Scala test suite (43 passing tests)
cd services/graphx-reroute
.\sbt.bat --batch test

# 2. Execute RouteRecommender single-pass snapshot
.\sbt.bat --batch "runMain urbanix.graphx.RouteRecommender --once"
cd ../..

# 3. Inspect generated route recommendations
.\services\streaming\venv\Scripts\python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').getOrCreate()
df = spark.read.parquet('data/recommended_routes')
df.select('origin_id', 'destination_id', 'total_travel_time_minutes', 'path_formatted', 'is_reachable').show(20, truncate=False)
"
```
