# 💥 Urbanix Day 6: Checkpoint Recovery & Crash Test Runbook

This document details the **Crash-and-Restart Verification Procedure** for the Urbanix Spark Structured Streaming live inference engine. It serves as both an operational runbook for developers and an academic viva defense artifact demonstrating fault tolerance and delivery semantics in distributed stream processing.

---

## 🎯 1. Objective & Purpose

The purpose of this test is to intentionally inject an ungraceful process failure (`kill -9` / process termination) into the active Spark Structured Streaming inference pipeline to verify that:
1. The query utilizes a durable checkpoint directory (`infra/checkpoints/traffic/`).
2. Source Kafka offsets and micro-batch commit states survive process death.
3. Upon query restart, processing resumes from the last committed offset without unhandled data loss.
4. Downstream Parquet partitions remain non-corrupted and readable.

---

## 🧠 2. Distributed Systems Fundamentals: Delivery Semantics

In distributed stream processing, understanding recovery guarantees is critical:

| Semantic Guarantee | Formal Definition | Does Urbanix Day 6 Provide This? | Why? |
| :--- | :--- | :---: | :--- |
| **At-Most-Once** | Messages are processed at most once; failures may cause message loss, but duplicates never occur. | ❌ No | Checkpointing ensures uncommitted messages are re-read, so data is not lost. |
| **At-Least-Once** | No message is ever lost; in failure/recovery scenarios, previously uncommitted messages may be re-processed. | **✅ YES (Guaranteed)** | Structured Streaming checkpointing stores Kafka offsets. If a crash occurs between micro-batch computation and commit, the restarted query replays from the last committed offset. |
| **Exactly-Once** | Every message affects final state exactly once; no loss and no duplicates. | ⚠️ Conditional | Structured Streaming achieves end-to-end exactly-once ONLY when paired with an **idempotent sink** (e.g., key-value store with upsert, or transactional ACID storage like Delta Lake). Standard Parquet file sinks provide **at-least-once** guarantees because retried micro-batches append new files. |

> **Viva Defense Insight:**  
> When asked: *"Does your streaming pipeline guarantee exactly-once processing?"*  
> **Answer:** *"Our Spark Structured Streaming engine provides **at-least-once delivery guarantees** to the Parquet feature store. Source offsets are persisted durably in `infra/checkpoints/traffic/commits/` and `offsets/`. However, because standard Parquet file appends do not support transactional multi-file rollback without Delta Lake or an idempotent database upsert, a mid-batch crash reprocesses the micro-batch upon restart, ensuring zero data loss with potential duplicate records."*

---

## 🔬 3. Step-by-Step Manual Crash Test Procedure

Follow these exact steps to execute the crash-and-recovery experiment manually:

### Step 3.1: Start Infrastructure & Sensor Simulation
```powershell
# 1. Start Docker cluster (Zookeeper, Kafka, Spark)
cd infra
docker compose up -d
cd ..

# 2. Start Kafka telemetry producers in background
.\services\producers\venv\Scripts\python services/producers/run_all.py
```

### Step 3.2: Start Live Streaming Inference Job
```powershell
# Activate Python 3.10 virtual environment
.\services\streaming\venv\Scripts\python services/streaming/streaming_job.py `
    --bootstrap-servers localhost:9092 `
    --output-root data/feature_table `
    --checkpoint-root infra/checkpoints `
    --model-path models/linear_regression `
    --trigger-interval "10 seconds"
```

### Step 3.3: Observe Normal Baseline Execution
Allow the pipeline to process at least 2 full trigger cycles. Observe the console output:
```text
2026-10-20 22:02:28 [INFO] [UrbanixStreaming] Loaded primary forecasting model from 'models/linear_regression'
2026-10-20 22:02:30 [INFO] [UrbanixInference] [Batch 0] Ingested 5 windowed traffic rows for scoring.
2026-10-20 22:02:33 [INFO] [UrbanixInference] [Batch 0] Successfully appended 5 predicted traffic rows to 'data/feature_table/traffic'
2026-10-20 22:02:40 [INFO] [UrbanixInference] [Batch 1] Ingested 5 windowed traffic rows for scoring.
2026-10-20 22:02:43 [INFO] [UrbanixInference] [Batch 1] Successfully appended 5 predicted traffic rows to 'data/feature_table/traffic'
```
*Note the latest completed `batch_id` (e.g., `Batch 1`).*

### Step 3.4: Inject Ungraceful Failure (`kill -9`)
Do **NOT** use `Ctrl+C` (which initiates graceful JVM shutdown hooks). Simulate a real-world server power outage or SIGKILL:

**In PowerShell (Windows):**
```powershell
# Find PID of active streaming job
Get-Process python | Where-Object { $_.CommandLine -like "*streaming_job.py*" }

# Hard kill the process immediately
Stop-Process -Name python -Force
```

**In Linux/macOS:**
```bash
kill -9 $(pgrep -f "streaming_job.py")
```

### Step 3.5: Verify Checkpoint Persistence on Disk
Inspect `infra/checkpoints/traffic/` to confirm that checkpoint metadata survived:
```powershell
Get-ChildItem -Path infra\checkpoints\traffic\commits
Get-ChildItem -Path infra\checkpoints\traffic\offsets
```
You will see numbered commit files (e.g., `0`, `1`) recording the exact JSON offset state.

### Step 3.6: Restart Streaming Query Using the Same Checkpoint
Re-execute the **EXACT SAME** startup command:
```powershell
.\services\streaming\venv\Scripts\python services/streaming/streaming_job.py `
    --bootstrap-servers localhost:9092 `
    --output-root data/feature_table `
    --checkpoint-root infra/checkpoints `
    --model-path models/linear_regression `
    --trigger-interval "10 seconds"
```

### Step 3.7: Verify Resumed Progress
Watch the console output. Notice that the batch numbering does **NOT** reset to 0:
```text
2026-10-20 22:03:10 [INFO] [UrbanixInference] [Batch 2] Ingested 5 windowed traffic rows for scoring.
2026-10-20 22:03:14 [INFO] [UrbanixInference] [Batch 2] Successfully appended 5 predicted traffic rows to 'data/feature_table/traffic'
```
The query resumed directly from `Batch 2`!

---

## 📊 4. Measured Experimental Recovery Results

To provide fully reproducible, machine-verified evidence for Day 6 audit, the recovery harness in [`services/streaming/test_checkpoint_recovery.py`](file:///c:/Users/Tokachichu%20SriCharan/OneDrive/Documents/GitHub/urbanix/services/streaming/test_checkpoint_recovery.py) executed this exact protocol in an isolated test environment.

### Observed Process-Level Execution Log:
```text
==============================================================================
URBANIX DAY 6: GENUINE PROCESS-CRASH & CHECKPOINT RECOVERY DEMONSTRATION
==============================================================================
[*] Step 1: Staging initial streaming micro-batch 0 (5 zones for 07:00:00 UTC)...
[+] Staged micro-batch 0 at infra\checkpoints\test_process_recovery_input\batch_0.json

[*] Step 2: Spawning Child OS Process 1 running live Structured Streaming inference...
[+] Child Process 1 active with PID=28320
[*] Waiting for Child Process 1 to commit Batch 0 to checkpoint...
[+] Batch 0 committed to disk by Child Process 1!

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
[!] INJECTING HARD OS-LEVEL PROCESS CRASH: Sending proc.kill() to PID=28320...
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
[+] Process 28320 forcefully terminated. Exit code: 1 (Non-zero confirmed)

[*] Step 4: Inspecting checkpoint persistence on disk after hard kill...
[+] Checkpoint verified: 2 commit log(s), 2 offset log(s) intact on disk.

[*] Step 5: Staging micro-batch 1 while streaming process is dead...
[+] Staged micro-batch 1 at infra\checkpoints\test_process_recovery_input\batch_1.json

[*] Step 6: Spawning Child OS Process 2: Restarting query from EXACT SAME checkpoint...
[+] Child Process 2 active with PID=28252
[+] Child Process 2 completed cleanly with exit code 0

[*] Step 7: Performing rigorous data audit on post-recovery Parquet store...
[+] Total Rows Persisted in Sink: 10
[+] Distinct (zone_id, window_start) Keys: 10 (Expected: 10)
[+] Duplicate Key Count: 0
    Delivery Semantics Analysis: Because the hard kill occurred after commits/0 was written,
    the recovery process correctly resumed at Batch 1, yielding exactly 10 rows and 0 duplicates.
    If killed prior to commits/0, at-least-once re-execution would write duplicate part-files
    which are safely deduplicated downstream by (zone_id, window_start).
[+] Missing Expected Keys Count: 0
[+] Invalid / Out-of-Bounds Predictions: 0

[+] Sample of Recovered Predictions Table:
+-------------------+-------+--------------+--------------------+--------------------+
|window_start       |zone_id|avg_speed_kmph|predicted_speed_kmph|prediction_model    |
+-------------------+-------+--------------+--------------------+--------------------+
|2012-03-01 07:00:00|zone_1 |95.5          |96.8                |LinearRegression_OLS|
|2012-03-01 07:00:00|zone_2 |88.2          |91.66               |LinearRegression_OLS|
|2012-03-01 07:00:00|zone_3 |102.1         |101.02              |LinearRegression_OLS|
|2012-03-01 07:00:00|zone_4 |75.8          |83.04               |LinearRegression_OLS|
|2012-03-01 07:00:00|zone_5 |108.4         |104.81              |LinearRegression_OLS|
|2012-03-01 07:05:00|zone_1 |93.2          |95.28               |LinearRegression_OLS|
|2012-03-01 07:05:00|zone_2 |86.4          |90.66               |LinearRegression_OLS|
|2012-03-01 07:05:00|zone_3 |101.5         |100.66              |LinearRegression_OLS|
|2012-03-01 07:05:00|zone_4 |73.1          |81.43               |LinearRegression_OLS|
|2012-03-01 07:05:00|zone_5 |106.8         |103.86              |LinearRegression_OLS|
+-------------------+-------+--------------+--------------------+--------------------+

==============================================================================
[+] GENUINE PROCESS-LEVEL CRASH & CHECKPOINT RECOVERY: ALL CHECKS PASSED!
==============================================================================
```

### Key Verification Findings:
1. **Genuine Process Crash Survived:** Process 28320 was forcefully killed via `proc.kill()` (exit code 1); commit logs and offset records on disk remained completely intact.
2. **Stable Record Identity Verification:** Exactly 10 distinct `(zone_id, window_start)` composite keys exist. Zero missing expected records.
3. **Duplicate Key Audit:** Exactly 0 duplicate records observed for this checkpoint boundary. Delivery guarantee confirmed as at-least-once.
4. **Zero Nulls / NaNs:** 100% of recovered records contain non-null, physically bounded velocity predictions ($73.1\text{--}108.4\text{ km/h}$).
5. **Continuous State Progression:** Child Process 2 advanced cleanly from `Batch 0` to `Batch 1` after recovery, utilizing the persisted checkpoint offsets.

---

## 🛠️ 5. Automated Verification Commands

To independently rerun the automated verification suite:

```powershell
# 1. Run full Day 6 unit & integration test suite (10 tests):
.\services\streaming\venv\Scripts\pytest services/streaming/tests/test_streaming_inference.py -v

# 2. Run standalone process-level crash-recovery demonstration:
.\services\streaming\venv\Scripts\python services/streaming/test_checkpoint_recovery.py

# 3. Run independent inference verifier:
.\services\streaming\venv\Scripts\python services/streaming/verify_inference.py
```
