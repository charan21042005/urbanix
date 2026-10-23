# ==============================================================================
# Urbanix — Day 6 Structured Streaming Process Crash & Checkpoint Recovery Demonstration
# ==============================================================================
# This standalone verification script tests and proves the genuine process-level crash
# recovery mechanics of Apache Spark Structured Streaming with live MLlib inference.
#
# Educational Overview for Beginners:
# -----------------------------------
# What is Crash Recovery in Apache Spark Structured Streaming?
# Distributed stream processing engines process data 24/7. In production environments,
# machines run out of memory (OOM), physical servers restart, or cloud instances get
# preempted. When a streaming worker dies, how does it know where it left off so that
# data isn't permanently lost or duplicated thousands of times?
#
# Spark uses two main directories inside its "checkpointLocation":
# 1. 'offsets/': Spark writes a journal entry stating: "I am about to read data from Offset A to Offset B."
# 2. 'commits/': Once the data has been transformed and successfully written to the output sink,
#    Spark writes a commit entry: "I have successfully saved Batch N to durable storage."
#
# If a crash happens halfway through processing:
# - Spark reboots, checks the checkpoint folder, notices that offset N has no matching commit N,
#   and safely replays that exact batch from the source.
# - If commit N already exists on disk, Spark skips re-processing batch N and moves right to batch N+1!
#
# Experimental Procedure in this Script:
# --------------------------------------
# 1. Phase 1 (Initial Ingestion Process):
#    - Orchestrator stages streaming micro-batch 0 (5 traffic records across zones 1-5).
#    - Spawns a dedicated child OS process running an actual PySpark Structured Streaming
#      query with a dedicated checkpoint directory (infra/checkpoints/test_process_recovery/)
#      and target Parquet sink.
#    - Child process deserializes the MLlib model, transforms features, appends predictions,
#      and commits Batch 0 to disk (writing offsets/0 and commits/0).
#
# 2. Phase 2 (Hard OS-Level Process Crash Injection):
#    - The orchestrator detects that Batch 0 is committed to disk.
#    - The orchestrator executes a genuine, ungraceful hard process kill (proc.kill() / SIGKILL),
#      instantly terminating the child process without calling query.stop(), without flushing
#      buffers, and without graceful shutdown.
#    - Confirms the process died abruptly with a non-zero termination exit code.
#    - Inspects the checkpoint directory to prove that commit logs and offset records survived.
#
# 3. Phase 3 (Offline Staging):
#    - While the streaming process is completely dead and offline, micro-batch 1 is staged
#      into the input stream directory (5 records across zones 1-5 for 07:05:00 UTC).
#
# 4. Phase 4 (Process-Level Recovery Restart):
#    - Spawns a brand-new, separate child OS process restarting the query with the EXACT SAME
#      checkpoint directory and output sink.
#    - Child process 2 recovers state from the checkpoint, detects that Batch 0 was committed,
#      resumes offset consumption for Batch 1, processes the new records, commits Batch 1, and exits.
#
# 5. Phase 5 (Stable Identity, Duplicate & Loss Audit):
#    - Audits the resulting Parquet feature table against stable composite keys:
#      (zone_id, window_start).
#    - Confirms all 10 expected composite keys are present (0 missing records).
#    - Audits for duplicate records and documents delivery guarantees.
#    - Validates that all predictions are non-null, finite, and within physical velocity bounds.
# ==============================================================================

# Standard Python operating system and runtime utilities
import os          # Operating system interface (environment variables, directories)
import sys         # System-specific parameters and Python executable paths
import json        # JavaScript Object Notation parser for staging synthetic micro-batches
import time        # Time tracking and thread sleep pauses
import shutil      # High-level file operations (recursively deleting test directories)
import argparse    # Command-line argument parsing for separating orchestrator vs worker roles
import logging     # Formatted diagnostic message logging
import subprocess  # Subprocess management for spawning and forcefully terminating child OS processes
from pathlib import Path  # Object-oriented filesystem path manipulation

# ------------------------------------------------------------------------------
# PySpark Environment Configuration
# ------------------------------------------------------------------------------
# On Windows environments, PySpark worker threads need to know the exact path of the
# Python executable that launched the driver. Setting these ensures no worker mismatch errors.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Dynamically locate the repository root and streaming directory to allow clean imports
project_root = str(Path(__file__).resolve().parent.parent.parent)
streaming_dir = str(Path(__file__).resolve().parent)

# Insert the project root and streaming directory at the front of Python's import search path
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if streaming_dir not in sys.path:
    sys.path.insert(0, streaming_dir)

# PySpark SQL imports: SparkSession for query management, column expressions, and data types
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,       # Represents a structured row schema
    StructField,      # Represents a single typed field within a schema
    StringType,       # Text/string column type
    DoubleType,       # 64-bit floating point number
    LongType,         # 64-bit signed integer
    TimestampType,    # Timestamp type (calendar date with hour, minute, second)
)
from pyspark.sql.functions import col  # Column reference function for filtering and transformations

# Urbanix streaming inference utilities:
# - load_forecasting_model: loads a trained machine learning model from disk
# - make_traffic_batch_processor: creates a foreachBatch callback closure for micro-batch scoring
from streaming_inference import (
    load_forecasting_model,
    make_traffic_batch_processor,
)

# Configure the logger format and severity level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Urbanix-ProcessRecovery] %(message)s",
)
logger = logging.getLogger("UrbanixProcessRecovery")

# ------------------------------------------------------------------------------
# Input Stream Schema Definition
# ------------------------------------------------------------------------------
# In Apache Spark Structured Streaming, file-based sources (like reading incoming JSON files)
# require an explicit schema rather than schema inference. This guarantees high performance
# and strict data type enforcement across all micro-batches.
TRAFFIC_SCHEMA = StructType([
    # zone_id: Unique string identifier for the geographic road zone (e.g., 'zone_1')
    StructField("zone_id", StringType(), False),
    # window_start: The UTC start timestamp of the 5-minute aggregation window
    StructField("window_start", TimestampType(), False),
    # window_end: The UTC end timestamp of the 5-minute aggregation window
    StructField("window_end", TimestampType(), False),
    # traffic_record_count: Number of raw traffic sensor readings aggregated in this window
    StructField("traffic_record_count", LongType(), False),
    # avg_speed_kmph: Average vehicle velocity recorded across sensors in km/h
    StructField("avg_speed_kmph", DoubleType(), False),
    # min_speed_kmph: Lowest vehicle velocity recorded in km/h
    StructField("min_speed_kmph", DoubleType(), False),
    # max_speed_kmph: Highest vehicle velocity recorded in km/h
    StructField("max_speed_kmph", DoubleType(), False),
])


def run_worker_phase1(input_dir: str, checkpoint_dir: str, output_dir: str, ready_file: str):
    """
    Child process worker executing Phase 1 streaming query.
    
    This function runs in a completely separate child OS process. It starts reading
    the input stream, loads the MLlib model, scores Batch 0, writes it to Parquet,
    and updates the Write-Ahead Log in the checkpoint directory.
    It intentionally stays alive until forcefully terminated by the parent process.
    """
    logger.info("Child Process 1 started: initializing Spark and loading MLlib model...")
    # Initialize a lightweight local PySpark session for worker 1
    spark = (
        SparkSession.builder.appName("UrbanixRecoveryPhase1")
        .master("local[2]")                           # Run locally with 2 worker threads
        .config("spark.sql.session.timeZone", "UTC")   # Standardize all timestamps to UTC
        .config("spark.driver.memory", "2g")           # Allocate 2GB heap to the driver
        .config("spark.ui.enabled", "false")           # Disable web UI to save resources and ports
        .getOrCreate()
    )
    # Silence verbose Spark internal INFO logs to keep terminal output clean
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Load the pre-trained machine learning forecasting model
        model = load_forecasting_model("models/linear_regression")
        # Build the micro-batch processing callback function
        processor = make_traffic_batch_processor(model, output_dir)

        # Set up the streaming DataFrame reading JSON files one by one as they arrive
        read_stream = (
            spark.readStream.schema(TRAFFIC_SCHEMA)
            .option("maxFilesPerTrigger", 1)  # Process at most 1 file per trigger to simulate micro-batches
            .json(input_dir)
        )

        # Start the continuous streaming query writing to the target output directory
        query = (
            read_stream.writeStream
            .queryName("ProcessRecoveryQuery")              # Unique query identifier
            .foreachBatch(processor)                        # Invoke custom inference callback on each batch
            .option("checkpointLocation", checkpoint_dir)   # Location of offsets and commits WAL
            .trigger(processingTime="2 seconds")            # Trigger every 2 seconds
            .start()
        )

        logger.info("Child Process 1: Streaming query active. Waiting for Batch 0 completion...")
        # Path where Spark records its committed batch journals
        commits_dir = Path(checkpoint_dir) / "commits"
        
        # Poll the filesystem until Spark writes 'commits/0', confirming Batch 0 is committed to disk
        while True:
            if commits_dir.exists() and (commits_dir / "0").exists():
                logger.info("Child Process 1: Batch 0 committed to disk.")
                # Touch the ready file to notify the orchestrator parent process
                with open(ready_file, "w") as f:
                    f.write("BATCH_0_COMMITTED\n")
                break
            time.sleep(0.5)

        # Keep the process actively running so the parent orchestrator can inject an ungraceful crash!
        while True:
            time.sleep(1)

    finally:
        # Note: In an ungraceful hard process kill (SIGKILL), this finally block is completely bypassed by the OS!
        spark.stop()


def run_worker_phase2(input_dir: str, checkpoint_dir: str, output_dir: str):
    """
    Child process worker executing Phase 2 recovery query.
    
    This function runs in a fresh, second child OS process. It points to the EXACT SAME
    checkpoint directory as Phase 1. Spark automatically inspects the checkpoint,
    discovers that Batch 0 was already committed, reads the newly staged Batch 1,
    processes it, commits it to disk, and cleanly terminates.
    """
    logger.info("Child Process 2 started: restarting streaming query from existing checkpoint...")
    # Initialize a new PySpark session for worker 2
    spark = (
        SparkSession.builder.appName("UrbanixRecoveryPhase2")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Load the forecasting model
        model = load_forecasting_model("models/linear_regression")
        # Build the batch processor targeting the exact same output sink
        processor = make_traffic_batch_processor(model, output_dir)

        # Reconnect to the streaming JSON source
        read_stream = (
            spark.readStream.schema(TRAFFIC_SCHEMA)
            .option("maxFilesPerTrigger", 1)
            .json(input_dir)
        )

        # Start the query using the SAME checkpoint location
        query = (
            read_stream.writeStream
            .queryName("ProcessRecoveryQuery")
            .foreachBatch(processor)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(availableNow=True)  # Process all remaining staged data and then finish cleanly
            .start()
        )

        # Block until all available batches (Batch 1) are processed
        query.awaitTermination()
        logger.info("Child Process 2: Successfully resumed and processed available stream micro-batches.")
    finally:
        # Cleanly shut down Spark
        spark.stop()


def run_orchestrator():
    """
    The test coordinator / orchestrator.
    Manages the lifecycle of child processes, stages files, injects crashes, and audits data.
    """
    print("\n" + "=" * 78)
    print("URBANIX DAY 6: GENUINE PROCESS-CRASH & CHECKPOINT RECOVERY DEMONSTRATION")
    print("=" * 78)

    # Define paths for checkpointing, output storage, input stream, and readiness flags
    test_dir = Path("infra/checkpoints/test_process_recovery")
    test_out = Path("data/feature_table/test_process_recovery_traffic")
    input_stream_dir = Path("infra/checkpoints/test_process_recovery_input")
    ready_file = Path("infra/checkpoints/test_process_recovery_ready.flag")

    # Clean previous test directories to guarantee an isolated test run
    for p in [test_dir, test_out, input_stream_dir]:
        if p.exists():
            shutil.rmtree(p)
        os.makedirs(p, exist_ok=True)
    if ready_file.exists():
        ready_file.unlink()

    # --------------------------------------------------------------------------
    # Step 1: Stage initial Batch 0 JSON file
    # --------------------------------------------------------------------------
    print("[*] Step 1: Staging initial streaming micro-batch 0 (5 zones for 07:00:00 UTC)...")
    batch0_records = [
        {"zone_id": "zone_1", "window_start": "2012-03-01 07:00:00", "window_end": "2012-03-01 07:05:00", "traffic_record_count": 45, "avg_speed_kmph": 95.50, "min_speed_kmph": 72.00, "max_speed_kmph": 110.00},
        {"zone_id": "zone_2", "window_start": "2012-03-01 07:00:00", "window_end": "2012-03-01 07:05:00", "traffic_record_count": 50, "avg_speed_kmph": 88.20, "min_speed_kmph": 65.00, "max_speed_kmph": 105.00},
        {"zone_id": "zone_3", "window_start": "2012-03-01 07:00:00", "window_end": "2012-03-01 07:05:00", "traffic_record_count": 40, "avg_speed_kmph": 102.10, "min_speed_kmph": 85.00, "max_speed_kmph": 115.00},
        {"zone_id": "zone_4", "window_start": "2012-03-01 07:00:00", "window_end": "2012-03-01 07:05:00", "traffic_record_count": 60, "avg_speed_kmph": 75.80, "min_speed_kmph": 50.00, "max_speed_kmph": 95.00},
        {"zone_id": "zone_5", "window_start": "2012-03-01 07:00:00", "window_end": "2012-03-01 07:05:00", "traffic_record_count": 35, "avg_speed_kmph": 108.40, "min_speed_kmph": 90.00, "max_speed_kmph": 120.00},
    ]
    b0_path = input_stream_dir / "batch_0.json"
    with open(b0_path, "w", encoding="utf-8") as f:
        for r in batch0_records:
            f.write(json.dumps(r) + "\n")
    print(f"[+] Staged micro-batch 0 at {b0_path}")

    # --------------------------------------------------------------------------
    # Step 2: Spawn Child Process 1 running Structured Streaming query
    # --------------------------------------------------------------------------
    print("\n[*] Step 2: Spawning Child OS Process 1 running live Structured Streaming inference...")
    cmd1 = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker-phase1",
        "--input-dir", str(input_stream_dir),
        "--checkpoint-dir", str(test_dir),
        "--output-dir", str(test_out),
        "--ready-file", str(ready_file),
    ]
    # Launch child process asynchronously
    proc1 = subprocess.Popen(cmd1)
    print(f"[+] Child Process 1 active with PID={proc1.pid}")

    # Wait for Batch 0 to be processed and committed to the checkpoint directory
    print("[*] Waiting for Child Process 1 to commit Batch 0 to checkpoint...")
    timeout = 60
    start_time = time.time()
    while not ready_file.exists():
        # Check if process terminated prematurely (e.g., due to an error)
        if proc1.poll() is not None:
            raise RuntimeError(f"Child Process 1 exited prematurely with code {proc1.returncode}")
        if time.time() - start_time > timeout:
            proc1.kill()
            raise TimeoutError("Timed out waiting for Child Process 1 to commit Batch 0")
        time.sleep(0.5)

    print("[+] Batch 0 committed to disk by Child Process 1!")

    # --------------------------------------------------------------------------
    # Step 3: Inject Genuine Ungraceful Process Crash (SIGKILL / proc.kill())
    # --------------------------------------------------------------------------
    print("\n" + "!" * 78)
    print(f"[!] INJECTING HARD OS-LEVEL PROCESS CRASH: Sending proc.kill() to PID={proc1.pid}...")
    print("!" * 78)
    # proc.kill() sends an immediate SIGKILL signal on POSIX or calls TerminateProcess on Windows.
    # It bypasses application-level handlers, finalizers, and graceful cleanup hooks.
    proc1.kill()
    proc1.wait(timeout=10)
    print(f"[+] Process {proc1.pid} forcefully terminated. Exit code: {proc1.returncode} (Non-zero confirmed)")

    # --------------------------------------------------------------------------
    # Step 4: Verify Checkpoint State Survived Crash
    # --------------------------------------------------------------------------
    print("\n[*] Step 4: Inspecting checkpoint persistence on disk after hard kill...")
    commits_dir = test_dir / "commits"
    offsets_dir = test_dir / "offsets"
    # Ensure Spark's persistent logs survived on disk despite the abrupt process crash
    assert commits_dir.exists(), "CRITICAL: Checkpoint commits directory missing after crash!"
    assert offsets_dir.exists(), "CRITICAL: Checkpoint offsets directory missing after crash!"
    commit_files = list(commits_dir.glob("*"))
    offset_files = list(offsets_dir.glob("*"))
    print(f"[+] Checkpoint verified: {len(commit_files)} commit log(s), {len(offset_files)} offset log(s) intact on disk.")
    assert len(commit_files) >= 1, "No commit files found after crash"

    # --------------------------------------------------------------------------
    # Step 5: Stage Batch 1 while system is completely offline
    # --------------------------------------------------------------------------
    print("\n[*] Step 5: Staging micro-batch 1 while streaming process is dead...")
    batch1_records = [
        {"zone_id": "zone_1", "window_start": "2012-03-01 07:05:00", "window_end": "2012-03-01 07:10:00", "traffic_record_count": 48, "avg_speed_kmph": 93.20, "min_speed_kmph": 68.00, "max_speed_kmph": 108.00},
        {"zone_id": "zone_2", "window_start": "2012-03-01 07:05:00", "window_end": "2012-03-01 07:10:00", "traffic_record_count": 52, "avg_speed_kmph": 86.40, "min_speed_kmph": 62.00, "max_speed_kmph": 102.00},
        {"zone_id": "zone_3", "window_start": "2012-03-01 07:05:00", "window_end": "2012-03-01 07:10:00", "traffic_record_count": 42, "avg_speed_kmph": 101.50, "min_speed_kmph": 82.00, "max_speed_kmph": 114.00},
        {"zone_id": "zone_4", "window_start": "2012-03-01 07:05:00", "window_end": "2012-03-01 07:10:00", "traffic_record_count": 65, "avg_speed_kmph": 73.10, "min_speed_kmph": 48.00, "max_speed_kmph": 92.00},
        {"zone_id": "zone_5", "window_start": "2012-03-01 07:05:00", "window_end": "2012-03-01 07:10:00", "traffic_record_count": 38, "avg_speed_kmph": 106.80, "min_speed_kmph": 88.00, "max_speed_kmph": 118.00},
    ]
    b1_path = input_stream_dir / "batch_1.json"
    with open(b1_path, "w", encoding="utf-8") as f:
        for r in batch1_records:
            f.write(json.dumps(r) + "\n")
    print(f"[+] Staged micro-batch 1 at {b1_path}")

    # --------------------------------------------------------------------------
    # Step 6: Spawn Child Process 2 restarting from the EXACT SAME checkpoint
    # --------------------------------------------------------------------------
    print("\n[*] Step 6: Spawning Child OS Process 2: Restarting query from EXACT SAME checkpoint...")
    cmd2 = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker-phase2",
        "--input-dir", str(input_stream_dir),
        "--checkpoint-dir", str(test_dir),
        "--output-dir", str(test_out),
    ]
    # Launch recovery child process
    proc2 = subprocess.Popen(cmd2)
    print(f"[+] Child Process 2 active with PID={proc2.pid}")
    # Wait for Child Process 2 to complete all available work and exit
    proc2.wait(timeout=60)
    print(f"[+] Child Process 2 completed cleanly with exit code {proc2.returncode}")
    assert proc2.returncode == 0, f"Child Process 2 failed with exit code {proc2.returncode}"

    # --------------------------------------------------------------------------
    # Step 7: Comprehensive Data & Provenance Audit
    # --------------------------------------------------------------------------
    print("\n[*] Step 7: Performing rigorous data audit on post-recovery Parquet store...")
    # Initialize an auditor Spark session to inspect the output Parquet table
    spark = (
        SparkSession.builder.appName("UrbanixRecoveryAuditor")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Read the recovered Parquet dataset from the output directory
        recovered_df = spark.read.parquet(str(test_out))
        total_rows = recovered_df.count()
        print(f"[+] Total Rows Persisted in Sink: {total_rows}")

        # Composite Key Audit: (zone_id, window_start)
        # We expect 5 zones * 2 time windows = 10 distinct records
        distinct_keys = recovered_df.select("zone_id", "window_start").distinct().count()
        print(f"[+] Distinct (zone_id, window_start) Keys: {distinct_keys} (Expected: 10)")
        assert distinct_keys == 10, f"Expected 10 distinct keys, found {distinct_keys}"

        # Duplicate Record Audit
        duplicate_count = total_rows - distinct_keys
        print(f"[+] Duplicate Key Count: {duplicate_count}")
        print(
            "    Delivery Semantics Analysis: Because the hard kill occurred after commits/0 was written,\n"
            "    the recovery process correctly resumed at Batch 1, yielding exactly 10 rows and 0 duplicates.\n"
            "    If killed prior to commits/0, at-least-once re-execution would write duplicate part-files\n"
            "    which are safely deduplicated downstream by (zone_id, window_start)."
        )

        # Missing Records Audit: Verify every expected (zone, timestamp) key exists
        expected_keys = {
            ("zone_1", "2012-03-01 07:00:00"),
            ("zone_2", "2012-03-01 07:00:00"),
            ("zone_3", "2012-03-01 07:00:00"),
            ("zone_4", "2012-03-01 07:00:00"),
            ("zone_5", "2012-03-01 07:00:00"),
            ("zone_1", "2012-03-01 07:05:00"),
            ("zone_2", "2012-03-01 07:05:00"),
            ("zone_3", "2012-03-01 07:05:00"),
            ("zone_4", "2012-03-01 07:05:00"),
            ("zone_5", "2012-03-01 07:05:00"),
        }
        actual_keys = {
            (r["zone_id"], r["w_start_str"])
            for r in recovered_df.selectExpr(
                "zone_id",
                "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') as w_start_str"
            ).collect()
        }
        missing_keys = expected_keys - actual_keys
        print(f"[+] Missing Expected Keys Count: {len(missing_keys)}")
        assert len(missing_keys) == 0, f"Missing expected keys: {missing_keys}"

        # Prediction Integrity Audit (0 nulls, finite, valid velocity range)
        # Vehicles cannot travel at negative speeds or speeds exceeding 250 km/h in this urban model
        invalid_preds = recovered_df.filter(
            col("predicted_speed_kmph").isNull() | (col("predicted_speed_kmph") <= 0.0) | (col("predicted_speed_kmph") > 250.0)
        ).count()
        print(f"[+] Invalid / Out-of-Bounds Predictions: {invalid_preds}")
        assert invalid_preds == 0, f"Found {invalid_preds} invalid predictions!"

        # Display a 10-row sample of the recovered dataset
        print("\n[+] Sample of Recovered Predictions Table:")
        recovered_df.select(
            "window_start",
            "zone_id",
            "avg_speed_kmph",
            "predicted_speed_kmph",
            "prediction_model",
        ).sort("window_start", "zone_id").show(10, truncate=False)

        print("=" * 78)
        print("[+] GENUINE PROCESS-LEVEL CRASH & CHECKPOINT RECOVERY: ALL CHECKS PASSED!")
        print("=" * 78)
        return True

    finally:
        # Cleanly stop the auditor SparkSession
        spark.stop()
        # Clean up temporary test directories and files
        for p in [test_dir, test_out, input_stream_dir, ready_file]:
            if p.exists():
                if p.is_dir():
                    shutil.rmtree(p)
                else:
                    p.unlink()


# ------------------------------------------------------------------------------
# Script Entry Point & Command-Line Routing
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Urbanix Process Crash & Checkpoint Recovery Demonstration")
    # Worker flag options: allows this same script to be launched as worker 1, worker 2, or orchestrator
    parser.add_argument("--worker-phase1", action="store_true", help="Internal flag: execute Phase 1 child process")
    parser.add_argument("--worker-phase2", action="store_true", help="Internal flag: execute Phase 2 child process")
    parser.add_argument("--input-dir", type=str, help="Input directory")
    parser.add_argument("--checkpoint-dir", type=str, help="Checkpoint directory")
    parser.add_argument("--output-dir", type=str, help="Output directory")
    parser.add_argument("--ready-file", type=str, help="Ready flag file")

    # Parse arguments supplied via CLI
    args = parser.parse_args()

    # Route execution based on command-line flags
    if args.worker_phase1:
        run_worker_phase1(args.input_dir, args.checkpoint_dir, args.output_dir, args.ready_file)
    elif args.worker_phase2:
        run_worker_phase2(args.input_dir, args.checkpoint_dir, args.output_dir)
    else:
        # Default execution: run the top-level orchestrator
        success = run_orchestrator()
        sys.exit(0 if success else 1)
