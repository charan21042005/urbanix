#!/usr/bin/env python3
"""
Urbanix — Concurrent Producer Runner (Day 2)
Launches all three telemetry simulation producers (Traffic, AQI, Weather) concurrently,
streams their output, and shuts them down cleanly on Ctrl+C.
"""

# --- Standard Library Imports ---
import os          # Operating system file path operations
import sys         # System parameters, interpreter path, and standard error stream
import time        # Sleep delays to prevent CPU thrashing in monitoring loops
import subprocess  # Spawning and managing external OS processes
import signal      # Intercepting Ctrl+C signals (SIGINT) for clean child process termination
import argparse    # Command-line interface parsing

# Determine the absolute directory path containing this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Identify the exact Python executable running this master process (ensures virtualenv parity)
PYTHON_EXE = sys.executable

# Configuration manifest defining the three telemetry simulation producer child processes
PRODUCERS = [
    {"name": "Traffic Producer", "script": "traffic_producer.py", "interval": "0.1"},
    {"name": "AQI Producer",     "script": "aqi_producer.py",     "interval": "0.2"},
    {"name": "Weather Producer", "script": "weather_producer.py", "interval": "0.2"},
]


def main():
    """
    Main supervisor entry point.
    Spawns all 3 producers concurrently as background child processes,
    multiplexes their stdout output to the console, and terminates all cleanly on exit.
    """
    # Ensure standard output uses UTF-8 encoding on Windows consoles
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    # Configure command-line argument parser
    parser = argparse.ArgumentParser(
        description="Launch all 3 Urbanix simulation producers (Traffic, AQI, Weather) concurrently."
    )
    parser.add_argument(
        "max_events",
        nargs="?",
        type=int,
        default=None,
        help="Optional maximum events limit PER PRODUCER (e.g. 50 runs 50 events each, 150 total)",
    )
    args = parser.parse_args()

    # Validate that max_events, if provided, is strictly positive
    if args.max_events is not None and args.max_events <= 0:
        parser.error("max_events must be a positive integer")

    # Print startup banner and configuration details
    print("=" * 70)
    print("[RUN-ALL] Launching Urbanix Concurrent Telemetry Simulation (Day 2)")
    print(f" - Python Runtime: {PYTHON_EXE}")
    print(f" - Mode:           3 Concurrent Producers (Traffic, AQI, Weather)")
    if args.max_events:
        print(f" - Max Events:     {args.max_events} PER PRODUCER (Total: {args.max_events * len(PRODUCERS)})")
    print(" - Press Ctrl+C at any time to shut down all producers cleanly.")
    print("=" * 70)

    # List to hold tuples of (producer_name, subprocess.Popen_instance)
    processes = []

    # Launch each producer script as an independent operating system process
    for p in PRODUCERS:
        # Build command-line invocation list: [python, script_path, --interval, value]
        cmd = [
            PYTHON_EXE,
            os.path.join(BASE_DIR, p["script"]),
            "--interval", p["interval"],
        ]
        # Append --max-events flag if configured
        if args.max_events:
            cmd.extend(["--max-events", str(args.max_events)])

        # Spawn asynchronous non-blocking child process
        proc = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,     # Capture standard output stream
            stderr=subprocess.STDOUT,   # Merge standard error into standard output stream
            text=True,                  # Decode byte stream into human-readable string text
            bufsize=1,                  # Line-buffered output for instant real-time streaming
        )
        processes.append((p["name"], proc))
        print(f"[+] Started {p['name']} (PID: {proc.pid})")

    # State flag controlling the continuous monitoring loop
    running = True

    # Signal handler: invoked when user presses Ctrl+C
    def handle_sigint(sig, frame):
        nonlocal running
        print("\n[!] Ctrl+C detected. Shutting down all producers gracefully...")
        running = False
        # Broadcast SIGINT signal to each active child process
        for name, proc in processes:
            if proc.poll() is None:  # Process is still running
                proc.send_signal(signal.SIGINT)

    # Register OS signal handler for Ctrl+C
    signal.signal(signal.SIGINT, handle_sigint)

    # Monitor child process outputs and lifecycle
    try:
        while running:
            all_done = True
            for name, proc in processes:
                # Read line from child process stdout stream
                line = proc.stdout.readline()
                if line:
                    # Print formatted line with producer prefix
                    print(f"[{name}] {line.strip()}")
                # Check if process is still actively running (poll() returns None if active)
                if proc.poll() is None:
                    all_done = False

            # If all child processes have naturally finished their work, exit loop
            if all_done:
                print("\n[OK] All simulation producers have completed their streams.")
                break
            # Small sleep pause to yield CPU time slices
            time.sleep(0.02)
    except KeyboardInterrupt:
        # Catch KeyboardInterrupt if signal handler didn't catch it first
        handle_sigint(None, None)
    finally:
        failed = False
        # Cleanly terminate and reap any remaining child processes
        for name, proc in processes:
            if proc.poll() is None:
                proc.terminate()
                try:
                    # Wait up to 3 seconds for graceful shutdown
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    # Force kill if process does not exit in time
                    proc.kill()
            # Flag any non-zero exit codes as failure
            if proc.returncode not in (0, None):
                failed = True
                print(f"[-] {name} exited with non-zero code: {proc.returncode}", file=sys.stderr)
        print("[OK] All producer processes terminated cleanly.")
        # Exit with error status if any producer crashed
        if failed:
            sys.exit(1)


# Standard Python boilerplate to execute main() when run directly from command line
if __name__ == "__main__":
    main()
