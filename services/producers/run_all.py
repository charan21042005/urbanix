#!/usr/bin/env python3
"""
Urbanix — Concurrent Producer Runner (Day 2)
Launches all three telemetry simulation producers (Traffic, AQI, Weather) concurrently,
streams their output, and shuts them down cleanly on Ctrl+C.
"""

import os
import sys
import time
import subprocess
import signal

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_EXE = sys.executable

PRODUCERS = [
    {"name": "Traffic Producer", "script": "traffic_producer.py", "interval": "0.1"},
    {"name": "AQI Producer",     "script": "aqi_producer.py",     "interval": "0.2"},
    {"name": "Weather Producer", "script": "weather_producer.py", "interval": "0.2"},
]


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    max_events = None
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        max_events = sys.argv[1]

    print("=" * 70)
    print("[RUN-ALL] Launching Urbanix Concurrent Telemetry Simulation (Day 2)")
    print(f" - Python Runtime: {PYTHON_EXE}")
    print(f" - Mode:           3 Concurrent Producers (Traffic, AQI, Weather)")
    if max_events:
        print(f" - Max Events:     {max_events} per producer")
    print(" - Press Ctrl+C at any time to shut down all producers cleanly.")
    print("=" * 70)

    processes = []

    for p in PRODUCERS:
        cmd = [
            PYTHON_EXE,
            os.path.join(BASE_DIR, p["script"]),
            "--interval", p["interval"],
        ]
        if max_events:
            cmd.extend(["--max-events", max_events])

        proc = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        processes.append((p["name"], proc))
        print(f"[+] Started {p['name']} (PID: {proc.pid})")

    running = True

    def handle_sigint(sig, frame):
        nonlocal running
        print("\n[!] Ctrl+C detected. Shutting down all producers gracefully...")
        running = False
        for name, proc in processes:
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)

    signal.signal(signal.SIGINT, handle_sigint)

    # Monitor outputs and process life
    try:
        while running:
            all_done = True
            for name, proc in processes:
                # Read any available line without blocking
                line = proc.stdout.readline()
                if line:
                    print(f"[{name}] {line.strip()}")
                if proc.poll() is None:
                    all_done = False

            if all_done:
                print("\n[OK] All simulation producers have completed their streams.")
                break
            time.sleep(0.02)
    except KeyboardInterrupt:
        handle_sigint(None, None)
    finally:
        failed = False
        for name, proc in processes:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
            if proc.returncode not in (0, None):
                failed = True
                print(f"[-] {name} exited with non-zero code: {proc.returncode}")
        print("[OK] All producer processes terminated cleanly.")
        if failed:
            sys.exit(1)


if __name__ == "__main__":
    main()
