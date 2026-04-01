"""
start_airflow_local.py
───────────────────────
Start the Airflow webserver and scheduler for local development.

Run from the project root after running setup_airflow_local.py:
    python scripts/start_airflow_local.py

Starts two processes:
  - webserver  on http://localhost:8080
  - scheduler  (triggers DAG runs per schedule or manual trigger)

Stop with Ctrl+C — both processes are shut down cleanly.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_HOME = PROJECT_ROOT / "airflow" / "home"

# ── Environment ───────────────────────────────────────────────────────────────
# SQLite DSN must use 4 slashes + forward slashes on Windows
_db_uri = "sqlite:////" + (AIRFLOW_HOME / "airflow.db").as_posix()

env = {
    **os.environ,
    "AIRFLOW_HOME":                                str(AIRFLOW_HOME),
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN":         _db_uri,
    "AIRFLOW__CORE__DAGS_FOLDER":                  str(PROJECT_ROOT / "airflow" / "dags"),
    "AIRFLOW__CORE__LOAD_EXAMPLES":                "False",
    "AIRFLOW__CORE__EXECUTOR":                     "SequentialExecutor",
    "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION":  "True",
    "AIRFLOW__LOGGING__LOGGING_LEVEL":             "WARNING",
    "NEXCART_ROOT":                                str(PROJECT_ROOT),
    "NEXCART_PYTHON":                              sys.executable,
    "PYTHONPATH":                                  str(PROJECT_ROOT),
}

procs: list[subprocess.Popen] = []


def _shutdown(signum, frame):
    print("\n\nShutting down Airflow...")
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
    sys.exit(0)


signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def main() -> None:
    if not AIRFLOW_HOME.exists():
        print("Airflow not set up yet. Run: python scripts/setup_airflow_local.py")
        sys.exit(1)

    print("=" * 60)
    print("  NexCart Lakehouse — Starting Airflow")
    print(f"  AIRFLOW_HOME : {AIRFLOW_HOME}")
    print(f"  DAGs folder  : {PROJECT_ROOT / 'airflow' / 'dags'}")
    print("=" * 60)
    print("\n  Webserver : http://localhost:8080  (admin / admin)")
    print("  Stop      : Ctrl+C")
    print()

    # Start webserver
    ws = subprocess.Popen(
        [sys.executable, "-m", "airflow", "webserver", "--port", "8080"],
        env=env,
    )
    procs.append(ws)
    time.sleep(3)   # give webserver a head start

    # Start scheduler
    sc = subprocess.Popen(
        [sys.executable, "-m", "airflow", "scheduler"],
        env=env,
    )
    procs.append(sc)

    print("\nBoth processes started. Open http://localhost:8080\n")

    # Keep alive until Ctrl+C
    while True:
        for p in procs:
            if p.poll() is not None:
                print(f"Process {p.args} exited unexpectedly. Shutting down.")
                _shutdown(None, None)
        time.sleep(5)


if __name__ == "__main__":
    main()
