"""
setup_airflow_local.py
──────────────────────
One-command local Airflow setup for the NexCart Lakehouse project.

What this does:
  1. Installs apache-airflow and its constraints into the current Python env
  2. Initialises the Airflow SQLite database
  3. Creates an admin user  (admin / admin)
  4. Prints the commands to start the webserver + scheduler

Run from the project root:
    python scripts/setup_airflow_local.py

After setup, start Airflow with:
    python scripts/start_airflow_local.py

Then open http://localhost:8080  (admin / admin)
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ── Airflow version pinned to match docker-compose ────────────────────────────
AIRFLOW_VERSION = "2.9.1"
PYTHON_VERSION  = f"{sys.version_info.major}.{sys.version_info.minor}"
CONSTRAINTS_URL = (
    f"https://raw.githubusercontent.com/apache/airflow/"
    f"constraints-{AIRFLOW_VERSION}/constraints-{PYTHON_VERSION}.txt"
)

# ── Airflow home inside the project (keeps everything self-contained) ─────────
AIRFLOW_HOME = PROJECT_ROOT / "airflow" / "home"
AIRFLOW_HOME.mkdir(parents=True, exist_ok=True)

# Build the SQLite DSN with 4 slashes (required on Windows)
# Forward slashes work cross-platform; backslashes do not.
_db_path = AIRFLOW_HOME / "airflow.db"
_db_uri  = "sqlite:////" + _db_path.as_posix()

# Set env var so all subsequent subprocess calls see the right home
os.environ["AIRFLOW_HOME"]                               = str(AIRFLOW_HOME)
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]        = _db_uri
os.environ["AIRFLOW__CORE__DAGS_FOLDER"]                 = str(PROJECT_ROOT / "airflow" / "dags")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"]               = "False"
os.environ["AIRFLOW__CORE__EXECUTOR"]                    = "SequentialExecutor"
os.environ["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] = "True"
os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"]            = "WARNING"
os.environ["NEXCART_ROOT"]                               = str(PROJECT_ROOT)
os.environ["PYTHONPATH"]                                 = str(PROJECT_ROOT)


def run(cmd: list[str], **kwargs) -> None:
    print(f"\n>>> {' '.join(cmd)}")
    result = subprocess.run(cmd, **kwargs)
    if result.returncode != 0:
        print(f"ERROR: command failed with exit code {result.returncode}")
        sys.exit(result.returncode)


def main() -> None:
    print("=" * 60)
    print("  NexCart Lakehouse — Local Airflow Setup")
    print(f"  AIRFLOW_HOME : {AIRFLOW_HOME}")
    print(f"  Python       : {sys.executable}")
    print(f"  Airflow ver  : {AIRFLOW_VERSION}")
    print("=" * 60)

    # Step 1 — Install Airflow
    print("\n[1/4] Installing apache-airflow (this may take a few minutes)...")
    run([
        sys.executable, "-m", "pip", "install",
        f"apache-airflow=={AIRFLOW_VERSION}",
        "--constraint", CONSTRAINTS_URL,
        "--quiet",
    ])

    # Step 2 — Initialise DB
    print("\n[2/4] Initialising Airflow database...")
    run([sys.executable, "-m", "airflow", "db", "migrate"],
        env={**os.environ})

    # Step 3 — Create admin user
    print("\n[3/4] Creating admin user (admin / admin)...")
    run([
        sys.executable, "-m", "airflow", "users", "create",
        "--username", "admin",
        "--password", "admin",
        "--firstname", "NexCart",
        "--lastname",  "Admin",
        "--role",      "Admin",
        "--email",     "admin@nexcart.io",
    ], env={**os.environ})

    # Step 4 — Unpause the DAG
    print("\n[4/4] Unpausing nexcart_lakehouse_daily DAG...")
    run([
        sys.executable, "-m", "airflow", "dags", "unpause",
        "nexcart_lakehouse_daily",
    ], env={**os.environ})

    print("\n" + "=" * 60)
    print("  Setup complete!")
    print("=" * 60)
    print(f"\n  AIRFLOW_HOME is: {AIRFLOW_HOME}")
    print("\n  Next steps:")
    print("    python scripts/start_airflow_local.py")
    print("\n  Then open:  http://localhost:8080  (admin / admin)")
    print("\n  To trigger the pipeline manually:")
    print("    airflow dags trigger nexcart_lakehouse_daily")
    print("    # or click 'Trigger DAG' in the UI")
    print("=" * 60)


if __name__ == "__main__":
    main()
