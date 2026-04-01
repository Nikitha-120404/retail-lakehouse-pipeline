"""
nexcart_lakehouse_dag.py
─────────────────────────
NexCart Lakehouse — Primary Airflow DAG.

Orchestrates the full medallion pipeline by invoking the existing
run_pipeline_audited.py entry point with --layer flags via BashOperator.

Using BashOperator gives each PySpark layer its own JVM process,
which avoids SparkContext conflicts between tasks and maps cleanly
onto how the pipeline already runs locally.

DAG flow
────────
    start
      └─► generate_data
              └─► ingest_bronze
                      └─► transform_silver
                              ├─► build_gold
                              └─► engineer_features
                                      └─► run_quality_checks
                                              └─► pipeline_complete

Schedule : daily at 01:00 UTC
Retries  : 2, with 5-minute exponential back-off
Timeout  : 60 minutes per task

Local run (no Docker needed)
─────────────────────────────
    # Dry-run one logical date — no scheduler needed
    airflow dags test nexcart_lakehouse_daily 2024-01-01

    # Trigger manually
    airflow dags trigger nexcart_lakehouse_daily

    # Or click "Trigger DAG" in the Airflow UI at http://localhost:8080
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Project root resolution ────────────────────────────────────────────────────
# Supports two modes:
#   1. Local pip  — DAG lives at <project>/airflow/dags/ → parents[2] = project root
#   2. Docker     — set NEXCART_ROOT=/opt/nexcart in docker-compose env
PROJECT_ROOT = os.environ.get(
    "NEXCART_ROOT",
    str(Path(__file__).resolve().parents[2]),
)

# Python executable — override via NEXCART_PYTHON if virtualenv path differs
PYTHON = os.environ.get("NEXCART_PYTHON", "python")

# ── Base shell command ─────────────────────────────────────────────────────────
# Sets PYTHONPATH so all src.* imports resolve, then delegates to the
# existing audited runner with a single --layer flag.
_BASE = (
    f"cd {PROJECT_ROOT} && "
    f"PYTHONPATH={PROJECT_ROOT} "
    f"{PYTHON} {PROJECT_ROOT}/scripts/run_pipeline_audited.py"
)

# ── Default task arguments ─────────────────────────────────────────────────────
DEFAULT_ARGS: dict = {
    "owner":                     "data-engineering",
    "depends_on_past":           False,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(minutes=60),
}

# ══════════════════════════════════════════════════════════════════════════════
# DAG definition
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="nexcart_lakehouse_daily",
    description=(
        "NexCart Lakehouse — full medallion pipeline "
        "(Bronze -> Silver -> Gold -> Features -> Quality)"
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *",   # daily at 01:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nexcart", "lakehouse", "medallion"],
    doc_md="""
## NexCart Lakehouse Pipeline

Full medallion architecture orchestrated by Airflow.

| # | Task ID              | Layer    | Description                              |
|---|----------------------|----------|------------------------------------------|
| 1 | `generate_data`      | raw      | Synthesise retail CSV data               |
| 2 | `ingest_bronze`      | bronze   | CSV -> Parquet with audit columns        |
| 3 | `transform_silver`   | silver   | Cleanse + enrich Bronze tables           |
| 4 | `build_gold`         | gold     | Business KPI aggregations                |
| 5 | `engineer_features`  | features | ML-ready feature vectors                 |
| 6 | `run_quality_checks` | quality  | DQ validation across Silver layer        |

**Retry policy**: 2 retries, 5-min exponential back-off, 60-min timeout per task.
""",
) as dag:

    # ── Sentinels ──────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.ALL_DONE,   # runs even if quality fails
    )

    # ── Step 1: Data generation ────────────────────────────────────────────────
    generate = BashOperator(
        task_id="generate_data",
        bash_command=f"{_BASE} --layer generate ",
        doc_md="Generate synthetic retail CSV data and write to `data/raw/`.",
    )

    # ── Step 2: Bronze ingestion ───────────────────────────────────────────────
    bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{_BASE} --layer bronze ",
        doc_md=(
            "Ingest all raw CSVs into Bronze Parquet tables, "
            "adding `_ingested_at` and `_source_file` audit columns."
        ),
    )

    # ── Step 3: Silver transformation ─────────────────────────────────────────
    silver = BashOperator(
        task_id="transform_silver",
        bash_command=f"{_BASE} --layer silver ",
        doc_md=(
            "Cleanse, type-cast, deduplicate, and enrich all Bronze tables "
            "into Silver Parquet tables."
        ),
    )

    # ── Step 4: Gold aggregation ───────────────────────────────────────────────
    gold = BashOperator(
        task_id="build_gold",
        bash_command=f"{_BASE} --layer gold ",
        doc_md=(
            "Compute business KPIs (daily revenue, customer LTV, product performance, "
            "inventory health, fulfillment KPIs) from Silver into Gold Parquet tables."
        ),
    )

    # ── Step 5: Feature engineering ───────────────────────────────────────────
    features = BashOperator(
        task_id="engineer_features",
        bash_command=f"{_BASE} --layer features ",
        doc_md=(
            "Build ML-ready feature tables (customer churn, product velocity, "
            "order risk, delivery delay) from Silver data."
        ),
    )

    # ── Step 6: Quality checks ────────────────────────────────────────────────
    quality = BashOperator(
        task_id="run_quality_checks",
        bash_command=f"{_BASE} --layer quality ",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md=(
            "Run data quality validation rules across the Silver layer "
            "(null rates, row counts, duplicates, enumerations) "
            "and write the quality report to data/gold/quality_report."
        ),
    )

    # ── Dependency graph ───────────────────────────────────────────────────────
    #
    #   start -> generate -> bronze -> silver -+-> gold     -+
    #                                          +-> features  -+-> quality -> end
    #
    start >> generate >> bronze >> silver
    silver >> [gold, features]
    [gold, features] >> quality >> end
