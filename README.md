# NexCart Lakehouse

### Production-Grade Retail Data Platform — Medallion Architecture v2.0

> End-to-end data lakehouse built with **PySpark 3.5**, **Apache Airflow**, **PostgreSQL**, and **Streamlit**.
> Implements the full **Medallion Architecture** (Bronze → Silver → Gold → Features) with a live analytics dashboard, automated orchestration, audit logging, and data quality monitoring.
> Fully compatible with **Windows, Mac, and Linux** — no Delta Lake or winutils required.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          NexCart Lakehouse Pipeline                             │
└─────────────────────────────────────────────────────────────────────────────────┘

  [Raw CSV Files]
       │
       │  Schema enforcement · Audit columns · Partitioning
       ▼
  [Bronze Layer — Parquet]
       │
       │  Type casting · Deduplication · Business rule validation
       ▼
  [Silver Layer — Parquet]
       │
       ├──────────────────────────────────────┐
       │                                      │
       │  KPI aggregations · Pre-aggregated   │  ML-ready features · Lookback
       │  Business metrics                    │  windows · Encoded variables
       ▼                                      ▼
  [Gold Layer]                          [Features Layer — Parquet]
  ├── Parquet (archive)
  └── PostgreSQL (serving layer)
       │
       │  psycopg2 · @st.cache_data · Live queries
       ▼
  [Streamlit Dashboard]
  Executive Overview · Gold Explorer · Pipeline Monitor · Quality Checks

       ┌──────────────────────────────┐
       │     Apache Airflow (Docker)  │
       │  DAG scheduling · Run logs   │
       │  Retry logic · Monitoring    │
       └──────────────────────────────┘

       ┌──────────────────────────────┐
       │       Audit Layer            │
       │  pipeline_runs.jsonl         │
       │  nexcart.log · Run history   │
       └──────────────────────────────┘
```

---

## Tech Stack

| Component       | Technology                        |
|-----------------|-----------------------------------|
| Processing      | PySpark 3.5                       |
| Storage         | Parquet + Snappy compression      |
| Serving Layer   | PostgreSQL 15 (Gold tables)       |
| Orchestration   | Apache Airflow 2.9 (Docker)       |
| Dashboard       | Streamlit + Plotly                |
| DB Connector    | psycopg2                          |
| Query Layer     | DuckDB (ad-hoc Parquet queries)   |
| Config          | YAML                              |
| Data Generation | Faker + Pandas                    |
| Logging         | Python logging + JSONL audit log  |

---

## Project Features

| Feature                    | Description                                                         |
|----------------------------|---------------------------------------------------------------------|
| Medallion Architecture     | Bronze → Silver → Gold → Features, each layer with a clear contract |
| PostgreSQL Serving Layer   | Gold tables written to PostgreSQL for low-latency dashboard reads   |
| Live Streamlit Dashboard   | Multi-page analytics app reading directly from PostgreSQL           |
| Airflow Orchestration      | Docker-based DAG with per-layer tasks, retries, and run tracking    |
| Data Quality Framework     | Rule-based DQ checks across Silver layer with a quality report      |
| Audit Logging              | Every pipeline run logged to JSONL with status, duration, row count |
| Feature Engineering        | 4 ML-ready feature tables with labels for supervised learning       |
| DuckDB Query Layer         | Query any Parquet layer without starting Spark                      |

---

## Data Pipeline — Layer by Layer

### Bronze — Raw Ingestion
- Reads raw CSV files from `data/raw/`
- Enforces schema and data types
- Adds audit columns: `ingested_at`, `source_file`, `batch_id`
- Writes immutable Parquet partitions to `data/bronze/`
- Never modifies source files

### Silver — Cleaning & Enrichment
- Reads from Bronze Parquet
- Applies type casting, null handling, and deduplication
- Enforces business rules (e.g., valid order statuses, price ranges)
- Enriches records (e.g., joins customer segments, computes derived fields)
- Writes cleaned Parquet to `data/silver/`

### Gold — Business Aggregations
- Reads from Silver Parquet
- Computes KPI tables: revenue, customer LTV, product performance, inventory health, fulfillment
- Writes to **two destinations**:
  - `data/gold/` — Parquet archive for DuckDB/batch queries
  - **PostgreSQL** — live serving layer for the Streamlit dashboard

### Features — ML Preparation
- Reads from Silver Parquet
- Engineers features: lookback windows, RFM scores, lag features, encoding
- Produces 4 supervised learning datasets with labels
- Writes to `data/features/` as Parquet

### Quality — Data Quality Checks
- Runs rule-based DQ checks across Silver tables
- Checks: completeness, uniqueness, referential integrity, value range
- Produces a quality report stored as Parquet
- Results visible in the dashboard Quality Checks page

### Audit — Observability
- Every layer run writes a structured record to `logs/pipeline_runs.jsonl`
- Fields: `layer`, `status`, `start_time`, `duration_s`, `rows_written`, `error`
- Viewable via CLI (`--history` flag) and the Pipeline Monitor dashboard page

---

## Gold Tables (PostgreSQL)

| Table                | Rows (approx.) | Description                          |
|----------------------|----------------|--------------------------------------|
| `daily_revenue`      | ~23,000        | Revenue KPIs by day / channel / segment |
| `customer_ltv`       | ~5,000         | Lifetime value, RFM tiers, churn flag   |
| `product_performance`| ~12,000        | Monthly sales and margin by product     |
| `inventory_health`   | ~500           | Stock status and reorder alerts         |
| `fulfillment_kpis`   | ~8,300         | Carrier on-time rates by week           |

All five tables are queried live by the dashboard via psycopg2 with `@st.cache_data(ttl=300)`.

---

## ML Feature Tables (Parquet)

| Table                | Label          | Use Case                   |
|----------------------|----------------|----------------------------|
| `customer_features`  | `is_churned`   | Customer churn prediction  |
| `product_features`   | `orders_30d`   | Demand forecasting         |
| `order_risk_features`| `is_anomaly`   | Fraud / anomaly detection  |
| `delivery_features`  | `is_delayed`   | Delivery delay prediction  |

---

## PostgreSQL Integration

Gold tables are written to PostgreSQL at the end of the Gold build step, making them available as a low-latency SQL serving layer.

**Connection details (local / Docker setup):**

| Setting  | Value      |
|----------|------------|
| Host     | `localhost` |
| Port     | `5433`     |
| Database | `airflow`  |
| User     | `airflow`  |
| Password | `airflow`  |

**How the dashboard connects:**

```python
# dashboard/utils.py
import psycopg2
import pandas as pd
import streamlit as st

_PG_KWARGS = dict(host="localhost", port=5433,
                  dbname="airflow", user="airflow", password="airflow")

def _pg_conn():
    return psycopg2.connect(**_PG_KWARGS)

@st.cache_data(ttl=300, show_spinner=False)
def load_table(layer: str, table: str):
    if layer == "gold":
        conn = _pg_conn()
        try:
            return pd.read_sql(f'SELECT * FROM "{table}"', conn)
        finally:
            conn.close()
```

Results are cached for 5 minutes per table using `@st.cache_data`, preventing redundant queries on every page interaction.

---

## Airflow Orchestration

The pipeline is fully automated via an Apache Airflow DAG running in Docker.

**DAG:** `nexcart_lakehouse_dag`
**Schedule:** `@daily` (configurable)
**Location:** `airflow/dags/nexcart_lakehouse_dag.py`

**Task graph:**

```
generate_data → bronze → silver → gold → features → quality
                                     └──────────────────────→ audit_log
```

**Key DAG properties:**

| Property        | Value              |
|-----------------|--------------------|
| Schedule        | Daily (`@daily`)   |
| Retries         | 2 per task         |
| Retry delay     | 5 minutes          |
| Catchup         | Disabled           |
| Execution mode  | Sequential (local) |

**Start Airflow (Docker):**

```bash
cd airflow
docker compose up -d
# Airflow UI → http://localhost:8080
# Username: airflow  |  Password: airflow
```

**Trigger a manual run:**

```bash
docker exec -it <airflow-webserver-container> \
  airflow dags trigger nexcart_lakehouse_dag
```

**View task logs:**

```bash
docker exec -it <airflow-webserver-container> \
  airflow tasks logs nexcart_lakehouse_dag gold <run_id>
```

---

## Streamlit Dashboard

The dashboard connects exclusively to PostgreSQL for Gold layer data and to local Parquet for Bronze / Silver / Features metadata.

**Launch:**

```bash
streamlit run dashboard/app.py
# http://localhost:8501
```

**Pages:**

| Page                  | Content                                                     |
|-----------------------|-------------------------------------------------------------|
| **Home**              | Architecture overview, layer inventory, table availability  |
| **Executive Overview**| C-suite KPIs: revenue, orders, AOV, churn, on-time delivery |
| **Gold Layer**        | Interactive explorer for all 5 Gold tables with charts      |
| **Features Layer**    | ML feature table previews and distributions                 |
| **Quality Checks**    | DQ rule results, pass/fail rates, data health summary       |
| **Pipeline Monitor**  | Run history, layer status, last execution timestamps        |

**Data refresh:** Gold table queries are cached for 5 minutes. Force a refresh by clearing the Streamlit cache (`C` keyboard shortcut or restart the app).

---

## Project Structure

```
nexcart-lakehouse/
├── config/
│   └── settings.yaml                    # Central config (paths, Spark settings)
│
├── data/
│   ├── raw/              *.csv          # Source files — never modified
│   ├── bronze/           *.parquet      # Immutable ingested archive
│   ├── silver/           *.parquet      # Cleaned + enriched
│   ├── gold/             *.parquet      # Aggregated (archive copy)
│   └── features/         *.parquet      # ML-ready feature tables
│
├── src/
│   ├── ingestion/
│   │   └── bronze_ingestor.py           # CSV → Bronze Parquet
│   ├── transformation/
│   │   └── silver_transformer.py        # Bronze → Silver Parquet
│   ├── gold/
│   │   └── gold_builder.py              # Silver → Gold Parquet + PostgreSQL
│   ├── features/
│   │   └── feature_engineer.py          # Silver → Feature Parquet
│   ├── quality/
│   │   └── data_quality.py              # DQ checks + quality report
│   └── utils/
│       ├── spark_session.py             # SparkSession factory (Windows-safe)
│       ├── parquet_utils.py             # Parquet read/write helpers
│       ├── config_loader.py             # YAML config loader
│       ├── pipeline_logger.py           # Audit log writer (JSONL)
│       └── logger.py                    # Stdlib logging wrapper
│
├── dashboard/
│   ├── app.py                           # Streamlit entry point
│   ├── utils.py                         # Shared helpers, PostgreSQL connector
│   ├── requirements.txt                 # Dashboard dependencies
│   └── pages/
│       ├── 1_Home.py
│       ├── 2_Executive_Overview.py
│       ├── 3_Gold_Layer.py
│       ├── 4_Features_Layer.py
│       ├── 5_Quality_Checks.py
│       └── 6_Pipeline_Monitor.py
│
├── airflow/
│   ├── docker-compose.yaml              # Airflow + PostgreSQL Docker setup
│   └── dags/
│       └── nexcart_lakehouse_dag.py     # Main pipeline DAG
│
├── scripts/
│   ├── run_pipeline.py                  # One-command local runner
│   ├── run_pipeline_audited.py          # Runner with full audit logging
│   ├── query_duckdb.py                  # Ad-hoc Parquet SQL (no Spark)
│   ├── generate_data_lite.py            # Synthetic data generator
│   └── setup_env.sh                     # Environment setup helper
│
├── logs/
│   ├── nexcart.log                      # Structured application log
│   └── pipeline_runs.jsonl             # Audit trail (one JSON line per run)
│
└── requirements.txt                     # Project dependencies
```

---

## Setup & Run

### Prerequisites

| Tool        | Version     | Notes                         |
|-------------|-------------|-------------------------------|
| Python      | 3.11+       |                               |
| Java        | 11 or 17    | Required for PySpark          |
| Docker      | 24+         | Required for Airflow          |
| Docker Compose | 2.x      | Bundled with Docker Desktop   |

---

### Step 1 — Install Python dependencies

```bash
pip install -r requirements.txt
```

Or install manually:

```bash
pip install pyspark==3.5.1 pyarrow pandas numpy pyyaml faker \
            duckdb psycopg2-binary streamlit plotly
```

---

### Step 2 — Generate synthetic data

```bash
python scripts/generate_data_lite.py
# Generates ~345K rows across 7 retail source tables in data/raw/
```

---

### Step 3 — Start Airflow (Docker)

```bash
cd airflow
docker compose up -d
```

- Airflow UI: [http://localhost:8080](http://localhost:8080)
- PostgreSQL (shared): `localhost:5433`
- Default credentials: `airflow / airflow`

Wait for the webserver to be healthy (30–60 seconds), then enable and trigger the DAG from the UI or CLI.

---

### Step 4 — Run the pipeline

**Option A — Via Airflow (recommended):**

Trigger the `nexcart_lakehouse_dag` from the Airflow UI. It will run all layers in order and load Gold tables into PostgreSQL automatically.

**Option B — Via CLI (local, no Airflow):**

```bash
# Full pipeline
python scripts/run_pipeline_audited.py

# Individual layers
python scripts/run_pipeline.py --layer bronze
python scripts/run_pipeline.py --layer silver
python scripts/run_pipeline.py --layer gold
python scripts/run_pipeline.py --layer features
python scripts/run_pipeline.py --layer quality
```

**Run order if executing manually:**

```bash
1. python scripts/generate_data_lite.py          # Synthetic retail data
2. python -m src.ingestion.bronze_ingestor        # CSV → Bronze
3. python -m src.transformation.silver_transformer # Bronze → Silver
4. python -m src.gold.gold_builder                # Silver → Gold + PostgreSQL
5. python -m src.features.feature_engineer        # Silver → Features
6. python -m src.quality.data_quality             # DQ checks
```

---

### Step 5 — Launch the dashboard

```bash
cd dashboard
streamlit run app.py --server.port 8501
```

Open [http://localhost:8501](http://localhost:8501). The dashboard reads Gold tables directly from PostgreSQL — no Parquet files required for the analytics views.

---

### Step 6 — View pipeline history (optional)

```bash
python scripts/run_pipeline.py --history
```

---

### Step 7 — Query Parquet layers with DuckDB (optional)

```bash
# List all available tables
python scripts/query_duckdb.py --list

# Query a specific table
python scripts/query_duckdb.py --layer gold --table customer_ltv

# Run custom SQL
python scripts/query_duckdb.py --sql \
  "SELECT carrier, AVG(on_time_rate_pct) FROM gold__fulfillment_kpis GROUP BY carrier"
```

---

## Storage Design

| Layer    | Format              | Purpose                                      |
|----------|---------------------|----------------------------------------------|
| raw      | CSV                 | Immutable source files, never modified       |
| bronze   | Parquet (Snappy)    | Schema-enforced archive with audit columns   |
| silver   | Parquet (Snappy)    | Cleaned, typed, deduplicated records         |
| gold     | Parquet + PostgreSQL| KPI aggregations — Parquet archive + live DB |
| features | Parquet (Snappy)    | Encoded ML training datasets with labels     |

---

## Observability & Audit

Every pipeline execution writes a structured record to `logs/pipeline_runs.jsonl`:

```json
{
  "run_id": "gold_20240315_143022",
  "layer": "gold",
  "status": "success",
  "start_time": "2024-03-15T14:30:22",
  "end_time": "2024-03-15T14:31:05",
  "duration_s": 43.1,
  "rows_written": 23258,
  "tables": ["daily_revenue", "customer_ltv", "product_performance",
             "inventory_health", "fulfillment_kpis"]
}
```

View the full audit trail:

```bash
python scripts/run_pipeline.py --history
```

Pipeline run status is also visible on the **Pipeline Monitor** page of the dashboard.

---

## Screenshots

> Dashboard pages running against live PostgreSQL data.

| Page | Preview |
|------|---------|
| Executive Overview | *(KPI cards, revenue trend, channel split)* |
| Gold Layer Explorer | *(Table selector, schema viewer, charts)* |
| Pipeline Monitor | *(Layer status cards, run history table)* |
| Quality Checks | *(DQ rule pass/fail, data health summary)* |

---

## Why This Project — Interview Value

- **Medallion Architecture** — demonstrates understanding of layered data design and incremental refinement
- **Dual storage strategy** — Gold tables in both Parquet (batch) and PostgreSQL (serving), showing pragmatic storage decisions
- **Production patterns** — connection pooling, query caching (`@st.cache_data`), proper resource cleanup, silent-failure handling
- **Airflow orchestration** — real DAG with dependencies, retries, and scheduling — not just scripts
- **Audit & observability** — every run is tracked; shows operational maturity beyond just making the pipeline work
- **Data quality framework** — rule-based DQ checks with a report layer, not an afterthought
- **Feature engineering** — four ML-ready datasets with labels, demonstrating bridge between DE and ML
- **Full-stack** — raw data to interactive dashboard, end-to-end ownership

---

## Requirements

```
pyspark==3.5.1
pyarrow>=16.0.0
pandas>=2.0.0
numpy>=1.26.0
pyyaml>=6.0
faker>=24.0.0
duckdb>=0.10.0
psycopg2-binary>=2.9.0
streamlit>=1.32.0
plotly>=5.20.0
```

---

*Portfolio project demonstrating production-grade data engineering:
Medallion Architecture · Distributed Processing · PostgreSQL Serving Layer ·
Airflow Orchestration · Feature Engineering · Data Quality · Live Analytics Dashboard.*
