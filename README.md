# NexCart Lakehouse

### End-to-End Retail Data Engineering Platform

NexCart Lakehouse is an end-to-end retail data engineering project built with **PySpark, Apache Airflow, PostgreSQL, Docker, and Streamlit**.

The project simulates a retail/e-commerce data platform where raw business data is processed through a lakehouse-style pipeline using the layered flow:

**Raw → Bronze → Silver → Gold → Features**

Gold analytics tables are served through **PostgreSQL**, and the results are visualized in an interactive **Streamlit dashboard**.

---

## Project Overview

This project processes synthetic retail data including:

- Customers
- Products
- Orders
- Payments
- Inventory
- Clickstream
- Shipments

The pipeline cleans, enriches, aggregates, and prepares data for analytics and machine learning use cases. Apache Airflow orchestrates the workflow, PostgreSQL stores the Gold analytics tables, and Streamlit provides the dashboard interface.

---

## Architecture

```text
Raw CSV Data
     ↓
Bronze Layer
     ↓
Silver Layer
     ↓
Gold Layer
     ↓
PostgreSQL Serving Layer
     ↓
Streamlit Dashboard

Apache Airflow orchestrates the complete pipeline through Docker.
DuckDB is included as an optional utility for local Parquet exploration.
Tech Stack
Component	Technology
Data Processing	Python, PySpark
Orchestration	Apache Airflow
Containerization	Docker, Docker Compose
Serving Layer	PostgreSQL
Dashboard	Streamlit, Plotly
Optional Query Utility	DuckDB
Data Format	CSV, Parquet
Configuration	YAML
Logging / Audit	Python logging, JSONL audit logs
Project Features
Feature	Description
Layered Lakehouse Pipeline	Raw, Bronze, Silver, Gold, and Features layers
PySpark Transformations	Cleans, joins, validates, and aggregates retail data
Airflow Orchestration	Runs the full pipeline with a Dockerized DAG
PostgreSQL Serving Layer	Stores Gold tables for dashboard queries
Streamlit Dashboard	Visualizes business KPIs and pipeline outputs
Data Quality Checks	Validates curated datasets before analytics use
Feature Engineering	Creates ML-ready feature tables
DuckDB Utility	Optional local querying of Parquet outputs
Data Pipeline Layers
Raw Layer

The Raw layer stores the original generated retail source files.

Main raw tables:

customers
products
orders
payments
inventory
clickstream
shipments
Bronze Layer

The Bronze layer ingests raw data and prepares it for downstream processing.

It handles:

Reading raw CSV files
Applying initial structure
Adding ingestion tracking fields
Preparing data for Silver transformations
Silver Layer

The Silver layer contains cleaned and enriched data.

It handles:

Type casting
Null handling
Deduplication
Business rule validation
Joins between related datasets
Derived fields for analytics

Example Silver tables:

customers_clean
products_clean
orders_enriched
payments_validated
inventory_clean
clickstream_sessions
shipments_enriched
Gold Layer

The Gold layer contains business-ready analytics tables.

Gold tables are loaded into PostgreSQL and used by the Streamlit dashboard.

Table	Description
daily_revenue	Revenue, orders, and sales trends
customer_ltv	Customer lifetime value and customer behavior
product_performance	Product sales and performance metrics
inventory_health	Stock status and inventory monitoring
fulfillment_kpis	Shipment and delivery performance metrics
Features Layer

The Features layer prepares ML-ready datasets for analytical and machine learning use cases.

Feature tables:

customer_features
product_features
order_risk_features
delivery_features

These tables support use cases such as customer behavior analysis, demand forecasting, order risk detection, and delivery delay prediction.

PostgreSQL Serving Layer

PostgreSQL is used as the serving layer for Gold analytics tables.

The Streamlit dashboard reads Gold tables from PostgreSQL instead of directly reading local files. This makes the project closer to a real analytics platform where dashboards query a database layer.

pgAdmin Connection

Use these values when connecting through pgAdmin on your laptop:

Host: localhost
Port: 5433
Database: airflow
Username: airflow
Password: airflow
Docker Connection

Inside Docker containers, Streamlit connects to PostgreSQL using:

Host: airflow-db
Port: 5432
Database: airflow
Username: airflow
Password: airflow
Airflow Orchestration

Apache Airflow orchestrates the complete NexCart pipeline.

DAG name:

nexcart_lakehouse_daily

DAG location:

airflow/dags/nexcart_lakehouse_dag.py

Pipeline flow:

generate_data
    ↓
ingest_bronze
    ↓
transform_silver
    ↓
build_gold
    ↓
engineer_features
    ↓
run_quality_checks

Airflow provides task scheduling, dependency management, run logs, and pipeline monitoring.

Streamlit Dashboard

The Streamlit dashboard provides an interactive interface for exploring the project outputs.

Dashboard pages include:

Page	Description
Home	Project overview and architecture
Executive Overview	Business KPIs and revenue insights
Gold Layer	Gold table explorer with charts
Features Layer	ML feature table previews
Quality Checks	Data quality results
Pipeline Monitor	Pipeline status and table availability

The dashboard connects to PostgreSQL for Gold layer data and displays charts, table previews, and pipeline status.

DuckDB Utility

DuckDB is included as an optional local query utility.

It can be used to explore Parquet outputs without starting Spark.

Example commands:

python scripts/query_duckdb.py --list
python scripts/query_duckdb.py --layer gold --table daily_revenue
python scripts/query_duckdb.py --sql "SELECT * FROM gold__customer_ltv LIMIT 10"

DuckDB is not the main serving layer. The main serving layer for the dashboard is PostgreSQL.

Project Structure
nexcart-lakehouse/
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── nexcart_lakehouse_dag.py
│
├── config/
│   └── settings.yaml
│
├── dashboard/
│   ├── app.py
│   ├── utils.py
│   ├── requirements.txt
│   └── pages/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── features/
│
├── docs/
│   └── images/
│       ├── airflow_dag.png
│       ├── dashboard_overview.png
│       ├── gold_layer.png
│       ├── postgres_revenue_data.png
│       └── pipeline_monitor.png
│
├── logs/
│   └── pipeline_runs.jsonl
│
├── scripts/
│   ├── generate_data.py
│   ├── generate_data_lite.py
│   ├── query_duckdb.py
│   ├── run_pipeline.py
│   └── run_pipeline_audited.py
│
├── src/
│   ├── ingestion/
│   ├── transformation/
│   ├── gold/
│   ├── features/
│   ├── quality/
│   └── utils/
│
├── tests/
├── docker-compose.yml
├── requirements.txt
└── README.md
Setup and Run
Prerequisites

Make sure these are installed:

Docker Desktop
Python 3.11+
Git
pgAdmin 4
VS Code
Run with Docker

Run all commands from the root project folder:

nexcart-lakehouse/
Step 1: Initialize Airflow
docker compose up airflow-init

Airflow login:

Username: admin
Password: admin
Step 2: Start Airflow
docker compose up airflow-webserver airflow-scheduler

Open Airflow:

http://localhost:8080

Turn on and trigger the DAG:

nexcart_lakehouse_daily
Step 3: Start Streamlit Dashboard

Open a new terminal and run:

docker compose up streamlit

Open the dashboard:

http://localhost:8501
Step 4: Connect pgAdmin to PostgreSQL

Use these connection details:

Host: localhost
Port: 5433
Database: airflow
Username: airflow
Password: airflow

Example query:

SELECT *
FROM public.daily_revenue
LIMIT 20;
Manual Pipeline Commands

The pipeline can also be run manually.

Run full pipeline:

python scripts/run_pipeline.py

Run individual layers:

python scripts/run_pipeline.py --layer generate
python scripts/run_pipeline.py --layer bronze
python scripts/run_pipeline.py --layer silver
python scripts/run_pipeline.py --layer gold
python scripts/run_pipeline.py --layer features
python scripts/run_pipeline.py --layer quality

View pipeline history:

python scripts/run_pipeline.py --history
Useful PostgreSQL Queries

List all tables:

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

Preview daily revenue:

SELECT *
FROM public.daily_revenue
LIMIT 20;

Check row count:

SELECT COUNT(*)
FROM public.daily_revenue;
Screenshots
Airflow DAG

Streamlit Dashboard Overview

Gold Layer Explorer

PostgreSQL Revenue Data

Pipeline Monitor

What This Project Demonstrates
Built an end-to-end retail data engineering pipeline
Designed Raw, Bronze, Silver, Gold, and Features layers
Used PySpark for data processing and transformations
Used Airflow to orchestrate the workflow inside Docker
Loaded Gold analytics tables into PostgreSQL
Built a Streamlit dashboard connected to PostgreSQL
Added data quality checks and pipeline monitoring
Included DuckDB for optional local Parquet exploration
Organized the project with clear structure, screenshots, and run commands
Requirements
pyspark
pandas
numpy
pyarrow
pyyaml
faker
duckdb
psycopg2-binary
streamlit
plotly
apache-airflow

Summary

NexCart Lakehouse demonstrates a complete data engineering workflow from raw retail data to business analytics. It combines PySpark processing, Airflow orchestration, PostgreSQL serving, and Streamlit visualization in one project.