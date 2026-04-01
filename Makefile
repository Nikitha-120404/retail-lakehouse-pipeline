# ============================================================
# NexCart Lakehouse — Makefile
# Run `make help` to see all targets.
# ============================================================

PYTHON     = python
PIP        = pip
VENV_DIR   = .venv
PYTEST     = $(VENV_DIR)/bin/pytest
AIRFLOW_HOME = $(shell pwd)/.airflow

.DEFAULT_GOAL := help

# ── Setup ────────────────────────────────────────────────────────
.PHONY: venv
venv:                          ## Create Python virtual environment
	$(PYTHON) -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install --upgrade pip
	$(VENV_DIR)/bin/pip install -r requirements.txt
	@echo "✓ venv ready — activate with: source $(VENV_DIR)/bin/activate"

.PHONY: install
install:                       ## Install dependencies into current env
	$(PIP) install -r requirements.txt

# ── Data generation ───────────────────────────────────────────────
.PHONY: generate
generate:                      ## Generate all synthetic datasets
	$(PYTHON) scripts/generate_data.py

# ── Pipeline phases ───────────────────────────────────────────────
.PHONY: bronze
bronze:                        ## Run Bronze ingestion (all tables)
	$(PYTHON) -m src.ingestion.bronze_ingestor

.PHONY: silver
silver:                        ## Run Silver transformations
	$(PYTHON) -m src.transformation.silver_transformer

.PHONY: gold
gold:                          ## Run Gold aggregations
	$(PYTHON) -m src.transformation.gold_aggregator

.PHONY: features
features:                      ## Build ML feature tables
	$(PYTHON) -m src.features.feature_builder

.PHONY: pipeline
pipeline: generate bronze silver gold features  ## Run full end-to-end pipeline

# ── Quality ───────────────────────────────────────────────────────
.PHONY: quality
quality:                       ## Run data quality checks on Silver
	$(PYTHON) -m src.quality.quality_runner

# ── Streaming ─────────────────────────────────────────────────────
.PHONY: stream
stream:                        ## Start Structured Streaming job (clickstream)
	$(PYTHON) -m src.ingestion.streaming_ingestor

# ── Airflow ───────────────────────────────────────────────────────
.PHONY: airflow-init
airflow-init:                  ## Initialise Airflow DB and create admin user
	export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	airflow db init && \
	airflow users create \
	  --username admin --password admin \
	  --firstname NexCart --lastname Admin \
	  --role Admin --email admin@nexcart.io

.PHONY: airflow-up
airflow-up:                    ## Start Airflow webserver + scheduler
	export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	airflow webserver --port 8080 &
	export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	airflow scheduler

# ── Tests ─────────────────────────────────────────────────────────
.PHONY: test
test:                          ## Run unit tests
	$(PYTEST) tests/unit -v --tb=short

.PHONY: test-all
test-all:                      ## Run all tests with coverage report
	$(PYTEST) tests/ -v --cov=src --cov-report=term-missing

# ── Cleanup ───────────────────────────────────────────────────────
.PHONY: clean-data
clean-data:                    ## Delete generated Delta tables (keep raw CSVs)
	rm -rf data/bronze data/silver data/gold data/features data/checkpoints
	@echo "✓ Delta tables cleaned"

.PHONY: clean-all
clean-all: clean-data          ## Delete all generated data including raw CSVs
	rm -rf data/raw logs/*.log
	@echo "✓ All generated data cleaned"

# ── Help ──────────────────────────────────────────────────────────
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
