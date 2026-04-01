#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
# setup_env.sh — One-command local dev environment bootstrap
# ══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  NexCart Lakehouse — Environment Setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1. Python version check ────────────────────────────────────────────────────
REQUIRED_PYTHON="3.11"
PYTHON=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
if [[ "$PYTHON" != "$REQUIRED_PYTHON" ]]; then
  echo "⚠ WARNING: Python $REQUIRED_PYTHON recommended, found $PYTHON"
fi

# ── 2. Java check (Spark needs Java 11 or 17) ─────────────────────────────────
java -version 2>&1 | head -1 || { echo "❌ Java not found. Install JDK 11+"; exit 1; }

# ── 3. Virtual environment ─────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
  python3 -m venv .venv
  echo "✅ Virtual environment created"
fi
source .venv/bin/activate

# ── 4. Install dependencies ────────────────────────────────────────────────────
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
echo "✅ Python dependencies installed"

# ── 5. Create .env if missing ─────────────────────────────────────────────────
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "✅ .env created from template — review and customise"
fi

# ── 6. Create data layer directories ──────────────────────────────────────────
for dir in data/raw data/bronze data/silver data/gold data/features data/checkpoints logs; do
  mkdir -p "$dir"
done
echo "✅ Data directories ready"

# ── 7. Validate Spark + Delta ─────────────────────────────────────────────────
python3 - << 'PYEOF'
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
builder = configure_spark_with_delta_pip(
    SparkSession.builder.appName("smoke_test").master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(f"✅ Spark {spark.version} + Delta Lake — OK")
spark.stop()
PYEOF

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Environment ready. Next: run Phase 2 data generation"
echo "  Command: python scripts/generate_data.py"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
