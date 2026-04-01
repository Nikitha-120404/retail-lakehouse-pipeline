"""
utils.py
────────
Shared utilities for the NexCart Lakehouse Streamlit dashboard.
Imported by app.py and all pages/*.py files.

Gold-layer tables are served from PostgreSQL.
Bronze / Silver / Features tables are still read from local Parquet.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import psycopg2
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
import yaml

# Project root is one level above dashboard/
ROOT = Path(__file__).resolve().parent.parent

sys.path.insert(0, str(ROOT))

# ── PostgreSQL connection ──────────────────────────────────────────────────────
# psycopg2 is used directly because pandas 2.x + SQLAlchemy 1.4 are incompatible
# (pd.read_sql no longer accepts a SQLAlchemy 1.4 Connection object).
_PG_KWARGS: dict = dict(
    host="localhost",
    port=5433,
    dbname="airflow",
    user="airflow",
    password="airflow",
)


def _pg_conn() -> "psycopg2.connection":
    """Open and return a new psycopg2 connection. Caller must close it."""
    return psycopg2.connect(**_PG_KWARGS)


@st.cache_data(ttl=300, show_spinner=False)
def get_data(query: str) -> pd.DataFrame:
    """
    Execute *query* against PostgreSQL and return a pandas DataFrame.
    Results are cached for 5 minutes.
    """
    conn = _pg_conn()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


# Gold tables that live in PostgreSQL
_GOLD_TABLES = {
    "daily_revenue",
    "customer_ltv",
    "product_performance",
    "inventory_health",
    "fulfillment_kpis",
}

# ── Known tables per layer ─────────────────────────────────────────────────────
LAYER_TABLES: dict[str, list[str]] = {
    "bronze":   ["customers", "products", "orders", "payments",
                 "inventory", "clickstream", "shipments"],
    "silver":   ["customers_clean", "products_clean", "orders_enriched",
                 "payments_validated", "inventory_clean",
                 "clickstream_sessions", "shipments_enriched"],
    "gold":     ["daily_revenue", "customer_ltv", "product_performance",
                 "inventory_health", "fulfillment_kpis"],
    "features": ["customer_features", "product_features",
                 "order_risk_features", "delivery_features"],
}

# ── Plotly colour palette ──────────────────────────────────────────────────────
PLOT_BG   = "#13161f"
PAPER_BG  = "#13161f"
FONT_CLR  = "#e2e8f0"   # bright: readable axis ticks, legends, titles
GRID_CLR  = "#1e2438"

# Minimal base layout (no xaxis/yaxis — use get_layout() instead)
PLOT_LAYOUT: dict = dict(
    plot_bgcolor=PLOT_BG,
    paper_bgcolor=PAPER_BG,
    font_color=FONT_CLR,
    legend=dict(font=dict(color=FONT_CLR, size=12), bgcolor="rgba(0,0,0,0)"),
    margin=dict(t=40, b=30, l=10, r=10),
)

_AXIS_DEFAULTS = dict(
    gridcolor=GRID_CLR,
    linecolor=GRID_CLR,
    zerolinecolor=GRID_CLR,
    tickfont=dict(color=FONT_CLR, size=11),
    title_font=dict(color=FONT_CLR),
)


def get_layout(**overrides) -> dict:
    """
    Return a Plotly layout dict merging dark-theme defaults with caller overrides.
    Dict-type values (xaxis, yaxis, legend, margin) are deep-merged so that
    update_layout() never receives a duplicate keyword argument.
    """
    result: dict = {
        "plot_bgcolor":  PLOT_BG,
        "paper_bgcolor": PAPER_BG,
        "font_color":    FONT_CLR,
        "xaxis":         dict(_AXIS_DEFAULTS),
        "yaxis":         dict(_AXIS_DEFAULTS),
        "legend":        {"font": {"color": FONT_CLR, "size": 12},
                          "bgcolor": "rgba(0,0,0,0)"},
        "title_font":    {"color": "#f1f5f9", "size": 15},
        "margin":        {"t": 44, "b": 30, "l": 10, "r": 10},
    }
    for key, val in overrides.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = {**result[key], **val}
        else:
            result[key] = val
    return result


# ── Config loading ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_config() -> dict:
    path = ROOT / "config" / "settings.yaml"
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def get_layer_path(layer: str) -> Path:
    cfg = load_config()
    return ROOT / cfg["paths"][layer]


# ── Table loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def load_table(layer: str, table: str) -> Optional[pd.DataFrame]:
    """
    Load a table into a pandas DataFrame.

    * Gold tables  → PostgreSQL (direct engine call — avoids nested @st.cache_data)
    * Other layers → local Spark-written Parquet directory
    Returns None (never raises) if the table is missing or unreadable.
    """
    if layer == "gold" and table in _GOLD_TABLES:
        try:
            conn = _pg_conn()
            try:
                df = pd.read_sql(f'SELECT * FROM "{table}"', conn)
            finally:
                conn.close()
            return df if not df.empty else None
        except Exception:
            return None

    # ── Parquet fallback for bronze / silver / features ────────────────────────
    path = get_layer_path(layer) / table
    if not path.exists():
        return None
    try:
        files = list(path.glob("**/*.parquet"))
        if not files:
            return None
        return pq.read_table(str(path)).to_pandas()
    except Exception:
        try:
            frames = [pq.read_table(str(f)).to_pandas() for f in files]
            return pd.concat(frames, ignore_index=True) if frames else None
        except Exception:
            return None


@st.cache_data(ttl=60, show_spinner=False)
def get_table_inventory() -> list[dict]:
    """
    Walk every known table and return metadata for the Pipeline Monitor.
    Gold tables are checked against PostgreSQL; all others use the filesystem.
    """
    # Fetch row counts for gold tables in a single connection (best-effort)
    pg_row_counts: dict[str, int] = {}
    try:
        conn = _pg_conn()
        try:
            cur = conn.cursor()
            for tbl in _GOLD_TABLES:
                cur.execute(f'SELECT COUNT(*) FROM "{tbl}"')
                pg_row_counts[tbl] = cur.fetchone()[0]
            cur.close()
        finally:
            conn.close()
    except Exception:
        pass  # if Postgres is down, gold tables will show as missing

    rows = []
    for layer, tables in LAYER_TABLES.items():
        for table in tables:
            if layer == "gold" and table in _GOLD_TABLES:
                row_count = pg_row_counts.get(table)
                exists = row_count is not None
                rows.append({
                    "layer":         layer,
                    "table":         table,
                    "exists":        exists,
                    "parquet_files": row_count if exists else 0,  # repurposed as row count
                    "size_mb":       0,
                    "last_modified": "PostgreSQL" if exists else "—",
                })
                continue

            # Filesystem check for bronze / silver / features
            layer_path = get_layer_path(layer)
            table_path = layer_path / table
            exists = table_path.exists()
            parquet_files: list[Path] = []
            total_bytes = 0
            last_mod: Optional[float] = None

            if exists:
                parquet_files = list(table_path.glob("**/*.parquet"))
                for f in parquet_files:
                    stat = f.stat()
                    total_bytes += stat.st_size
                    if last_mod is None or stat.st_mtime > last_mod:
                        last_mod = stat.st_mtime

            rows.append({
                "layer":         layer,
                "table":         table,
                "exists":        exists,
                "parquet_files": len(parquet_files),
                "size_mb":       round(total_bytes / 1_048_576, 2) if total_bytes else 0,
                "last_modified": (
                    pd.Timestamp(last_mod, unit="s").strftime("%Y-%m-%d %H:%M")
                    if last_mod else "—"
                ),
            })
    return rows


# ── Formatters ─────────────────────────────────────────────────────────────────
def fmt_currency(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "$0"
    if val >= 1_000_000:
        return f"${val / 1_000_000:.2f}M"
    if val >= 1_000:
        return f"${val / 1_000:.1f}K"
    return f"${val:,.2f}"


def fmt_num(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "0"
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"{val / 1_000:.1f}K"
    return f"{int(val):,}"


def fmt_pct(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "0%"
    return f"{val:.1f}%"


# ── HTML helpers ───────────────────────────────────────────────────────────────
def metric_card(label: str, value: str, sub: str = "") -> str:
    sub_html = (
        f'<div style="color:#64748b;font-size:11px;margin-top:8px;'
        f'letter-spacing:0.4px">{sub}</div>'
    ) if sub else ""
    return f"""
    <div style="
        background: linear-gradient(135deg, #13161f 0%, #1a1f2e 100%);
        border: 1px solid #1e2438;
        border-radius: 14px;
        padding: 22px 20px;
        text-align: center;
        height: 100%;
        box-shadow: 0 4px 24px rgba(0,0,0,0.35);
    ">
      <div style="
          color: #94a3b8;
          font-size: 10px;
          font-weight: 700;
          letter-spacing: 1.2px;
          text-transform: uppercase;
          margin-bottom: 10px;
      ">{label}</div>
      <div style="
          color: #f8fafc;
          font-size: 26px;
          font-weight: 700;
          letter-spacing: -0.5px;
          line-height: 1.1;
      ">{value}</div>
      {sub_html}
    </div>"""


PAGE_CSS = """
<style>
/* ── Layout ────────────────────────────────────────────────────────────────── */
.stApp { background: #0f1117; }
.main .block-container {
    padding-top: 2rem;
    padding-bottom: 3rem;
    max-width: 1400px;
}

/* ── Typography ────────────────────────────────────────────────────────────── */
h1 { color: #f8fafc !important; font-size: 1.9rem !important; font-weight: 700 !important; letter-spacing: -0.3px !important; }
h2, h3 { color: #f1f5f9 !important; font-weight: 600 !important; }
p   { color: #e2e8f0; }

/* ── Sidebar ───────────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #0d1018 !important;
    border-right: 1px solid #1e2438 !important;
}
[data-testid="stSidebar"] .stMarkdown p { color: #94a3b8 !important; }
[data-testid="stSidebarNav"] a {
    color: #94a3b8 !important;
    border-radius: 8px !important;
    padding: 6px 12px !important;
}
[data-testid="stSidebarNav"] a:hover { color: #f1f5f9 !important; background: #1a1f2e !important; }
[data-testid="stSidebarNav"] a[aria-current="page"] {
    color: #60a5fa !important;
    background: #1a1f2e !important;
    font-weight: 600 !important;
}

/* ── st.metric ─────────────────────────────────────────────────────────────── */
[data-testid="metric-container"] {
    background: #13161f;
    border: 1px solid #1e2438;
    border-radius: 12px;
    padding: 1rem 1.2rem;
}
[data-testid="metric-container"] label {
    color: #94a3b8 !important;
    font-size: 0.72rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.8px !important;
    text-transform: uppercase !important;
}
[data-testid="stMetricValue"] {
    color: #f8fafc !important;
    font-size: 1.65rem !important;
    font-weight: 700 !important;
}
[data-testid="stMetricDelta"] { font-size: 0.8rem !important; }

/* ── DataFrames ────────────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    border: 1px solid #1e2438 !important;
    border-radius: 10px !important;
}

/* ── Expanders ─────────────────────────────────────────────────────────────── */
details summary {
    background: #13161f !important;
    color: #f1f5f9 !important;
    border: 1px solid #1e2438 !important;
    border-radius: 8px !important;
    padding: 10px 16px !important;
}

/* ── Buttons ───────────────────────────────────────────────────────────────── */
.stButton > button {
    background: #1e2438 !important;
    color: #f1f5f9 !important;
    border: 1px solid #2d3a50 !important;
    border-radius: 8px !important;
    font-weight: 500 !important;
}
.stButton > button:hover {
    background: #2d3a50 !important;
    border-color: #3b82f6 !important;
}

/* ── Select / multiselect labels ───────────────────────────────────────────── */
.stSelectbox label, .stMultiSelect label, .stSlider label { color: #e2e8f0 !important; font-weight: 500 !important; }

/* ── Progress bar ──────────────────────────────────────────────────────────── */
.stProgress > div > div { background: #3b82f6 !important; }

/* ── Divider ───────────────────────────────────────────────────────────────── */
hr { border-color: #1e2438 !important; margin: 1.5rem 0 !important; }

/* ── Caption ───────────────────────────────────────────────────────────────── */
.stCaption, small { color: #64748b !important; }

/* ── Info / warning / error boxes ──────────────────────────────────────────── */
.stInfo    { background: #0c1a2e !important; border-left: 3px solid #3b82f6 !important; color: #bfdbfe !important; }
.stWarning { background: #1c1200 !important; border-left: 3px solid #f59e0b !important; color: #fde68a !important; }
.stError   { background: #1c0808 !important; border-left: 3px solid #ef4444 !important; color: #fca5a5 !important; }
.stSuccess { background: #061810 !important; border-left: 3px solid #22c55e !important; color: #86efac !important; }

/* ── Tabs ──────────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    background: #13161f;
    border-radius: 10px;
    padding: 4px;
    gap: 4px;
}
.stTabs [data-baseweb="tab"] { color: #94a3b8 !important; border-radius: 7px; }
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    color: #f1f5f9 !important;
    background: #1e2438 !important;
}

/* ── Custom components ─────────────────────────────────────────────────────── */
.arch-box {
    background: #13161f;
    border: 1px solid #1e2438;
    border-radius: 12px;
    padding: 22px 26px;
    font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace;
    color: #60a5fa;
    font-size: 13px;
    line-height: 2.1;
    white-space: pre;
    box-shadow: 0 4px 20px rgba(0,0,0,0.3);
}
.badge-ok   { color: #22c55e; font-weight: 600; }
.badge-miss { color: #ef4444; font-weight: 600; }

/* ── Spinner ───────────────────────────────────────────────────────────────── */
.stSpinner { color: #3b82f6 !important; }
</style>
"""


def inject_css() -> None:
    st.markdown(PAGE_CSS, unsafe_allow_html=True)
