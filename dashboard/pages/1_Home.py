"""
1_Home.py
─────────
Project overview, layer status, and table inventory.
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils import (
    get_layout, get_table_inventory,
    inject_css, metric_card,
)

st.set_page_config(
    page_title="Home | NexCart Lakehouse",
    page_icon="[NC]",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

st.markdown("<h1 style='color:#f8fafc'>Project Overview</h1>", unsafe_allow_html=True)
st.markdown(
    "<p style='color:#94a3b8;font-size:14px;margin-top:-6px'>"
    "End-to-end data lakehouse built on PySpark, Parquet, and a medallion architecture"
    "</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

# ── Architecture ───────────────────────────────────────────────────────────────
col_arch, col_stack = st.columns([3, 2])

with col_arch:
    st.subheader("Pipeline Architecture")
    st.markdown("""
<div class="arch-box">
  [Raw CSV]
      |
      v   schema enforcement + audit columns
  [Bronze]  7 tables  (customers, products, orders ...)
      |
      v   type casting + business rules + deduplication
  [Silver]  7 tables  (orders_enriched, customers_clean ...)
      |
      +---> [Gold]     5 KPI tables    (daily_revenue, customer_ltv ...)
      |
      +---> [Features] 4 feature sets  (customer_features, order_risk ...)
      |
      +---> [Quality]  Quality Report  (DQ rules across Silver layer)
</div>
""", unsafe_allow_html=True)

with col_stack:
    st.subheader("Tech Stack")
    stack = {
        "Processing":    "PySpark 3.5",
        "Storage":       "Parquet (Snappy compressed)",
        "Orchestration": "Apache Airflow 2.9",
        "Audit Log":     "PostgreSQL",
        "Dashboard":     "Streamlit + Plotly",
        "Query Engine":  "DuckDB + PyArrow",
        "Data Synthesis": "Faker + Pandas",
        "Data Quality":  "Custom DQ Framework",
    }
    for k, v in stack.items():
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;"
            f"align-items:center;padding:8px 0;border-bottom:1px solid #1e2438'>"
            f"<span style='color:#94a3b8;font-size:13px'>{k}</span>"
            f"<span style='color:#60a5fa;font-weight:600;font-size:13px'>{v}</span></div>",
            unsafe_allow_html=True,
        )

st.markdown("---")

# ── Layer status ───────────────────────────────────────────────────────────────
st.subheader("Data Layer Inventory")
st.markdown(
    "<p style='color:#94a3b8;font-size:13px;margin-top:-8px'>"
    "Availability and storage footprint of all pipeline layers"
    "</p>",
    unsafe_allow_html=True,
)

inventory = get_table_inventory()
inv_df    = pd.DataFrame(inventory)

layer_cols = st.columns(4)
for i, layer in enumerate(["bronze", "silver", "gold", "features"]):
    sub       = inv_df[inv_df["layer"] == layer]
    available = int(sub["exists"].sum())
    total     = len(sub)
    files     = int(sub["parquet_files"].sum())
    size_mb   = sub["size_mb"].sum()
    with layer_cols[i]:
        st.markdown(
            metric_card(
                layer.upper(),
                f"{available}/{total} tables",
                f"{files} files  |  {size_mb:.1f} MB",
            ),
            unsafe_allow_html=True,
        )

st.markdown("")

# Detailed table grid
for layer in ["bronze", "silver", "gold", "features"]:
    sub = inv_df[inv_df["layer"] == layer].copy()
    with st.expander(
        f"{layer.upper()} — {int(sub['exists'].sum())}/{len(sub)} tables available",
        expanded=(layer in ("gold", "features")),
    ):
        sub["status"]        = sub["exists"].map({True: "OK", False: "MISSING"})
        sub["size_mb"]       = sub["size_mb"].astype(str) + " MB"
        sub["parquet_files"] = sub["parquet_files"].astype(str) + " files"
        display_cols = ["table", "status", "parquet_files", "size_mb", "last_modified"]
        st.dataframe(
            sub[display_cols].rename(columns={
                "table":         "Table",
                "status":        "Status",
                "parquet_files": "File Count",
                "size_mb":       "Size",
                "last_modified": "Last Modified",
            }),
            use_container_width=True,
            hide_index=True,
        )

st.markdown("---")

# ── Table availability bar chart ───────────────────────────────────────────────
st.subheader("Dataset Availability by Layer")
avail = (
    inv_df.groupby("layer")
    .agg(available=("exists", "sum"), total=("exists", "count"))
    .reset_index()
)
avail["missing"] = avail["total"] - avail["available"]

fig = px.bar(
    avail, x="layer",
    y=["available", "missing"],
    barmode="stack",
    color_discrete_map={"available": "#22c55e", "missing": "#ef4444"},
    labels={"value": "Tables", "layer": "Layer", "variable": ""},
)
fig.update_layout(**get_layout(height=280, showlegend=True))
st.plotly_chart(fig, use_container_width=True)
