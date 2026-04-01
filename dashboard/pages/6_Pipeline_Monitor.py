"""
6_Pipeline_Monitor.py
──────────────────────
Live monitor for local pipeline output folders.
Shows every known table: layer, existence, file count, size, last modified.
Optionally reads row counts (slower — requires pyarrow scan).
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils import (
    LAYER_TABLES, get_layout,
    get_table_inventory, get_layer_path,
    inject_css, load_table,
)

st.set_page_config(
    page_title="Pipeline Monitor | NexCart",
    page_icon="[NC]",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

st.markdown("<h1 style='color:#f8fafc'>Pipeline Monitor</h1>", unsafe_allow_html=True)
st.markdown(
    "<p style='color:#94a3b8;font-size:14px;margin-top:-6px'>"
    "Operational view of dataset availability, file health, and recent pipeline activity"
    "</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

# ── Controls ──────────────────────────────────────────────────────────────────
ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 1])
with ctrl1:
    layer_filter = st.multiselect(
        "Filter by layer",
        options=list(LAYER_TABLES.keys()),
        default=list(LAYER_TABLES.keys()),
    )
with ctrl2:
    status_filter = st.multiselect(
        "Filter by status",
        options=["OK", "MISSING"],
        default=["OK", "MISSING"],
    )
with ctrl3:
    st.markdown("<br>", unsafe_allow_html=True)
    refresh = st.button("Refresh", use_container_width=True)

if refresh:
    st.cache_data.clear()
    st.rerun()

# ── Inventory ─────────────────────────────────────────────────────────────────
with st.spinner("Scanning pipeline outputs..."):
    inventory = get_table_inventory()

inv_df = pd.DataFrame(inventory)
inv_df["status"] = inv_df["exists"].map({True: "OK", False: "MISSING"})

# Apply filters
if layer_filter:
    inv_df = inv_df[inv_df["layer"].isin(layer_filter)]
if status_filter:
    inv_df = inv_df[inv_df["status"].isin(status_filter)]

# ── Summary cards ──────────────────────────────────────────────────────────────
all_inv = pd.DataFrame(get_table_inventory())
total_tables  = len(all_inv)
ok_tables     = int(all_inv["exists"].sum())
missing       = total_tables - ok_tables
total_size_mb = all_inv["size_mb"].sum()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Datasets", str(total_tables))
c2.metric("Available",      str(ok_tables))
c3.metric("Missing",        str(missing))
c4.metric("Total Size",     f"{total_size_mb:.1f} MB")

st.markdown("")

# ── Layer health bar ──────────────────────────────────────────────────────────
layer_health = (
    all_inv.groupby("layer")
    .agg(ok=("exists", "sum"), total=("exists", "count"), size_mb=("size_mb", "sum"))
    .reset_index()
)
layer_health["missing"] = layer_health["total"] - layer_health["ok"]
layer_health["pct"]     = (layer_health["ok"] / layer_health["total"] * 100).round(0)

fig = px.bar(
    layer_health, x="layer", y=["ok", "missing"],
    barmode="stack",
    color_discrete_map={"ok": "#22c55e", "missing": "#ef4444"},
    title="Dataset Availability by Layer",
    labels={"value": "Tables", "layer": "Layer", "variable": ""},
    text_auto=True,
)
fig.update_layout(**get_layout(height=260, showlegend=True))
st.plotly_chart(fig, use_container_width=True)

# ── Size distribution ─────────────────────────────────────────────────────────
available = all_inv[all_inv["exists"] & (all_inv["size_mb"] > 0)]
if not available.empty:
    fig = px.bar(
        available.sort_values("size_mb", ascending=False),
        x="table", y="size_mb", color="layer",
        color_discrete_sequence=px.colors.qualitative.Set2,
        title="Storage Footprint by Table (MB)",
        labels={"size_mb": "Size (MB)", "table": "", "layer": "Layer"},
    )
    fig.update_layout(**get_layout(height=280, xaxis=dict(tickangle=45)))
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Detailed table grid ────────────────────────────────────────────────────────
st.subheader("Dataset Inventory")

show_df = inv_df[["layer", "table", "status", "parquet_files",
                   "size_mb", "last_modified"]].copy()
show_df.columns = ["Layer", "Table", "Status", "File Count", "Size (MB)", "Last Modified"]


def _style_status(val):
    if val == "OK":
        return "color: #22c55e; font-weight: 600"
    if val == "MISSING":
        return "color: #ef4444; font-weight: 600"
    return ""


styled = show_df.style.applymap(_style_status, subset=["Status"])
st.dataframe(styled, use_container_width=True, hide_index=True)
st.caption(f"Showing {len(show_df)} datasets")

# ── Optional row counts ────────────────────────────────────────────────────────
st.markdown("---")
st.subheader("Dataset Row Count Scanner")
st.markdown(
    "<p style='color:#94a3b8;font-size:13px;margin-top:-8px'>"
    "Reads each Parquet file to compute exact row counts. Select a layer to scan."
    "</p>",
    unsafe_allow_html=True,
)

scan_layer = st.selectbox("Layer to scan", ["— skip —"] + list(LAYER_TABLES.keys()))
if scan_layer != "— skip —":
    tables_in_layer = [
        t for t in LAYER_TABLES[scan_layer]
        if (get_layer_path(scan_layer) / t).exists()
    ]
    if not tables_in_layer:
        st.info(f"No available tables found in the {scan_layer} layer.")
    else:
        results = []
        progress = st.progress(0)
        for i, tbl in enumerate(tables_in_layer):
            df_tbl = load_table(scan_layer, tbl)
            results.append({
                "Table": tbl,
                "Rows":  f"{len(df_tbl):,}" if df_tbl is not None else "N/A",
                "Cols":  str(len(df_tbl.columns)) if df_tbl is not None else "N/A",
            })
            progress.progress((i + 1) / len(tables_in_layer))
        progress.empty()
        st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

# ── Audit log tail ─────────────────────────────────────────────────────────────
st.markdown("---")
st.subheader("Recent Pipeline Activity")

log_path = Path(__file__).resolve().parents[2] / "logs" / "pipeline_runs.jsonl"
if log_path.exists():
    try:
        import json
        lines = log_path.read_text(encoding="utf-8").strip().split("\n")
        records = [json.loads(ln) for ln in lines if ln.strip()][-30:]
        log_df = pd.DataFrame(records)
        if not log_df.empty:
            display_cols = [c for c in ["timestamp", "layer", "table",
                                         "status", "duration_s", "error"]
                            if c in log_df.columns]

            col_rename = {
                "timestamp":  "Timestamp",
                "layer":      "Layer",
                "table":      "Table",
                "status":     "Status",
                "duration_s": "Duration (s)",
                "error":      "Error Message",
            }

            def _style_log(val):
                if val == "success": return "color:#22c55e;font-weight:600"
                if val == "failed":  return "color:#ef4444;font-weight:600"
                if val == "running": return "color:#f59e0b;font-weight:600"
                return ""

            display_df = log_df[display_cols].rename(
                columns={k: v for k, v in col_rename.items() if k in display_cols}
            )
            styled_log = display_df.style.applymap(
                _style_log, subset=["Status"] if "Status" in display_df.columns else []
            )
            st.dataframe(styled_log, use_container_width=True, hide_index=True)
            st.caption(f"Showing the {len(records)} most recent pipeline runs")
    except Exception as exc:
        st.warning(f"Could not parse pipeline log: {exc}")
else:
    st.info(
        "No pipeline activity log found.  \n"
        "Run the pipeline to generate it: `python scripts/run_pipeline_audited.py`"
    )
