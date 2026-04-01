"""
5_Quality_Checks.py
────────────────────
Data quality report viewer.
Reads the quality_report written by DataQualityChecker into data/gold/.
Shows PASS / WARN / FAIL per rule with charts.
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils import get_layout, inject_css, load_table

st.set_page_config(
    page_title="Quality Checks | NexCart",
    page_icon="[NC]",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

st.markdown("<h1 style='color:#f8fafc'>Data Quality Checks</h1>", unsafe_allow_html=True)
st.markdown(
    "<p style='color:#94a3b8;font-size:14px;margin-top:-6px'>"
    "Validation results for Silver-layer data quality rules &mdash; "
    "nulls, duplicates, row counts, and enumerations"
    "</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

# ── Load quality report ───────────────────────────────────────────────────────
with st.spinner("Loading quality report..."):
    dq = load_table("gold", "quality_report")

if dq is None or dq.empty:
    st.info(
        "Quality report not found.  \n"
        "Run the quality step to generate it:  \n"
        "```\npython scripts/run_pipeline_audited.py --layer quality\n```"
    )
    st.stop()

# ── Summary cards ──────────────────────────────────────────────────────────────
total  = len(dq)
passed = int((dq["status"] == "PASS").sum())
warned = int((dq["status"] == "WARN").sum())
failed = int((dq["status"] == "FAIL").sum())
pass_rate = passed / total * 100 if total else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Rules", str(total))
c2.metric("Passed",      str(passed))
c3.metric("Warnings",    str(warned))
c4.metric("Failed",      str(failed))
c5.metric("Pass Rate",   f"{pass_rate:.1f}%")

if failed > 0:
    st.error(f"{failed} rule(s) failed validation — review details below.")
elif warned > 0:
    st.warning(f"{warned} rule(s) raised a warning.")
else:
    st.success("All validation rules passed.")

st.markdown("---")

# ── Charts ────────────────────────────────────────────────────────────────────
CLR = {"PASS": "#22c55e", "WARN": "#f59e0b", "FAIL": "#ef4444"}

col1, col2 = st.columns(2)

with col1:
    status_counts = dq["status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]
    fig = px.pie(status_counts, names="status", values="count",
                 color="status", color_discrete_map=CLR,
                 hole=0.45, title="Rule Status Distribution")
    fig.update_layout(**get_layout(height=300))
    st.plotly_chart(fig, use_container_width=True)

with col2:
    if "table" in dq.columns:
        by_table = (
            dq.groupby(["table", "status"]).size()
            .reset_index(name="count")
        )
        fig = px.bar(by_table, x="table", y="count", color="status",
                     color_discrete_map=CLR,
                     title="Validation Rules by Table",
                     labels={"count": "Rules", "table": "", "status": "Status"})
        fig.update_layout(**get_layout(height=300, xaxis=dict(tickangle=20)))
        st.plotly_chart(fig, use_container_width=True)

# ── Rule type breakdown ───────────────────────────────────────────────────────
if "rule_name" in dq.columns:
    by_rule = dq["rule_name"].value_counts().reset_index()
    by_rule.columns = ["rule", "count"]
    fig = px.bar(by_rule, x="rule", y="count",
                 color_discrete_sequence=["#60a5fa"],
                 title="Rule Type Breakdown",
                 labels={"count": "Count", "rule": ""})
    fig.update_layout(**get_layout(showlegend=False, height=250))
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Filters + full table ──────────────────────────────────────────────────────
st.subheader("Quality Rule Details")

filter_col1, filter_col2 = st.columns(2)
with filter_col1:
    status_filter = st.multiselect(
        "Filter by status",
        options=["PASS", "WARN", "FAIL"],
        default=["WARN", "FAIL"],
    )
with filter_col2:
    table_options = ["All"] + sorted(dq["table"].unique().tolist()) if "table" in dq.columns else ["All"]
    table_filter  = st.selectbox("Filter by table", table_options)

filtered = dq.copy()
if status_filter:
    filtered = filtered[filtered["status"].isin(status_filter)]
if table_filter != "All" and "table" in filtered.columns:
    filtered = filtered[filtered["table"] == table_filter]


def _style_status(val):
    colors = {"PASS": "#22c55e", "WARN": "#f59e0b", "FAIL": "#ef4444"}
    color = colors.get(val, "")
    return f"color: {color}; font-weight: 600" if color else ""


display_cols = [c for c in ["table", "rule_name", "column", "status",
                             "metric", "threshold", "message", "checked_at"]
                if c in filtered.columns]

if not filtered.empty:
    styled = filtered[display_cols].style.applymap(_style_status, subset=["status"])
    st.dataframe(styled, use_container_width=True, hide_index=True)
    st.caption(f"Showing {len(filtered):,} of {len(dq):,} rules")
else:
    st.info("No rules match the current filters.")

# ── Most recent run timestamp ─────────────────────────────────────────────────
if "checked_at" in dq.columns:
    latest = pd.to_datetime(dq["checked_at"], errors="coerce").max()
    if pd.notna(latest):
        st.caption(f"Last quality run: {latest.strftime('%Y-%m-%d %H:%M:%S UTC')}")
