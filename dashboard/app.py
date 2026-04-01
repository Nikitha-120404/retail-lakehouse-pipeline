"""
app.py
──────
NexCart Lakehouse — Streamlit Dashboard entry point / landing page.

Run from the project root:
    streamlit run dashboard/app.py
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import (
    get_layout, fmt_currency, fmt_num, fmt_pct,
    get_table_inventory, inject_css, load_table, metric_card,
)

st.set_page_config(
    page_title="NexCart Lakehouse",
    page_icon="[NC]",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown(
    "<h1 style='color:#f8fafc'>NexCart Lakehouse Analytics</h1>",
    unsafe_allow_html=True,
)
st.markdown(
    "<p style='color:#94a3b8;font-size:15px;margin-top:-8px'>"
    "End-to-end data lakehouse &mdash; Bronze &rarr; Silver &rarr; Gold &rarr; Features"
    "</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

# ── Architecture diagram ────────────────────────────────────────────────────────
st.subheader("Pipeline Architecture")
st.markdown("""
<div class="arch-box">
  Raw CSV  --[schema enforcement + audit]-->  Bronze  --[cleanse + enrich]-->  Silver
           --[KPI aggregations]-->  Gold  --[ML-ready features]-->  Features
                                    --[quality checks]-->  Quality Report
</div>
""", unsafe_allow_html=True)

st.markdown("")

# ── Layer status cards ──────────────────────────────────────────────────────────
inventory = get_table_inventory()
inv_df    = pd.DataFrame(inventory)

layers = ["bronze", "silver", "gold", "features"]
cols   = st.columns(4)
for i, layer in enumerate(layers):
    sub       = inv_df[inv_df["layer"] == layer]
    available = sub["exists"].sum()
    total     = len(sub)
    size_mb   = sub["size_mb"].sum()
    with cols[i]:
        st.markdown(
            metric_card(
                f"{layer.upper()} Layer",
                f"{available}/{total} tables",
                f"{size_mb:.1f} MB on disk",
            ),
            unsafe_allow_html=True,
        )

st.markdown("---")

# ── Business KPIs ───────────────────────────────────────────────────────────────
st.subheader("Business KPIs at a Glance")

daily_rev = load_table("gold", "daily_revenue")
cust_ltv  = load_table("gold", "customer_ltv")
ship_kpis = load_table("gold", "fulfillment_kpis")

if daily_rev is not None and not daily_rev.empty:
    total_rev    = daily_rev["gross_revenue"].sum()
    total_orders = daily_rev["total_orders"].sum()
    avg_aov      = daily_rev["avg_order_value"].mean()
    n_customers  = len(cust_ltv) if cust_ltv is not None else 0
    on_time      = (
        ship_kpis["on_time_rate_pct"].mean()
        if ship_kpis is not None and not ship_kpis.empty else 0
    )

    kpi_cols = st.columns(5)
    kpis = [
        ("Total Revenue",       fmt_currency(total_rev)),
        ("Total Orders",        fmt_num(total_orders)),
        ("Average Order Value", fmt_currency(avg_aov)),
        ("Total Customers",     fmt_num(n_customers)),
        ("On-Time Delivery",    fmt_pct(on_time)),
    ]
    for col, (label, value) in zip(kpi_cols, kpis):
        with col:
            st.markdown(metric_card(label, value), unsafe_allow_html=True)

    st.markdown("")

    # Revenue sparkline
    if "order_date" in daily_rev.columns:
        daily_rev["order_date"] = pd.to_datetime(daily_rev["order_date"])
        trend = (
            daily_rev.groupby("order_date")["gross_revenue"]
            .sum().reset_index().sort_values("order_date")
        )
        fig = px.area(
            trend, x="order_date", y="gross_revenue",
            color_discrete_sequence=["#3b82f6"],
            labels={"gross_revenue": "Revenue ($)", "order_date": ""},
            title="Daily Revenue Trend",
        )
        fig.update_traces(fillcolor="rgba(59,130,246,0.12)", line=dict(width=2))
        fig.update_layout(**get_layout(showlegend=False, height=300))
        st.plotly_chart(fig, use_container_width=True)
else:
    st.error(
        "Could not load data from PostgreSQL.  \n"
        "Ensure the database is running at **localhost:5433** and the gold tables exist."
    )

st.markdown("---")
st.caption("Navigate using the sidebar  |  Powered by PySpark, Parquet, Streamlit, and Plotly")
