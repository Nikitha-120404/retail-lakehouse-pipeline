"""
2_Executive_Overview.py
────────────────────────
C-suite KPI dashboard: revenue, customers, fulfillment.
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils import (
    get_layout, fmt_currency, fmt_num, fmt_pct,
    inject_css, load_table, metric_card,
)

st.set_page_config(
    page_title="Executive Overview | NexCart",
    page_icon="[NC]",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

st.markdown("<h1 style='color:#f8fafc'>Executive Overview</h1>", unsafe_allow_html=True)
st.markdown(
    "<p style='color:#94a3b8;font-size:14px;margin-top:-6px'>"
    "Top-line business metrics and operational performance across the platform"
    "</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

# ── Load gold tables ───────────────────────────────────────────────────────────
with st.spinner("Loading analytics data..."):
    daily_rev = load_table("gold", "daily_revenue")
    cust_ltv  = load_table("gold", "customer_ltv")
    ship_kpis = load_table("gold", "fulfillment_kpis")
    prod_perf = load_table("gold", "product_performance")

if daily_rev is None or daily_rev.empty:
    st.error(
        "Could not load data from PostgreSQL.  \n"
        "Ensure the database is running at **localhost:5433** and the gold tables exist."
    )
    st.stop()

daily_rev["order_date"] = pd.to_datetime(daily_rev["order_date"], errors="coerce")

# ── KPI cards ──────────────────────────────────────────────────────────────────
total_rev    = daily_rev["gross_revenue"].sum()
total_orders = daily_rev["total_orders"].sum()
avg_aov      = daily_rev["avg_order_value"].mean()
net_rev      = daily_rev["net_revenue"].sum()
total_disc   = daily_rev["total_discounts"].sum()
disc_rate    = (total_disc / total_rev * 100) if total_rev else 0
n_customers  = len(cust_ltv) if cust_ltv is not None else 0
churn_rate   = (cust_ltv["is_churned"].mean() * 100) if cust_ltv is not None and "is_churned" in cust_ltv.columns else 0
on_time      = ship_kpis["on_time_rate_pct"].mean() if ship_kpis is not None and not ship_kpis.empty else 0

kpi_cols = st.columns(5)
kpis = [
    ("Gross Revenue",       fmt_currency(total_rev)),
    ("Net Revenue",         fmt_currency(net_rev)),
    ("Total Orders",        fmt_num(total_orders)),
    ("Average Order Value", fmt_currency(avg_aov)),
    ("Total Customers",     fmt_num(n_customers)),
]
for col, (label, val) in zip(kpi_cols, kpis):
    with col:
        st.markdown(metric_card(label, val), unsafe_allow_html=True)

st.markdown("")
kpi_cols2 = st.columns(5)
kpis2 = [
    ("Discount Rate",       fmt_pct(disc_rate)),
    ("Total Discounts",     fmt_currency(total_disc)),
    ("On-Time Delivery",    fmt_pct(on_time)),
    ("Customer Churn Rate", fmt_pct(churn_rate)),
    ("Paying Customers",    fmt_num(len(cust_ltv[cust_ltv["total_orders"] > 0])) if cust_ltv is not None else "N/A"),
]
for col, (label, val) in zip(kpi_cols2, kpis2):
    with col:
        st.markdown(metric_card(label, val), unsafe_allow_html=True)

st.markdown("---")

# ── Revenue Trend + Channel Split ──────────────────────────────────────────────
col_l, col_r = st.columns([3, 1])

with col_l:
    st.subheader("Revenue Trend")
    trend = (
        daily_rev.groupby("order_date")[["gross_revenue", "net_revenue"]]
        .sum().reset_index().sort_values("order_date")
    )
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend["order_date"], y=trend["gross_revenue"],
        name="Gross Revenue", fill="tozeroy",
        line=dict(color="#3b82f6", width=2),
        fillcolor="rgba(59,130,246,0.12)",
    ))
    fig.add_trace(go.Scatter(
        x=trend["order_date"], y=trend["net_revenue"],
        name="Net Revenue", line=dict(color="#22c55e", width=1.5, dash="dot"),
    ))
    fig.update_layout(**get_layout(title="Daily Revenue Trend", height=300))
    st.plotly_chart(fig, use_container_width=True)

with col_r:
    st.subheader("By Channel")
    if "channel" in daily_rev.columns:
        by_ch = daily_rev.groupby("channel")["gross_revenue"].sum().reset_index()
        fig = px.pie(by_ch, names="channel", values="gross_revenue",
                     color_discrete_sequence=px.colors.sequential.Blues_r, hole=0.45,
                     title="Revenue by Sales Channel")
        fig.update_layout(**get_layout(margin=dict(t=40, b=10), height=300))
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Monthly Revenue + Segment ───────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Monthly Performance")
    monthly = (
        daily_rev.assign(month=lambda x: x["order_date"].dt.to_period("M").astype(str))
        .groupby("month")[["gross_revenue", "net_revenue", "total_discounts"]]
        .sum().reset_index()
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Gross Revenue", x=monthly["month"],
                         y=monthly["gross_revenue"], marker_color="#3b82f6"))
    fig.add_trace(go.Bar(name="Net Revenue",   x=monthly["month"],
                         y=monthly["net_revenue"],   marker_color="#22c55e"))
    fig.update_layout(**get_layout(
        title="Monthly Revenue & Discount Trends",
        barmode="group", height=320,
        xaxis=dict(tickangle=45),
    ))
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Revenue by Segment")
    if "customer_segment" in daily_rev.columns:
        by_seg = (
            daily_rev.groupby("customer_segment")["gross_revenue"]
            .sum().reset_index().sort_values("gross_revenue", ascending=True)
        )
        fig = px.bar(by_seg, x="gross_revenue", y="customer_segment",
                     orientation="h", color="gross_revenue",
                     color_continuous_scale="Blues",
                     title="Revenue by Customer Segment",
                     labels={"gross_revenue": "Revenue ($)", "customer_segment": ""})
        fig.update_layout(**get_layout(coloraxis_showscale=False, height=320))
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Fulfillment + Weekend analysis ─────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Fulfillment Performance")
    if ship_kpis is not None and not ship_kpis.empty and "carrier" in ship_kpis.columns:
        carrier = (
            ship_kpis.groupby("carrier")
            .agg(total=("total_shipments", "sum"), on_time=("on_time_shipments", "sum"))
            .reset_index()
        )
        carrier["rate"] = (carrier["on_time"] / carrier["total"] * 100).round(1)
        fig = px.bar(
            carrier.sort_values("rate"),
            x="rate", y="carrier", orientation="h",
            color="rate", color_continuous_scale="RdYlGn", range_color=[0, 100],
            title="Carrier On-Time Delivery Rate",
            labels={"rate": "On-Time %", "carrier": ""},
        )
        fig.update_layout(**get_layout(coloraxis_showscale=False, height=280))
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Order Timing")
    if "is_weekend" in daily_rev.columns:
        wk = (
            daily_rev.groupby("is_weekend")[["gross_revenue", "total_orders"]]
            .sum().reset_index()
        )
        wk["label"] = wk["is_weekend"].map(
            {True: "Weekend", False: "Weekday", 1: "Weekend", 0: "Weekday"}
        )
        fig = px.pie(wk, names="label", values="gross_revenue",
                     color_discrete_sequence=["#3b82f6", "#22c55e"], hole=0.45,
                     title="Weekday vs Weekend Revenue Split")
        fig.update_layout(**get_layout(height=280))
        st.plotly_chart(fig, use_container_width=True)

# ── LTV distribution ───────────────────────────────────────────────────────────
if cust_ltv is not None and not cust_ltv.empty and "ltv_tier" in cust_ltv.columns:
    st.markdown("---")
    st.subheader("Customer Lifetime Value Distribution")
    tier_counts = cust_ltv["ltv_tier"].value_counts().reset_index()
    tier_counts.columns = ["tier", "count"]
    ORDER = {"top": 4, "high": 3, "medium": 2, "low": 1, "no_purchase": 0}
    tier_counts["ord"] = tier_counts["tier"].map(ORDER).fillna(-1)
    tier_counts = tier_counts.sort_values("ord", ascending=False)
    TIER_COLORS = {"top": "#f59e0b", "high": "#3b82f6", "medium": "#22c55e",
                   "low": "#a78bfa", "no_purchase": "#475569"}
    fig = px.bar(tier_counts, x="tier", y="count",
                 color="tier", color_discrete_map=TIER_COLORS,
                 title="Customer Lifetime Value Tiers",
                 labels={"count": "Customers", "tier": "LTV Tier"})
    fig.update_layout(**get_layout(showlegend=False, height=300))
    st.plotly_chart(fig, use_container_width=True)
