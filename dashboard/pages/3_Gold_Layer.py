"""
3_Gold_Layer.py
───────────────
Interactive explorer for all 5 Gold tables.
Select a table to see preview, schema, row count, and tailored charts.
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils import get_layout, inject_css, load_table

st.set_page_config(
    page_title="Gold Layer | NexCart",
    page_icon="[NC]",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

st.markdown("<h1 style='color:#f8fafc'>Gold Layer Explorer</h1>", unsafe_allow_html=True)
st.markdown(
    "<p style='color:#94a3b8;font-size:14px;margin-top:-6px'>"
    "Curated analytics tables built from the Silver layer &mdash; select a table to explore"
    "</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

GOLD_TABLES = [
    "daily_revenue",
    "customer_ltv",
    "product_performance",
    "inventory_health",
    "fulfillment_kpis",
]

selected = st.selectbox("Select Table", GOLD_TABLES)

with st.spinner(f"Loading {selected}..."):
    df = load_table("gold", selected)

if df is None or df.empty:
    st.error(
        f"Could not load **{selected}** from PostgreSQL.  \n"
        "Ensure the database is running at **localhost:5433** and the table exists."
    )
    st.stop()

# ── Meta row ──────────────────────────────────────────────────────────────────
m1, m2, m3 = st.columns(3)
m1.metric("Rows",    f"{len(df):,}")
m2.metric("Columns", str(len(df.columns)))
m3.metric("Table",   selected)

# ── Schema ────────────────────────────────────────────────────────────────────
with st.expander("Schema & Column Types"):
    schema_df = pd.DataFrame({
        "Column": df.columns.tolist(),
        "Type":   [str(t) for t in df.dtypes.tolist()],
        "Nulls":  [int(df[c].isna().sum()) for c in df.columns],
        "Null %": [f"{df[c].isna().mean()*100:.1f}%" for c in df.columns],
    })
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

# ── Data preview ──────────────────────────────────────────────────────────────
with st.expander("Data Preview — First 50 Rows", expanded=False):
    st.dataframe(df.head(50), use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# Table-specific charts
# ══════════════════════════════════════════════════════════════════════════════

# ── daily_revenue ─────────────────────────────────────────────────────────────
if selected == "daily_revenue":
    st.subheader("Revenue Analytics")
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    trend = df.groupby("order_date")[["gross_revenue", "net_revenue"]].sum().reset_index()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=trend["order_date"], y=trend["gross_revenue"],
                             name="Gross Revenue", fill="tozeroy",
                             line=dict(color="#3b82f6", width=2),
                             fillcolor="rgba(59,130,246,0.12)"))
    fig.add_trace(go.Scatter(x=trend["order_date"], y=trend["net_revenue"],
                             name="Net Revenue", line=dict(color="#22c55e", width=1.5, dash="dot")))
    fig.update_layout(**get_layout(title="Daily Revenue Trend", height=320))
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        monthly = (df.assign(month=lambda x: x["order_date"].dt.to_period("M").astype(str))
                   .groupby("month")[["gross_revenue", "total_discounts"]].sum().reset_index())
        fig = px.bar(monthly, x="month", y=["gross_revenue", "total_discounts"],
                     barmode="group",
                     color_discrete_map={"gross_revenue": "#3b82f6", "total_discounts": "#ef4444"},
                     labels={"value": "$", "month": "Month", "variable": ""},
                     title="Monthly Revenue vs Discounts")
        fig.update_layout(**get_layout(height=300, xaxis=dict(tickangle=45)))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "channel" in df.columns:
            by_ch = df.groupby("channel")[["gross_revenue", "total_orders"]].sum().reset_index()
            fig = px.bar(by_ch, x="channel", y="gross_revenue",
                         color="channel",
                         color_discrete_sequence=px.colors.qualitative.Set2,
                         title="Revenue by Sales Channel",
                         labels={"gross_revenue": "Revenue ($)", "channel": ""})
            fig.update_layout(**get_layout(showlegend=False, height=300))
            st.plotly_chart(fig, use_container_width=True)

    if "customer_segment" in df.columns and "discount_rate_pct" in df.columns:
        col1, col2 = st.columns(2)
        with col1:
            seg = df.groupby("customer_segment")["gross_revenue"].sum().reset_index()
            fig = px.pie(seg, names="customer_segment", values="gross_revenue",
                         hole=0.4, title="Revenue by Customer Segment",
                         color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_layout(**get_layout(height=300))
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            disc = df.groupby("channel")["discount_rate_pct"].mean().reset_index()
            fig = px.bar(disc, x="channel", y="discount_rate_pct",
                         color="discount_rate_pct", color_continuous_scale="Reds",
                         title="Average Discount Rate by Channel",
                         labels={"discount_rate_pct": "Avg Discount %", "channel": ""})
            fig.update_layout(**get_layout(coloraxis_showscale=False, height=300))
            st.plotly_chart(fig, use_container_width=True)

# ── customer_ltv ──────────────────────────────────────────────────────────────
elif selected == "customer_ltv":
    st.subheader("Customer Lifetime Value Analytics")

    buyers = df[df["total_orders"] > 0] if "total_orders" in df.columns else df

    col1, col2 = st.columns(2)
    with col1:
        if "ltv_tier" in df.columns:
            tier = df["ltv_tier"].value_counts().reset_index()
            tier.columns = ["tier", "count"]
            COLORS = {"top": "#f59e0b", "high": "#3b82f6", "medium": "#22c55e",
                      "low": "#a78bfa", "no_purchase": "#475569"}
            fig = px.bar(tier, x="tier", y="count", color="tier",
                         color_discrete_map=COLORS,
                         title="LTV Tier Distribution",
                         labels={"count": "Customers", "tier": ""})
            fig.update_layout(**get_layout(showlegend=False, height=320))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "total_spend" in buyers.columns:
            fig = px.histogram(buyers, x="total_spend", nbins=50,
                               color_discrete_sequence=["#60a5fa"],
                               title="Total Spend Distribution",
                               labels={"total_spend": "Total Spend ($)"})
            fig.update_layout(**get_layout(showlegend=False, height=320))
            st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        if "segment" in df.columns:
            seg = df["segment"].value_counts().reset_index()
            seg.columns = ["segment", "count"]
            SEG_CLR = {"bronze": "#cd7f32", "silver": "#c0c0c0",
                       "gold": "#ffd700", "platinum": "#e5e4e2", "unknown": "#6b7280"}
            fig = px.bar(seg, x="segment", y="count", color="segment",
                         color_discrete_map=SEG_CLR,
                         title="Customers by Segment",
                         labels={"count": "Customers", "segment": ""})
            fig.update_layout(**get_layout(showlegend=False, height=300))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "days_since_last_order" in buyers.columns and "total_spend" in buyers.columns:
            sample = buyers.dropna(subset=["days_since_last_order", "total_spend"]).head(2000)
            fig = px.scatter(sample, x="days_since_last_order", y="total_spend",
                             color="ltv_tier" if "ltv_tier" in sample.columns else None,
                             opacity=0.5, title="Recency vs Lifetime Spend",
                             labels={"days_since_last_order": "Days Since Last Order",
                                     "total_spend": "Total Spend ($)", "ltv_tier": "Tier"},
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_layout(**get_layout(height=300))
            st.plotly_chart(fig, use_container_width=True)

    if all(c in df.columns for c in ["days_since_last_order", "ltv_tier", "is_churned"]):
        st.subheader("High-Value Churn Risk")
        risk = df[
            (df["days_since_last_order"].fillna(999) > 60) &
            (df["ltv_tier"].isin(["top", "high"])) &
            (df["is_churned"].astype(str).isin(["False", "0", "false"]))
        ].sort_values("total_spend", ascending=False).head(20)
        if not risk.empty:
            cols = [c for c in ["customer_id", "full_name", "segment", "total_spend",
                                 "total_orders", "days_since_last_order", "ltv_tier"]
                    if c in risk.columns]
            st.dataframe(risk[cols], use_container_width=True, hide_index=True)
        else:
            st.info("No high-value churn risk customers found.")

# ── product_performance ───────────────────────────────────────────────────────
elif selected == "product_performance":
    st.subheader("Product Performance Analytics")

    top10 = (df.groupby(["product_id", "product_name", "category"])
             ["attributed_revenue"].sum()
             .reset_index()
             .sort_values("attributed_revenue", ascending=False)
             .head(10))
    fig = px.bar(top10, x="attributed_revenue", y="product_name", orientation="h",
                 color="category",
                 color_discrete_sequence=px.colors.qualitative.Set2,
                 title="Top 10 Products by Revenue",
                 labels={"attributed_revenue": "Revenue ($)", "product_name": ""})
    fig.update_layout(**get_layout(height=360))
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        by_cat = df.groupby("category")["attributed_revenue"].sum().reset_index()
        fig = px.pie(by_cat, names="category", values="attributed_revenue",
                     hole=0.4, title="Revenue by Category",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(**get_layout(height=320))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "margin_bucket" in df.columns:
            mb = df.groupby("margin_bucket")["attributed_revenue"].sum().reset_index()
            fig = px.pie(mb, names="margin_bucket", values="attributed_revenue",
                         hole=0.4, title="Revenue by Margin Bucket",
                         color_discrete_sequence=["#22c55e", "#60a5fa", "#ef4444"])
            fig.update_layout(**get_layout(height=320))
            st.plotly_chart(fig, use_container_width=True)

    if "year_month" in df.columns:
        monthly_cat = (df.groupby(["year_month", "category"])["attributed_revenue"]
                       .sum().reset_index().sort_values("year_month"))
        fig = px.line(monthly_cat, x="year_month", y="attributed_revenue",
                      color="category",
                      color_discrete_sequence=px.colors.qualitative.Set2,
                      title="Revenue Trend by Category",
                      labels={"attributed_revenue": "Revenue ($)", "year_month": "Month"})
        fig.update_layout(**get_layout(height=340, xaxis=dict(tickangle=45)))
        st.plotly_chart(fig, use_container_width=True)

# ── inventory_health ──────────────────────────────────────────────────────────
elif selected == "inventory_health":
    st.subheader("Inventory Health Analytics")

    col1, col2 = st.columns(2)
    with col1:
        if "overall_stock_status" in df.columns:
            status = df["overall_stock_status"].value_counts().reset_index()
            status.columns = ["status", "count"]
            CLR = {"healthy": "#22c55e", "low_stock": "#f59e0b", "out_of_stock": "#ef4444"}
            fig = px.pie(status, names="status", values="count",
                         color="status", color_discrete_map=CLR,
                         hole=0.45, title="Stock Status Distribution")
            fig.update_layout(**get_layout(height=320))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "category" in df.columns and "total_inventory_value" in df.columns:
            by_cat = df.groupby("category")["total_inventory_value"].sum().reset_index()
            fig = px.bar(by_cat.sort_values("total_inventory_value", ascending=True),
                         x="total_inventory_value", y="category", orientation="h",
                         color="total_inventory_value", color_continuous_scale="Blues",
                         title="Inventory Value by Category",
                         labels={"total_inventory_value": "Value ($)", "category": ""})
            fig.update_layout(**get_layout(coloraxis_showscale=False, height=320))
            st.plotly_chart(fig, use_container_width=True)

    if "overall_stock_status" in df.columns:
        alerts = df[df["overall_stock_status"].isin(["out_of_stock", "low_stock"])]
        st.subheader(f"Stock Alerts ({len(alerts):,} products)")
        if not alerts.empty:
            show_cols = [c for c in ["product_name", "category", "overall_stock_status",
                                     "total_qty_available", "total_qty_on_hand",
                                     "warehouses_needing_reorder", "total_inventory_value"]
                         if c in alerts.columns]
            st.dataframe(
                alerts[show_cols].sort_values("overall_stock_status").head(30),
                use_container_width=True, hide_index=True,
            )
        else:
            st.success("All products are fully stocked — no alerts.")

# ── fulfillment_kpis ──────────────────────────────────────────────────────────
elif selected == "fulfillment_kpis":
    st.subheader("Fulfillment Performance Analytics")

    if "week" in df.columns:
        df["week"] = pd.to_datetime(df["week"], errors="coerce")

    col1, col2 = st.columns(2)
    with col1:
        if "carrier" in df.columns:
            carrier = (df.groupby("carrier")
                       .agg(total=("total_shipments", "sum"),
                            on_time=("on_time_shipments", "sum"),
                            delayed=("delayed_shipments", "sum"))
                       .reset_index())
            carrier["on_time_rate"] = (carrier["on_time"] / carrier["total"] * 100).round(1)
            fig = px.bar(carrier.sort_values("on_time_rate"),
                         x="on_time_rate", y="carrier", orientation="h",
                         color="on_time_rate", color_continuous_scale="RdYlGn",
                         range_color=[0, 100],
                         title="On-Time Delivery Rate by Carrier",
                         labels={"on_time_rate": "On-Time %", "carrier": ""})
            fig.update_layout(**get_layout(coloraxis_showscale=False, height=300))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "carrier" in df.columns:
            fig = px.bar(carrier.sort_values("total", ascending=False),
                         x="carrier", y="total", color="carrier",
                         color_discrete_sequence=px.colors.qualitative.Set2,
                         title="Shipment Volume by Carrier",
                         labels={"total": "Shipments", "carrier": ""})
            fig.update_layout(**get_layout(showlegend=False, height=300))
            st.plotly_chart(fig, use_container_width=True)

    if "week" in df.columns:
        weekly = (df.groupby("week")[["total_shipments", "on_time_shipments", "delayed_shipments"]]
                  .sum().reset_index().sort_values("week"))
        fig = go.Figure()
        fig.add_trace(go.Bar(x=weekly["week"], y=weekly["on_time_shipments"],
                             name="On-Time", marker_color="#22c55e"))
        fig.add_trace(go.Bar(x=weekly["week"], y=weekly["delayed_shipments"],
                             name="Delayed", marker_color="#ef4444"))
        fig.update_layout(**get_layout(barmode="stack",
                                       title="Weekly Shipment Performance", height=320))
        st.plotly_chart(fig, use_container_width=True)

    if "avg_transit_days" in df.columns and "carrier" in df.columns:
        transit = df.groupby("carrier")["avg_transit_days"].mean().reset_index()
        fig = px.bar(transit.sort_values("avg_transit_days", ascending=False),
                     x="carrier", y="avg_transit_days",
                     color="avg_transit_days", color_continuous_scale="RdYlGn_r",
                     title="Average Transit Days by Carrier",
                     labels={"avg_transit_days": "Avg Days", "carrier": ""})
        fig.update_layout(**get_layout(coloraxis_showscale=False, height=280))
        st.plotly_chart(fig, use_container_width=True)
