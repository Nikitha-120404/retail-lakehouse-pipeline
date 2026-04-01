"""
4_Features_Layer.py
────────────────────
ML-ready feature table explorer with per-table visualisations
and a generic histogram builder.
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils import get_layout, fmt_num, fmt_pct, inject_css, load_table

st.set_page_config(
    page_title="Features Layer | NexCart",
    page_icon="[NC]",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

st.markdown("<h1 style='color:#f8fafc'>Features Layer Explorer</h1>", unsafe_allow_html=True)
st.markdown(
    "<p style='color:#94a3b8;font-size:14px;margin-top:-6px'>"
    "ML-ready feature sets engineered for downstream modeling and advanced analysis"
    "</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

FEATURE_TABLES = [
    "customer_features",
    "product_features",
    "order_risk_features",
    "delivery_features",
]

selected = st.selectbox("Select Feature Table", FEATURE_TABLES)

with st.spinner(f"Loading {selected}..."):
    df = load_table("features", selected)

if df is None or df.empty:
    st.warning(f"`{selected}` not found. Run the pipeline to generate it.")
    st.stop()

# ── Meta ──────────────────────────────────────────────────────────────────────
m1, m2, m3 = st.columns(3)
m1.metric("Rows",    f"{len(df):,}")
m2.metric("Columns", str(len(df.columns)))
m3.metric("Table",   selected)

with st.expander("Schema & Column Types"):
    schema_df = pd.DataFrame({
        "Column": df.columns.tolist(),
        "Type":   [str(t) for t in df.dtypes.tolist()],
        "Nulls":  [int(df[c].isna().sum()) for c in df.columns],
        "Null %": [f"{df[c].isna().mean()*100:.1f}%" for c in df.columns],
    })
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

with st.expander("Data Preview — First 50 Rows", expanded=False):
    st.dataframe(df.head(50), use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# Table-specific visualisations
# ══════════════════════════════════════════════════════════════════════════════

# ── customer_features ─────────────────────────────────────────────────────────
if selected == "customer_features":
    st.subheader("Customer Churn Feature Overview")

    if "is_churned" in df.columns:
        churn_rate = df["is_churned"].mean() * 100
        n_churned  = df["is_churned"].sum()

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Customers", fmt_num(len(df)))
        c2.metric("Churned",         fmt_num(n_churned))
        c3.metric("Churn Rate",      fmt_pct(churn_rate))
        st.markdown("")

        col1, col2 = st.columns(2)
        with col1:
            if "total_spend" in df.columns:
                lbl = df["is_churned"].map({1: "Churned", 0: "Active",
                                            True: "Churned", False: "Active"})
                fig = px.histogram(df, x="total_spend", color=lbl, nbins=40,
                                   barmode="overlay",
                                   color_discrete_map={"Churned": "#ef4444",
                                                       "Active":  "#22c55e"},
                                   title="Total Spend: Churned vs Active Customers",
                                   labels={"total_spend": "Total Spend ($)",
                                           "color": "Status"})
                fig.update_layout(**get_layout(showlegend=True, height=320))
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if "segment" in df.columns:
                seg_churn = (df.groupby("segment")["is_churned"]
                             .mean().mul(100).reset_index())
                seg_churn.columns = ["segment", "churn_rate"]
                fig = px.bar(seg_churn, x="segment", y="churn_rate",
                             color="churn_rate", color_continuous_scale="Reds",
                             title="Churn Rate by Customer Segment",
                             labels={"churn_rate": "Churn Rate (%)", "segment": ""})
                fig.update_layout(**get_layout(coloraxis_showscale=False, height=320))
                st.plotly_chart(fig, use_container_width=True)

    if "clicks_30d" in df.columns and "total_spend" in df.columns:
        sample = df.dropna(subset=["clicks_30d", "total_spend"]).head(2000)
        fig = px.scatter(sample, x="clicks_30d", y="total_spend",
                         color="is_churned" if "is_churned" in sample.columns else None,
                         opacity=0.5,
                         color_discrete_map={True: "#ef4444", False: "#22c55e",
                                             1: "#ef4444", 0: "#22c55e"},
                         title="Engagement vs Lifetime Spend",
                         labels={"clicks_30d": "Clicks (30 Days)",
                                 "total_spend": "Total Spend ($)"})
        fig.update_layout(**get_layout(height=300))
        st.plotly_chart(fig, use_container_width=True)

# ── product_features ──────────────────────────────────────────────────────────
elif selected == "product_features":
    st.subheader("Product Feature Overview")

    col1, col2 = st.columns(2)
    with col1:
        if "category" in df.columns and "orders_30d" in df.columns:
            cat = df.groupby("category")["orders_30d"].sum().reset_index()
            fig = px.bar(cat.sort_values("orders_30d", ascending=False),
                         x="category", y="orders_30d",
                         color="category",
                         color_discrete_sequence=px.colors.qualitative.Set2,
                         title="30-Day Order Volume by Category",
                         labels={"orders_30d": "Orders", "category": ""})
            fig.update_layout(**get_layout(showlegend=False, height=320))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "days_of_supply" in df.columns:
            clipped = df["days_of_supply"].clip(upper=200)
            fig = px.histogram(clipped, nbins=40,
                               color_discrete_sequence=["#60a5fa"],
                               title="Days of Supply Distribution",
                               labels={"value": "Days of Supply"})
            fig.update_layout(**get_layout(showlegend=False, height=320))
            st.plotly_chart(fig, use_container_width=True)

    if "velocity_trend_30_90" in df.columns and "orders_30d" in df.columns:
        top = df.nlargest(20, "orders_30d")[
            [c for c in ["product_name", "category", "orders_7d", "orders_30d",
                         "orders_90d", "velocity_trend_30_90", "days_of_supply"]
             if c in df.columns]
        ]
        st.subheader("Top 20 Products by 30-Day Order Velocity")
        st.dataframe(top, use_container_width=True, hide_index=True)

# ── order_risk_features ───────────────────────────────────────────────────────
elif selected == "order_risk_features":
    st.subheader("Order Risk & Anomaly Feature Overview")

    if "is_anomaly" in df.columns:
        anomaly_rate = df["is_anomaly"].mean() * 100
        n_anomaly    = df["is_anomaly"].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Orders",  fmt_num(len(df)))
        c2.metric("Anomalous",     fmt_num(n_anomaly))
        c3.metric("Anomaly Rate",  fmt_pct(anomaly_rate))
        st.markdown("")

        if "total_amount" in df.columns:
            lbl = df["is_anomaly"].map({1: "Anomaly", 0: "Normal",
                                        True: "Anomaly", False: "Normal"})
            fig = px.histogram(df, x="total_amount", color=lbl, nbins=60,
                               barmode="overlay",
                               color_discrete_map={"Anomaly": "#ef4444",
                                                   "Normal": "#60a5fa"},
                               title="Order Amount: Normal vs Anomalous",
                               labels={"total_amount": "Order Amount ($)"})
            fig.update_layout(**get_layout(height=320))
            st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        if "amount_z_score" in df.columns:
            fig = px.histogram(df, x="amount_z_score", nbins=60,
                               color_discrete_sequence=["#a78bfa"],
                               title="Order Amount Z-Score Distribution",
                               labels={"amount_z_score": "Z-Score"})
            fig.update_layout(**get_layout(showlegend=False, height=300))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "payment_lag_minutes" in df.columns:
            clipped = df["payment_lag_minutes"].clip(lower=0, upper=500)
            fig = px.histogram(clipped, nbins=50,
                               color_discrete_sequence=["#f59e0b"],
                               title="Payment Processing Lag (min)",
                               labels={"value": "Minutes"})
            fig.update_layout(**get_layout(showlegend=False, height=300))
            st.plotly_chart(fig, use_container_width=True)

# ── delivery_features ─────────────────────────────────────────────────────────
elif selected == "delivery_features":
    st.subheader("Delivery Delay Feature Overview")

    if "is_delayed" in df.columns:
        delay_rate = df["is_delayed"].mean() * 100
        n_delayed  = df["is_delayed"].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Shipments", fmt_num(len(df)))
        c2.metric("Delayed",         fmt_num(n_delayed))
        c3.metric("Delay Rate",      fmt_pct(delay_rate))
        st.markdown("")

    col1, col2 = st.columns(2)
    with col1:
        if "carrier_encoded" in df.columns and "carrier_ontime_rate" in df.columns:
            carrier_rate = df.groupby("carrier_encoded")["carrier_ontime_rate"].mean().reset_index()
            fig = px.bar(carrier_rate, x="carrier_encoded", y="carrier_ontime_rate",
                         color="carrier_ontime_rate", color_continuous_scale="RdYlGn",
                         title="Carrier On-Time Rate",
                         labels={"carrier_ontime_rate": "On-Time Rate",
                                 "carrier_encoded": "Carrier (encoded)"})
            fig.update_layout(**get_layout(coloraxis_showscale=False, height=300))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "delay_days" in df.columns:
            delayed = df[df["delay_days"] > 0]["delay_days"].clip(upper=30)
            fig = px.histogram(delayed, nbins=30,
                               color_discrete_sequence=["#ef4444"],
                               title="Delay Duration Distribution",
                               labels={"value": "Delay Days"})
            fig.update_layout(**get_layout(showlegend=False, height=300))
            st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# Generic numeric feature histogram
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("Feature Distribution Explorer")
st.markdown(
    "<p style='color:#94a3b8;font-size:13px;margin-top:-8px'>"
    "Explore the distribution of any numeric feature in this table"
    "</p>",
    unsafe_allow_html=True,
)

numeric_cols = df.select_dtypes(include="number").columns.tolist()
if numeric_cols:
    feat_col = st.selectbox("Select a numeric feature", numeric_cols)
    nbins    = st.slider("Bins", 10, 100, 40)
    series   = df[feat_col].dropna()
    fig = px.histogram(series, nbins=nbins,
                       color_discrete_sequence=["#60a5fa"],
                       title=f"Distribution of {feat_col}",
                       labels={"value": feat_col})
    q1, med, q3 = series.quantile([0.25, 0.5, 0.75])
    fig.add_vline(x=float(med), line_dash="dash", line_color="#f59e0b",
                  annotation_text=f"Median: {med:.2f}",
                  annotation_font_color="#f59e0b")
    fig.update_layout(**get_layout(showlegend=False, height=340))
    st.plotly_chart(fig, use_container_width=True)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Min",    f"{series.min():.2f}")
    c2.metric("Q1",     f"{q1:.2f}")
    c3.metric("Median", f"{med:.2f}")
    c4.metric("Q3",     f"{q3:.2f}")
    c5.metric("Max",    f"{series.max():.2f}")
else:
    st.info("No numeric columns found in this table.")
