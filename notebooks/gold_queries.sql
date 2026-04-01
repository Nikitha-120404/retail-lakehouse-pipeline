-- ══════════════════════════════════════════════════════════════════════════════
-- NexCart Lakehouse — Gold Layer Dashboard Queries (Phase 9)
-- ══════════════════════════════════════════════════════════════════════════════
-- Run these in a Spark SQL cell or any SQL interface connected to your Delta tables.
-- Replace ${GOLD_PATH} with your actual gold layer path.


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Executive Revenue Dashboard — Last 30 Days
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    order_date,
    SUM(gross_revenue)          AS daily_gross_revenue,
    SUM(net_revenue)            AS daily_net_revenue,
    SUM(total_orders)           AS daily_orders,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,
    SUM(unique_customers)       AS daily_active_customers,
    ROUND(AVG(discount_rate_pct), 2) AS avg_discount_rate_pct
FROM delta.`data/gold/daily_revenue`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY order_date
ORDER BY order_date DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Revenue by Channel (Last 90 Days)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    channel,
    COUNT(DISTINCT order_date)          AS active_days,
    SUM(total_orders)                   AS total_orders,
    ROUND(SUM(gross_revenue), 2)        AS total_revenue,
    ROUND(AVG(avg_order_value), 2)      AS avg_order_value,
    ROUND(SUM(total_discounts), 2)      AS total_discounts,
    ROUND(AVG(discount_rate_pct), 2)    AS avg_discount_pct
FROM delta.`data/gold/daily_revenue`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY channel
ORDER BY total_revenue DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Top 20 Customers by LTV
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    customer_id,
    full_name,
    email,
    segment,
    state,
    total_orders,
    ROUND(total_spend, 2)              AS total_spend,
    ROUND(avg_order_value, 2)          AS avg_order_value,
    ltv_tier,
    days_since_last_order,
    ROUND(orders_per_month, 3)         AS orders_per_month,
    is_churned,
    customer_tenure_days
FROM delta.`data/gold/customer_ltv`
WHERE total_orders > 0
ORDER BY total_spend DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Churn Risk Cohort — Recently Inactive High-Value Customers
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    customer_id,
    full_name,
    email,
    segment,
    ROUND(total_spend, 2)              AS total_spend,
    ltv_tier,
    days_since_last_order,
    total_orders,
    is_churned
FROM delta.`data/gold/customer_ltv`
WHERE
    days_since_last_order BETWEEN 60 AND 180   -- inactive but not fully churned
    AND ltv_tier IN ('top', 'high')             -- high value
    AND is_churned = false
ORDER BY total_spend DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Top 10 Products by Revenue (Current Month)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    product_id,
    product_name,
    category,
    brand,
    times_ordered,
    unique_buyers,
    ROUND(attributed_revenue, 2)           AS attributed_revenue,
    estimated_units_sold,
    ROUND(estimated_gross_profit, 2)       AS estimated_gross_profit,
    margin_bucket,
    price_tier,
    category_revenue_rank
FROM delta.`data/gold/product_performance`
WHERE year_month = DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
ORDER BY attributed_revenue DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. Inventory Stockout Alert
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    product_id,
    product_name,
    category,
    overall_stock_status,
    total_qty_available,
    total_qty_on_hand,
    warehouses_needing_reorder,
    num_warehouses,
    ROUND(total_inventory_value, 2)    AS total_inventory_value,
    ROUND(avg_days_since_restock, 1)   AS avg_days_since_restock
FROM delta.`data/gold/inventory_health`
WHERE overall_stock_status IN ('out_of_stock', 'low_stock')
  AND is_active = true
ORDER BY overall_stock_status, total_qty_available ASC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. Carrier Performance Scorecard (Last 12 Weeks)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    carrier,
    SUM(total_shipments)               AS total_shipments,
    SUM(delayed_shipments)             AS delayed_shipments,
    ROUND(AVG(on_time_rate_pct), 2)    AS avg_on_time_rate_pct,
    ROUND(AVG(avg_transit_days), 2)    AS avg_transit_days,
    ROUND(AVG(avg_delay_days), 2)      AS avg_delay_days,
    ROUND(SUM(total_shipping_cost), 2) AS total_shipping_cost,
    ROUND(AVG(avg_shipping_cost), 2)   AS avg_cost_per_shipment
FROM delta.`data/gold/fulfillment_kpis`
WHERE week >= DATE_SUB(CURRENT_DATE(), 84)   -- 12 weeks
GROUP BY carrier
ORDER BY avg_on_time_rate_pct DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. Weekend vs Weekday Revenue Split
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    is_weekend,
    COUNT(DISTINCT order_date)       AS days,
    SUM(total_orders)                AS total_orders,
    ROUND(SUM(gross_revenue), 2)     AS total_revenue,
    ROUND(AVG(avg_order_value), 2)   AS avg_order_value,
    ROUND(AVG(discount_rate_pct), 2) AS avg_discount_pct
FROM delta.`data/gold/daily_revenue`
GROUP BY is_weekend
ORDER BY is_weekend;


-- ─────────────────────────────────────────────────────────────────────────────
-- 9. Data Quality Report Summary
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    `table`,
    rule_name,
    status,
    COUNT(*)                    AS n_checks,
    ROUND(AVG(metric), 4)       AS avg_metric,
    MAX(checked_at)             AS last_checked
FROM delta.`data/gold/quality_report`
GROUP BY `table`, rule_name, status
ORDER BY status DESC, `table`;


-- ─────────────────────────────────────────────────────────────────────────────
-- 10. Month-over-Month Revenue Growth
-- ─────────────────────────────────────────────────────────────────────────────
WITH monthly AS (
    SELECT
        DATE_FORMAT(order_date, 'yyyy-MM')  AS year_month,
        ROUND(SUM(gross_revenue), 2)        AS gross_revenue,
        SUM(total_orders)                   AS total_orders
    FROM delta.`data/gold/daily_revenue`
    GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
),
with_prev AS (
    SELECT
        year_month,
        gross_revenue,
        total_orders,
        LAG(gross_revenue) OVER (ORDER BY year_month) AS prev_revenue,
        LAG(total_orders)  OVER (ORDER BY year_month) AS prev_orders
    FROM monthly
)
SELECT
    year_month,
    gross_revenue,
    total_orders,
    prev_revenue,
    ROUND((gross_revenue - prev_revenue) / NULLIF(prev_revenue, 0) * 100, 2) AS revenue_growth_pct,
    ROUND((total_orders  - prev_orders)  / NULLIF(prev_orders,  0) * 100, 2) AS order_growth_pct
FROM with_prev
ORDER BY year_month;
