"""
Unit tests for the data generation module.
No Spark required — pure pandas testing.
"""
import sys
from pathlib import Path
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_data import (
    generate_customers,
    generate_products,
    generate_orders,
    generate_payments,
    generate_inventory,
    generate_clickstream,
    generate_shipments,
    _seed_everything,
)

DR = {"start": "2023-01-01", "end": "2024-12-31"}


@pytest.fixture(autouse=True)
def fixed_seed():
    _seed_everything(42)


# ── Customers ─────────────────────────────────────────────────────────────────

class TestGenerateCustomers:
    def test_row_count_includes_dupes(self):
        df = generate_customers(100, DR)
        # ~2% dupes injected, so > 100
        assert len(df) >= 100

    def test_required_columns_present(self):
        df = generate_customers(50, DR)
        required = {"customer_id", "email", "segment", "signup_date", "is_active"}
        assert required.issubset(df.columns)

    def test_segment_values_valid(self):
        df = generate_customers(200, DR)
        valid = {"Premium", "Standard", "Budget", "Inactive"}
        assert set(df["segment"].unique()).issubset(valid)

    def test_country_always_us(self):
        df = generate_customers(100, DR)
        assert (df["country"] == "US").all()

    def test_some_nulls_in_email(self):
        df = generate_customers(1000, DR)
        assert df["email"].isna().sum() > 0, "Expected ~1% null emails"


# ── Products ──────────────────────────────────────────────────────────────────

class TestGenerateProducts:
    def test_row_count_exact(self):
        df = generate_products(50)
        assert len(df) == 50

    def test_sku_unique(self):
        df = generate_products(200)
        assert df["sku"].nunique() == 200

    def test_cost_less_than_price(self):
        df = generate_products(200)
        assert (df["cost_price"] < df["unit_price"]).all()

    def test_no_negative_prices(self):
        df = generate_products(200)
        assert (df["unit_price"] > 0).all()
        assert (df["cost_price"] > 0).all()


# ── Orders ────────────────────────────────────────────────────────────────────

class TestGenerateOrders:
    def test_row_count_exact(self):
        df = generate_orders(100, 50, 20, DR)
        assert len(df) == 100

    def test_customer_ids_in_range(self):
        df = generate_orders(200, 50, 20, DR)
        assert df["customer_id"].between(1, 50).all()

    def test_status_values_valid(self):
        df = generate_orders(200, 50, 20, DR)
        valid = {"pending", "processing", "shipped", "delivered", "cancelled", "returned"}
        assert set(df["status"].unique()).issubset(valid)

    def test_total_has_some_nulls(self):
        df = generate_orders(2000, 100, 50, DR)
        assert df["total_amount"].isna().sum() > 0


# ── Payments ──────────────────────────────────────────────────────────────────

class TestGeneratePayments:
    def test_payment_per_valid_order(self):
        orders = generate_orders(100, 50, 20, DR)
        payments = generate_payments(orders)
        valid_orders = orders.dropna(subset=["total_amount"])
        assert len(payments) == len(valid_orders)

    def test_payment_ids_unique(self):
        orders = generate_orders(100, 50, 20, DR)
        payments = generate_payments(orders)
        assert payments["payment_id"].nunique() == len(payments)

    def test_status_values_valid(self):
        orders = generate_orders(200, 50, 20, DR)
        payments = generate_payments(orders)
        valid = {"success", "failed", "pending", "refunded"}
        assert set(payments["status"].unique()).issubset(valid)


# ── Inventory ─────────────────────────────────────────────────────────────────

class TestGenerateInventory:
    def test_row_count_exact(self):
        df = generate_inventory(50, 100)
        assert len(df) == 100

    def test_warehouse_format(self):
        df = generate_inventory(50, 100)
        assert df["warehouse_id"].str.startswith("WH-").all()

    def test_reserved_lte_on_hand(self):
        df = generate_inventory(50, 500)
        assert (df["quantity_reserved"] <= df["quantity_on_hand"]).all()


# ── Clickstream ───────────────────────────────────────────────────────────────

class TestGenerateClickstream:
    def test_row_count_exact(self):
        df = generate_clickstream(100, 50, 20, DR)
        assert len(df) == 100

    def test_some_anonymous_events(self):
        df = generate_clickstream(500, 50, 20, DR)
        # ~30% should be anonymous (null customer_id)
        assert df["customer_id"].isna().sum() > 0

    def test_event_types_valid(self):
        df = generate_clickstream(300, 50, 20, DR)
        valid = {"page_view", "product_view", "add_to_cart", "remove_from_cart",
                 "search", "checkout_start", "checkout_complete", "product_review"}
        assert set(df["event_type"].unique()).issubset(valid)


# ── Shipments ─────────────────────────────────────────────────────────────────

class TestGenerateShipments:
    def test_only_shippable_orders(self):
        orders = generate_orders(200, 50, 20, DR)
        shipments = generate_shipments(orders)
        shippable_count = orders[orders["status"].isin(
            ["shipped", "delivered", "returned"]
        )].shape[0]
        assert len(shipments) == shippable_count

    def test_shipment_ids_unique(self):
        orders = generate_orders(200, 50, 20, DR)
        shipments = generate_shipments(orders)
        assert shipments["shipment_id"].nunique() == len(shipments)

    def test_delivered_has_actual_delivery_date(self):
        orders = generate_orders(500, 100, 50, DR)
        shipments = generate_shipments(orders)
        delivered = shipments[shipments["status"] == "delivered"]
        if len(delivered) > 0:
            assert delivered["actual_delivery"].notna().all()
