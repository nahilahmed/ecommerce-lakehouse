"""
Unit tests for SCD Type 1 transformation logic.
Tests apply_scd1() from src/transforms/scd1.py in isolation —
no Delta, no Unity Catalog, no Databricks environment needed.

Run with: pytest tests/unit/test_scd1.py -v
"""

import sys
import os
import pytest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))
from transforms.scd1 import apply_scd1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_products_df(spark, rows):
    """Helper: create a minimal products DataFrame matching bronze schema."""
    schema = ["product_id", "product_name", "price", "source_file_modified_at"]
    return spark.createDataFrame(rows, schema)


# ---------------------------------------------------------------------------
# Deduplication tests
# ---------------------------------------------------------------------------

class TestDeduplication:

    def test_single_record_passes_through(self, spark):
        """A single record for a key should be returned unchanged."""
        df = make_products_df(spark, [
            ("P001", "Widget A", 9.99, datetime(2024, 1, 1)),
        ])
        result = apply_scd1(df, ["product_id"], "products_sk")
        assert result.count() == 1

    def test_duplicate_keys_keep_latest(self, spark):
        """Given two records for the same product_id, only the most recent is kept."""
        df = make_products_df(spark, [
            ("P001", "Widget A", 9.99,  datetime(2024, 1, 1)),   # older
            ("P001", "Widget A", 12.99, datetime(2024, 6, 1)),   # newer — should win
        ])
        result = apply_scd1(df, ["product_id"], "products_sk")

        assert result.count() == 1
        assert result.first()["price"] == 12.99

    def test_different_keys_both_returned(self, spark):
        """Distinct product_ids should each produce one output row."""
        df = make_products_df(spark, [
            ("P001", "Widget A", 9.99,  datetime(2024, 1, 1)),
            ("P002", "Widget B", 19.99, datetime(2024, 1, 1)),
        ])
        result = apply_scd1(df, ["product_id"], "products_sk")
        assert result.count() == 2

    def test_three_versions_keeps_latest(self, spark):
        """Three versions of the same product — only the latest survives."""
        df = make_products_df(spark, [
            ("P001", "Widget A", 9.99,  datetime(2024, 1, 1)),
            ("P001", "Widget A", 11.99, datetime(2024, 3, 1)),
            ("P001", "Widget A", 14.99, datetime(2024, 6, 1)),   # latest
        ])
        result = apply_scd1(df, ["product_id"], "products_sk")

        assert result.count() == 1
        assert result.first()["price"] == 14.99


# ---------------------------------------------------------------------------
# Surrogate key tests
# ---------------------------------------------------------------------------

class TestSurrogateKey:

    def test_surrogate_key_column_added(self, spark):
        """Output must contain the surrogate key column."""
        df = make_products_df(spark, [("P001", "Widget A", 9.99, datetime(2024, 1, 1))])
        result = apply_scd1(df, ["product_id"], "products_sk")
        assert "products_sk" in result.columns

    def test_surrogate_key_is_not_null(self, spark):
        """Surrogate key must never be null."""
        df = make_products_df(spark, [("P001", "Widget A", 9.99, datetime(2024, 1, 1))])
        result = apply_scd1(df, ["product_id"], "products_sk")
        assert result.filter("products_sk IS NULL").count() == 0

    def test_surrogate_key_is_deterministic(self, spark):
        """Same input always produces the same MD5 surrogate key."""
        df = make_products_df(spark, [("P001", "Widget A", 9.99, datetime(2024, 1, 1))])
        sk_first  = apply_scd1(df, ["product_id"], "products_sk").first()["products_sk"]
        sk_second = apply_scd1(df, ["product_id"], "products_sk").first()["products_sk"]
        assert sk_first == sk_second

    def test_different_keys_produce_different_sks(self, spark):
        """Two distinct product_ids must produce distinct surrogate keys."""
        df = make_products_df(spark, [
            ("P001", "Widget A", 9.99,  datetime(2024, 1, 1)),
            ("P002", "Widget B", 19.99, datetime(2024, 1, 1)),
        ])
        result = apply_scd1(df, ["product_id"], "products_sk")
        sks = [r["products_sk"] for r in result.collect()]
        assert len(set(sks)) == 2


# ---------------------------------------------------------------------------
# Audit column tests
# ---------------------------------------------------------------------------

class TestAuditColumns:

    def test_silver_loaded_at_added(self, spark):
        df = make_products_df(spark, [("P001", "Widget A", 9.99, datetime(2024, 1, 1))])
        result = apply_scd1(df, ["product_id"], "products_sk")
        assert "silver_loaded_at" in result.columns
        assert result.filter("silver_loaded_at IS NULL").count() == 0

    def test_silver_updated_at_added(self, spark):
        df = make_products_df(spark, [("P001", "Widget A", 9.99, datetime(2024, 1, 1))])
        result = apply_scd1(df, ["product_id"], "products_sk")
        assert "silver_updated_at" in result.columns
        assert result.filter("silver_updated_at IS NULL").count() == 0
