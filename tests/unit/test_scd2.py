"""
Unit tests for SCD Type 2 transformation logic.
Tests detect_scd2_changes() and apply_scd2_window() from src/transforms/scd2.py.
No Delta, no Unity Catalog, no Databricks environment needed.

Covers AC-003: update customer email → old record end_date populated, new record current.

Run with: pytest tests/unit/test_scd2.py -v
"""

import sys
import os
import pytest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))
from transforms.scd2 import detect_scd2_changes, apply_scd2_window


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CUSTOMER_SCHEMA  = ["customer_id", "email", "region", "customer_modified_at"]
SILVER_SCHEMA    = ["customer_id", "email", "region", "customer_modified_at", "is_current"]
HIGH_WATERMARK   = datetime(9999, 12, 31, 23, 59, 59)


def make_incremental(spark, rows):
    return spark.createDataFrame(rows, CUSTOMER_SCHEMA)


def make_silver_current(spark, rows):
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def rows_by_email(result):
    """Index result rows by email for easy assertion."""
    return {r["email"]: r for r in result.collect()}


# ---------------------------------------------------------------------------
# detect_scd2_changes tests
# ---------------------------------------------------------------------------

class TestDetectChanges:

    def test_first_run_returns_all_records(self, spark):
        """When silver is empty (first run), all incremental records pass through."""
        incremental = make_incremental(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 1, 1)),
            ("C002", "bob@example.com",   "US", datetime(2024, 1, 1)),
        ])
        empty_silver = make_silver_current(spark, [])

        result = detect_scd2_changes(incremental, empty_silver, ["customer_id"], ["email", "region"])
        assert result.count() == 2

    def test_unchanged_record_excluded(self, spark):
        """A record with identical attributes should not appear in the output."""
        incremental = make_incremental(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 6, 1)),
        ])
        silver = make_silver_current(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 1, 1), True),
        ])

        result = detect_scd2_changes(incremental, silver, ["customer_id"], ["email", "region"])
        assert result.count() == 0

    def test_email_change_detected(self, spark):
        """A changed email must be included in the output (AC-003 prerequisite)."""
        incremental = make_incremental(spark, [
            ("C001", "alice_new@example.com", "EU", datetime(2024, 6, 1)),
        ])
        silver = make_silver_current(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 1, 1), True),
        ])

        result = detect_scd2_changes(incremental, silver, ["customer_id"], ["email", "region"])
        assert result.count() == 1
        assert result.first()["email"] == "alice_new@example.com"

    def test_region_change_detected(self, spark):
        """A changed region must be detected even when email stays the same."""
        incremental = make_incremental(spark, [
            ("C001", "alice@example.com", "APAC", datetime(2024, 6, 1)),
        ])
        silver = make_silver_current(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 1, 1), True),
        ])

        result = detect_scd2_changes(incremental, silver, ["customer_id"], ["email", "region"])
        assert result.count() == 1

    def test_new_customer_included(self, spark):
        """A customer_id not present in silver at all must be treated as new."""
        incremental = make_incremental(spark, [
            ("C002", "bob@example.com", "US", datetime(2024, 6, 1)),
        ])
        silver = make_silver_current(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 1, 1), True),
        ])

        result = detect_scd2_changes(incremental, silver, ["customer_id"], ["email", "region"])
        assert result.count() == 1
        assert result.first()["customer_id"] == "C002"

    def test_mix_of_changed_new_and_unchanged(self, spark):
        """Changed and new records are included; unchanged records are excluded."""
        incremental = make_incremental(spark, [
            ("C001", "alice_new@example.com", "EU", datetime(2024, 6, 1)),  # changed
            ("C002", "bob@example.com",       "EU", datetime(2024, 6, 1)),  # unchanged
            ("C003", "carol@example.com",     "US", datetime(2024, 6, 1)),  # new
        ])
        silver = make_silver_current(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 1, 1), True),
            ("C002", "bob@example.com",   "EU", datetime(2024, 1, 1), True),
        ])

        result = detect_scd2_changes(incremental, silver, ["customer_id"], ["email", "region"])
        ids = {r["customer_id"] for r in result.collect()}
        assert ids == {"C001", "C003"}


# ---------------------------------------------------------------------------
# apply_scd2_window tests
# ---------------------------------------------------------------------------

class TestApplyScd2Window:

    def test_single_record_is_current(self, spark):
        """A single record for a customer must be marked is_current = True."""
        df = make_incremental(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 1, 1)),
        ])
        result = apply_scd2_window(df, ["customer_id"], "customer_modified_at", "customer_sk")
        assert result.first()["is_current"] == True

    def test_single_record_effective_to_is_high_watermark(self, spark):
        """A current record's effective_to must be 9999-12-31."""
        df = make_incremental(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 1, 1)),
        ])
        result = apply_scd2_window(df, ["customer_id"], "customer_modified_at", "customer_sk")
        assert result.first()["effective_to"] == HIGH_WATERMARK

    def test_email_change_closes_old_record(self, spark):
        """
        AC-003: When a customer's email changes, the old record must have
        is_current=False and effective_to set to the date of the new record.
        """
        df = make_incremental(spark, [
            ("C001", "alice@example.com",     "EU", datetime(2024, 1, 1)),   # original
            ("C001", "alice_new@example.com", "EU", datetime(2024, 6, 1)),   # change
        ])
        result = apply_scd2_window(df, ["customer_id"], "customer_modified_at", "customer_sk")
        by_email = rows_by_email(result)

        # Old record must be closed
        assert by_email["alice@example.com"]["is_current"] == False
        assert by_email["alice@example.com"]["effective_to"] == datetime(2024, 6, 1)

        # New record must be current
        assert by_email["alice_new@example.com"]["is_current"] == True
        assert by_email["alice_new@example.com"]["effective_to"] == HIGH_WATERMARK

    def test_effective_dates_chain_for_multiple_changes(self, spark):
        """Three versions must chain: v1.effective_to = v2.effective_from, etc."""
        df = make_incremental(spark, [
            ("C001", "v1@example.com", "EU",   datetime(2024, 1, 1)),
            ("C001", "v2@example.com", "EU",   datetime(2024, 4, 1)),
            ("C001", "v3@example.com", "APAC", datetime(2024, 9, 1)),
        ])
        result = apply_scd2_window(df, ["customer_id"], "customer_modified_at", "customer_sk")
        by_email = rows_by_email(result)

        assert by_email["v1@example.com"]["effective_to"] == datetime(2024, 4, 1)
        assert by_email["v2@example.com"]["effective_to"] == datetime(2024, 9, 1)
        assert by_email["v3@example.com"]["effective_to"] == HIGH_WATERMARK
        assert by_email["v3@example.com"]["is_current"]   == True

    def test_only_latest_record_is_current(self, spark):
        """Only the most recent record per customer must have is_current=True."""
        df = make_incremental(spark, [
            ("C001", "v1@example.com", "EU", datetime(2024, 1, 1)),
            ("C001", "v2@example.com", "EU", datetime(2024, 6, 1)),
        ])
        result = apply_scd2_window(df, ["customer_id"], "customer_modified_at", "customer_sk")

        current_count = result.filter("is_current = true").count()
        assert current_count == 1

    def test_surrogate_key_unique_per_version(self, spark):
        """Each version of the same customer must have a distinct surrogate key."""
        df = make_incremental(spark, [
            ("C001", "v1@example.com", "EU", datetime(2024, 1, 1)),
            ("C001", "v2@example.com", "EU", datetime(2024, 6, 1)),
        ])
        result = apply_scd2_window(df, ["customer_id"], "customer_modified_at", "customer_sk")
        sks = [r["customer_sk"] for r in result.collect()]
        assert len(set(sks)) == 2

    def test_different_customers_independent(self, spark):
        """Two separate customers must each have their own is_current=True record."""
        df = make_incremental(spark, [
            ("C001", "alice@example.com", "EU", datetime(2024, 1, 1)),
            ("C002", "bob@example.com",   "US", datetime(2024, 1, 1)),
        ])
        result = apply_scd2_window(df, ["customer_id"], "customer_modified_at", "customer_sk")

        current_count = result.filter("is_current = true").count()
        assert current_count == 2

    def test_audit_columns_present(self, spark):
        """silver_loaded_at and silver_updated_at must be present and non-null."""
        df = make_incremental(spark, [("C001", "alice@example.com", "EU", datetime(2024, 1, 1))])
        result = apply_scd2_window(df, ["customer_id"], "customer_modified_at", "customer_sk")

        assert "silver_loaded_at"  in result.columns
        assert "silver_updated_at" in result.columns
        assert result.filter("silver_loaded_at IS NULL").count()  == 0
        assert result.filter("silver_updated_at IS NULL").count() == 0
