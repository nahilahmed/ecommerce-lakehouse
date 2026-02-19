"""
SCD Type 2 — pure transformation functions.

No Spark globals, no Delta calls, no side effects.
Accepts DataFrames, returns DataFrames. Fully unit-testable.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window


def detect_scd2_changes(
    incremental_df: DataFrame,
    silver_current_df: DataFrame,
    business_key: list,
    compare_cols: list,
) -> DataFrame:
    """
    Filters incremental records down to only those that are new or have changed
    attribute values compared to the currently active silver records.

    Args:
        incremental_df:    New records arriving from bronze (post column-mapping).
        silver_current_df: Current active rows from silver (is_current = True).
                           Pass an empty DataFrame on first run.
        business_key:      Natural key columns e.g. ["customer_id"].
        compare_cols:      Attribute columns to check for changes e.g. ["email", "region"].

    Returns:
        DataFrame containing only genuinely new or changed records.
        Returns incremental_df unfiltered when silver_current_df is empty (first run).
    """
    if silver_current_df.limit(1).count() == 0:
        return incremental_df

    change_condition = " OR ".join([
        f"(s.{c} != t.{c}) OR "
        f"(s.{c} IS NULL AND t.{c} IS NOT NULL) OR "
        f"(s.{c} IS NOT NULL AND t.{c} IS NULL)"
        for c in compare_cols
    ])

    # Records that already exist in silver but have changed attributes
    changed = (
        incremental_df.alias("s")
        .join(silver_current_df.alias("t"), business_key, "inner")
        .where(change_condition)
        .select("s.*")
    )

    # Records that are completely new (not in silver at all)
    new_records = (
        incremental_df.alias("s")
        .join(silver_current_df.alias("t"), business_key, "left_anti")
        .select("s.*")
    )

    return changed.unionByName(new_records)


def apply_scd2_window(
    df: DataFrame,
    business_key: list,
    date_col: str,
    sk_col_name: str,
) -> DataFrame:
    """
    Applies SCD Type 2 effective-date chaining on a DataFrame of change records:
    - effective_from  = the record's own date
    - effective_to    = the next record's date, or 9999-12-31 for the current row
    - is_current      = True only for the latest record per business key
    - surrogate key   = MD5 of (date_col + business_key) for uniqueness across versions

    Args:
        df:           DataFrame of change records (new + changed, post detect_scd2_changes).
        business_key: Natural key columns e.g. ["customer_id"].
        date_col:     Date/timestamp column that drives versioning e.g. "customer_modified_at".
        sk_col_name:  Name of the surrogate key column to add e.g. "customer_sk".

    Returns:
        DataFrame with effective_from, effective_to, is_current, silver audit cols, and SK added.
    """
    window_spec = Window.partitionBy(business_key).orderBy(f.col(date_col).asc())

    # rn is kept so the notebook can identify the earliest change per key (to close existing records)
    chained = (
        df
        .withColumn("rn", f.row_number().over(window_spec))
        .withColumn("effective_from", f.col(date_col))
        .withColumn("effective_to", f.lead("effective_from").over(window_spec))
        .withColumn("is_current", f.col("effective_to").isNull())
        .withColumn(
            "effective_to",
            f.coalesce(f.col("effective_to"), f.lit("9999-12-31 23:59:59").cast("timestamp"))
        )
        .withColumn("silver_loaded_at", f.current_timestamp())
        .withColumn("silver_updated_at", f.current_timestamp())
    )

    sk_cols = [date_col] + business_key
    return chained.withColumn(
        sk_col_name,
        f.md5(f.concat_ws("|", *[f.col(c).cast("string") for c in sk_cols]))
    )
