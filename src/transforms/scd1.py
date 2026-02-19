"""
SCD Type 1 — pure transformation functions.

No Spark globals, no Delta calls, no side effects.
Accepts DataFrames, returns DataFrames. Fully unit-testable.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window


def apply_scd1(
    df: DataFrame,
    key_cols: list,
    sk_col_name: str,
    order_col: str = "source_file_modified_at",
) -> DataFrame:
    """
    Deduplicates incoming records keeping the latest per key,
    adds a surrogate key (MD5) and silver audit timestamps.

    Args:
        df:           Incremental DataFrame from bronze (post column-mapping).
        key_cols:     Columns that uniquely identify a record e.g. ["product_id"].
        sk_col_name:  Name of the surrogate key column to add e.g. "products_sk".
        order_col:    Column used to pick the latest record when duplicates exist.
                      Defaults to "source_file_modified_at".

    Returns:
        Deduplicated DataFrame with surrogate key and audit columns added.
    """
    window_spec = Window.partitionBy(key_cols).orderBy(f.col(order_col).desc())

    deduped = (
        df
        .withColumn("_rank", f.row_number().over(window_spec))
        .filter(f.col("_rank") == 1)
        .drop("_rank")
        .withColumn("silver_loaded_at", f.current_timestamp())
        .withColumn("silver_updated_at", f.current_timestamp())
    )

    return deduped.withColumn(
        sk_col_name,
        f.md5(f.concat_ws("||", *[f.col(c).cast("string") for c in key_cols]))
    )
