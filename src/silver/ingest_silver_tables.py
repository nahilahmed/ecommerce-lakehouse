# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.window import Window

from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,SCD Type 1 process with empty check (Unity Catalog compatible)
def process_scd_type1(table_name, source_table, row_dict):
    """
    Processes SCD Type 1 for a given table using metadata and source table information.

    Args:
        table_name (str): Name of the target silver table.
        source_table (str): Name of the source bronze table.
        row_dict (dict): Metadata row containing dedup_columns, surrogate_key_column, and other relevant info.

    Steps:
        1. Reads the watermark from the metadata table.
        2. Filters and deduplicates new records from the source table.
        3. Adds surrogate key and timestamp columns.
        4. Creates or merges into the silver Delta table.
        5. Updates the watermark in the metadata table.
    """
    print("\n========== SCD Type 1 Processing Start ==========")

    print(f"Starting SCD Type 1 processing for table: {table_name}")
    print(f"Source table: {source_table}")

    key_columns = row_dict['dedup_columns']
    sk_column_name = row_dict['surrogate_key_column']

    # Read Watermark from Metadata Table
    watermark_row = spark.table('shopmetrics_ecommerce.metadata.silver_metadata') \
        .filter(f.col('table_name') == table_name) \
        .select('bronze_max_watermark') \
        .first()

    bronze_max_watermark = watermark_row['bronze_max_watermark'] if watermark_row and watermark_row['bronze_max_watermark'] is not None else '1900-01-01 00:00:00'
    print(f"Current bronze_max_watermark: {bronze_max_watermark}")

    incremental_df = (spark.table(source_table)
                        .filter(f.col('ingestion_timestamp') > bronze_max_watermark))
    
    # Apply column mapping for renaming to silver columns if present
    column_mapping = row_dict['column_mappings']
    if column_mapping:
        import json
        mapping_dict = json.loads(column_mapping)
        incremental_df = incremental_df.select([
            f.col(c).alias(mapping_dict[c]) if c in mapping_dict else f.col(c)
            for c in incremental_df.columns
        ])
        print(f"Applied column mapping: {mapping_dict}")

    window_spec = Window.partitionBy(key_columns).orderBy(f.col('source_file_modified_at').desc())

    change_df = (
        incremental_df
        .withColumn('dedupe_rank', f.row_number().over(window_spec))
        .filter(f.col('dedupe_rank') == 1)
        .drop('dedupe_rank')
        .withColumn('silver_loaded_at', f.current_timestamp())
        .withColumn('silver_updated_at', f.current_timestamp())
    )
    print(f"Filtered and deduplicated records from {source_table}")

    # Unity Catalog compatible empty check
    if change_df.limit(1).count() == 0:
        print(f"No new records to process for {table_name}. Skipping table write/merge and watermark update.")
        return

    change_df_with_sk = change_df.withColumn(
        sk_column_name,
        f.md5(f.concat_ws("||", *[f.col(c).cast("string") for c in key_columns]))
    )

    

    print(f"Added surrogate key column: {sk_column_name}")

    full_table_name = f"shopmetrics_ecommerce.silver.{table_name}"

    if not spark.catalog.tableExists(full_table_name):
        # First run — create table from the DataFrame
        (
            change_df_with_sk
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(full_table_name)
        )
        print(f"🆕 {full_table_name} does not exist. Creating the table.")
    else:
        # Subsequent runs — MERGE
        silver_table = DeltaTable.forName(spark, full_table_name)
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in key_columns])
        (
            silver_table.alias("t")
            .merge(change_df_with_sk.alias("s"), merge_condition)
            .whenMatchedUpdate(set={
                c: f"s.{c}" for c in change_df_with_sk.columns if c != "silver_loaded_at"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"🔄 Merged new records into {full_table_name}")

    # Update the watermark in the silver metadata table
    new_watermark = change_df_with_sk.agg(f.max("ingestion_timestamp")).first()[0]
    print(f"Updating bronze_max_watermark to: {new_watermark}")

    spark.sql(f"""
        UPDATE shopmetrics_ecommerce.metadata.silver_metadata
        SET bronze_max_watermark = '{new_watermark}'
        WHERE table_name = '{table_name}'
    """)

    print("\n========== SCD Type 1 Processing Complete ==========")

# COMMAND ----------

mdb_df = spark.table('shopmetrics_ecommerce.metadata.silver_metadata').filter(f.col('transform_type') == 'scd_type1')

for row in mdb_df.collect():
    table_name = row['table_name']
    source_table = row['source_table']
    transform_type = row['transform_type']

    print(f"\n--- Processing Table: {table_name} | Source: {source_table} ---")
    if transform_type == 'scd_type1':
        process_scd_type1(table_name, source_table, row)
