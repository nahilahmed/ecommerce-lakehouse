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

    print(f"\nStarting SCD Type 1 processing for table: {table_name}")
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

def process_scd_type2(table_name, source_table, row_dict):
    """
    Performs SCD Type 2 processing for a given table.

    Args:
        table_name (str): Name of the target silver table.
        source_table (str): Name of the source bronze table.
        row_dict (dict): Dictionary containing metadata for the table.

    Returns:
        None
    """
    print("\n========== SCD Type 2 Processing Start ==========")

    print(f"\nStarting SCD Type 2 processing for table: {table_name}")
    print(f"Source table: {source_table}")

    business_key = row_dict['scd_business_key']
    scd_date_col = row_dict['scd_date_col']
    sk_column_name = row_dict['surrogate_key_column']
    scd_compare_columns = row_dict['scd_compare_columns']
    
    sk_cols = [scd_date_col] + business_key

    # Read Watermark from Metadata Table
    watermark_row = spark.table('shopmetrics_ecommerce.metadata.silver_metadata') \
        .filter(f.col('table_name') == table_name) \
        .select('bronze_max_watermark') \
        .first()

    bronze_max_watermark = watermark_row['bronze_max_watermark'] if watermark_row and watermark_row['bronze_max_watermark'] is not None else '1900-01-01 00:00:00'
    print(f"Current bronze_max_watermark: {bronze_max_watermark}")

    incremental_df = (spark.table(source_table)
                        .filter(f.col('ingestion_timestamp') > bronze_max_watermark))
    
    # Unity Catalog compatible empty check
    if incremental_df.limit(1).count() == 0:
        print(f"No new records to process for {table_name}. Skipping table write/merge and watermark update.")
        return

    # Capture watermark BEFORE column mapping (in case ingestion_timestamp gets renamed)
    new_watermark = incremental_df.agg(f.max("ingestion_timestamp")).first()[0]

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

    full_table_name = f"shopmetrics_ecommerce.silver.{table_name}"

    # ---- Change Detection: Only keep records where attributes actually changed ----
    if spark.catalog.tableExists(full_table_name):
        silver_current = (spark.table(full_table_name)
                          .filter(f.col("is_current") == True))

        change_condition = " OR ".join(
            [f"(s.{c} != t.{c}) OR (s.{c} IS NULL AND t.{c} IS NOT NULL) OR (s.{c} IS NOT NULL AND t.{c} IS NULL)"
             for c in scd_compare_columns]
        )

        # Records that exist in silver but have changed attributes
        changed_records = (incremental_df.alias("s")
            .join(silver_current.alias("t"), business_key, "inner")
            .where(change_condition)
            .select("s.*"))

        # Completely new records not in silver at all
        new_to_silver = (incremental_df.alias("s")
            .join(silver_current.alias("t"), business_key, "left_anti")
            .select("s.*"))

        # Combine actual changes + new records
        incremental_df = changed_records.unionByName(new_to_silver)

        # Re-check if anything remains after change detection
        if incremental_df.limit(1).count() == 0:
            print(f"No actual changes detected for {table_name}. Skipping.")
            return
    
    # ---- Window and chain records ----
    window_spec = Window.partitionBy(business_key).orderBy(f.col(scd_date_col).asc())

    change_df = (
        incremental_df
        .withColumn("rn", f.row_number().over(window_spec))
        .withColumn('effective_from', f.col(scd_date_col))
        .withColumn('effective_to', f.lead('effective_from').over(window_spec))
        .withColumn("is_current", f.col("effective_to").isNull())
        .withColumn("effective_to", 
            f.coalesce(f.col("effective_to"), f.lit("9999-12-31 23:59:59").cast("timestamp")))
        .withColumn('silver_loaded_at', f.current_timestamp())
        .withColumn('silver_updated_at', f.current_timestamp())
    )

    # Generate surrogate key for all records
    change_df = change_df.withColumn(
        sk_column_name, 
        f.md5(f.concat_ws("|", *[f.col(c).cast("string") for c in sk_cols]))
    )

    print(f"Filtered and windowed records from {source_table}")

    if not spark.catalog.tableExists(full_table_name):
        # ---- First Run: Create table ----
        (
            change_df
                .write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(full_table_name)
        )
        print(f"🆕 {full_table_name} does not exist. Creating the table.")
    else:
        # ---- Subsequent Run: Close existing + insert new ----
        
        # Get earliest change per business key to close current records
        initial_change_df = (
            change_df
                .filter(f.col("rn") == 1)
                .select(*business_key, f.col('effective_from').alias('close_date'))
        )

        silver_table = DeltaTable.forName(spark, full_table_name)
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in business_key])

        # Step 1: Close existing current records
        (
            silver_table.alias('t')
            .merge(
                initial_change_df.alias('s'), 
                f"{merge_condition} AND t.is_current = true"
            )
            .whenMatchedUpdate(set={
                "is_current": f.lit(False), 
                "effective_to": f.col("s.close_date"), 
                "silver_updated_at": f.current_timestamp()
            })
            .execute()
        )
        print(f"🔄 Closed existing records in {full_table_name} for new SCD2 changes.")

        # Step 2: Insert new versions using MERGE to avoid duplicates on rerun
        (
            silver_table.alias('t')
            .merge(
                change_df.alias('s'),
                f"t.{sk_column_name} = s.{sk_column_name}"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"🆕 Inserted new SCD2 records into {full_table_name}")

    # ---- Update watermark AFTER successful merge ----
    print(f"Updating bronze_max_watermark to: {new_watermark}")

    spark.sql(f"""
        UPDATE shopmetrics_ecommerce.metadata.silver_metadata
        SET bronze_max_watermark = '{new_watermark}'
        WHERE table_name = '{table_name}'
    """)
    print(f"✅ Updated watermark for {table_name} in metadata table.")

    print("\n========== SCD Type 2 Processing Complete ==========")

# COMMAND ----------

# DBTITLE 1,Process Fact Table with Correct SK Selection After Joins
import json
from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql.window import Window

def process_fact(table_name, source_table, row_dict):
    print(f"\n========== Fact Table Processing Start ==========")
    print(f"Processing fact table: {table_name} | Source: {source_table}")

    # 1. Check dependencies
    dependencies = row_dict['dependencies']
    mdb_df = spark.table('shopmetrics_ecommerce.metadata.silver_metadata')
    for dep in dependencies:
        dep_row = mdb_df.filter(f.col('table_id') == dep).first()
        if not dep_row or dep_row['bronze_max_watermark'] is None:
            print(f"Dependency {dep} not processed. Skipping fact table.")
            return
        print(f"Dependency {dep} processed up to: {dep_row['bronze_max_watermark']}")

    # 2. Read Watermark from Metadata Table
    watermark_row = mdb_df.filter(f.col('table_name') == table_name).select('bronze_max_watermark').first()
    bronze_max_watermark = watermark_row['bronze_max_watermark'] if watermark_row and watermark_row['bronze_max_watermark'] is not None else '1900-01-01 00:00:00'
    print(f"Current bronze_max_watermark: {bronze_max_watermark}")

    # 3. Read incremental fact data
    fact_df = spark.table(source_table).filter(f.col('ingestion_timestamp') > bronze_max_watermark)
    if fact_df.limit(1).count() == 0:
        print(f"No new records to process for {table_name}. Skipping.")
        return
    new_watermark = fact_df.agg(f.max('ingestion_timestamp')).first()[0]

    # 4. Dynamic dimension joins (add only SK, deduplicate, fix ambiguity)
    dim_joins = row_dict['dimension_joins']
    sk_columns = []
    if dim_joins:
        dim_joins_list = json.loads(dim_joins)
        original_fact_columns = fact_df.columns.copy()
        for join_info in dim_joins_list:
            dim_table = join_info['dim_table']
            natural_key = join_info['natural_key']
            surrogate_key = join_info['surrogate_key']
            join_type = join_info.get('join_type', 'left')
            effective_date_col = join_info.get('effective_date_col', None)

            # Rename dimension natural key to avoid ambiguity
            dim_nk_renamed = f"dim_{natural_key}"
            select_expr = [f"{natural_key} as {dim_nk_renamed}", surrogate_key]
            if effective_date_col:
                select_expr += ['effective_from', 'effective_to']
            d_df = spark.table(dim_table).selectExpr(*select_expr).alias('d')
            f_df = fact_df.alias('f')

            if effective_date_col:
                # SCD2: time-range join
                join_cond = (
                    (f_df[natural_key] == d_df[dim_nk_renamed]) &
                    (f_df[effective_date_col] >= d_df['effective_from']) &
                    (f_df[effective_date_col] < d_df['effective_to'])
                )
            else:
                # SCD1: simple join
                join_cond = (f_df[natural_key] == d_df[dim_nk_renamed])

            fact_df = f_df.join(d_df, join_cond, join_type)

            # After join, select only original fact columns + previously joined SKs from 'f' + current SK from 'd'
            prev_sk_cols = [f"f.{sk}" for sk in sk_columns if sk in f_df.columns]
            select_cols = [f"f.{col}" for col in original_fact_columns] + prev_sk_cols + [f"d.{surrogate_key}"]
            fact_df = fact_df.selectExpr(*select_cols)
            print(f"Joined with {dim_table} on {natural_key} ({'SCD2' if effective_date_col else 'SCD1'}), added SK: {surrogate_key}")

            # Track SK columns, avoid duplicates
            if surrogate_key not in sk_columns:
                sk_columns.append(surrogate_key)

    # 5. Add load/update timestamps
    fact_df = fact_df.withColumn('silver_loaded_at', f.current_timestamp()) \
                     .withColumn('silver_updated_at', f.current_timestamp())

    print(fact_df.columns)

    # 6. MERGE into silver fact table
    full_table_name = f"shopmetrics_ecommerce.silver.{table_name}"
    key_columns = row_dict['dedup_columns']
    if not spark.catalog.tableExists(full_table_name):
        fact_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        print(f"🆕 Created fact table: {full_table_name}")
    else:
        silver_table = DeltaTable.forName(spark, full_table_name)
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in key_columns])
        (
            silver_table.alias("t")
            .merge(fact_df.alias("s"), merge_condition)
            .whenMatchedUpdate(set={c: f"s.{c}" for c in fact_df.columns if c != "silver_loaded_at"})
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"🔄 Merged new records into {full_table_name}")

    # 7. Update the watermark in the silver metadata table
    print(f"Updating bronze_max_watermark to: {new_watermark}")
    spark.sql(f"""
        UPDATE shopmetrics_ecommerce.metadata.silver_metadata
        SET bronze_max_watermark = '{new_watermark}'
        WHERE table_name = '{table_name}'
    """)
    print("\n========== Fact Table Processing Complete ==========")

# COMMAND ----------

mdb_df = spark.table('shopmetrics_ecommerce.metadata.silver_metadata')

for row in mdb_df.collect():
    table_name = row['table_name']
    source_table = row['source_table']
    transform_type = row['transform_type']

    print(f"\n--- Processing Table: {table_name} | Source: {source_table} ---")
    if transform_type == 'scd_type1':
        process_scd_type1(table_name, source_table, row)
    elif transform_type == 'scd_type2':
        process_scd_type2(table_name, source_table, row)
    elif transform_type == 'fact':
        process_fact(table_name, source_table, row)
    else:
        print(f"Unsupported transform type: {transform_type}")
