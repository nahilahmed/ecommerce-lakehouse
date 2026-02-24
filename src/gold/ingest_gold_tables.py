# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Master Orchestration Notebook
# MAGIC
# MAGIC Reads `gold_metadata` ordered by `processing_order`, calls each table's dedicated
# MAGIC notebook under `table_notebooks/`, and updates `silver_max_watermark` on success.
# MAGIC
# MAGIC **Parameter passing:**
# MAGIC - Master → Child: `table_id`, `silver_max_watermark`
# MAGIC - Child → Master: new watermark string via `dbutils.notebook.exit()`
# MAGIC   or `"NO_NEW_DATA"` if nothing to process
# MAGIC
# MAGIC **Watermark ownership:** Master owns `gold_metadata` — child notebooks are pure
# MAGIC transformation and have no metadata dependency.
# MAGIC
# MAGIC **Pipeline routing:**
# MAGIC - Pass `pipeline_type=batch` (default) for the daily batch job
# MAGIC - Pass `pipeline_type=streaming` for the 30-min streaming job
# MAGIC - Only gold tables whose `gold_metadata.pipeline_type` matches are executed

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

# Widget — set to 'streaming' when called from the streaming Databricks job
dbutils.widgets.text("pipeline_type", "batch")
pipeline_type = dbutils.widgets.get("pipeline_type")

assert pipeline_type in ("batch", "streaming"), \
    f"pipeline_type must be 'batch' or 'streaming', got: '{pipeline_type}'"

# COMMAND ----------

# DBTITLE 1,Load Active Gold Metadata (ordered by processing_order)
gold_meta = (
    spark.table('shopmetrics_ecommerce.metadata.gold_metadata')
    .filter(f.col('is_active') == True)
    .filter(f.col('pipeline_type') == pipeline_type)
    .orderBy('processing_order')
)

print("\n" + "="*80)
print(f"GOLD LAYER PROCESSING — pipeline_type={pipeline_type.upper()} — DEPENDENCY-ORDERED EXECUTION")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Process Each Gold Table
for row in gold_meta.collect():
    table_id      = row['table_id']
    table_name    = row['table_name']
    notebook_name = row['notebook_name']
    agg_type      = row['aggregation_type']
    order         = row['processing_order']

    gold_table_name = f"shopmetrics_ecommerce.gold.{table_name}"

    silver_max_watermark = (
        str(row['silver_max_watermark'])
        if row['silver_max_watermark'] is not None
        else '1900-01-01 00:00:00'
    )

    print(f"\n{'='*80}")
    print(f"[Order {order}] {table_id}  |  type: {agg_type}")
    print(f"Target : shopmetrics_ecommerce.gold.{table_name}")
    print(f"silver_max_watermark passed: {silver_max_watermark}")
    print(f"{'='*80}")

    notebook_path = f"table_notebooks/{notebook_name}"
    params = {
        "table_name": gold_table_name,
        "silver_max_watermark": silver_max_watermark,
    }

    try:
        exit_value = dbutils.notebook.run(notebook_path, timeout_seconds=1800, arguments=params)

        if exit_value == "NO_NEW_DATA":
            print(f"ℹ️  {table_id} — no new silver data, watermark unchanged.")
        else:
            # exit_value is the new watermark returned by the child
            spark.sql(f"""
                UPDATE shopmetrics_ecommerce.metadata.gold_metadata
                SET silver_max_watermark = '{exit_value}',
                    updated_at           = CURRENT_TIMESTAMP()
                WHERE table_id = '{table_id}'
            """)
            print(f"✅ {table_id} complete — watermark advanced to {exit_value}")

    except Exception as e:
        print(f"❌ {table_id} FAILED: {e}")
        raise  # halt pipeline; watermark is never advanced past a failed run

# COMMAND ----------

print("\n" + "="*80)
print("GOLD LAYER PROCESSING COMPLETE")
print("="*80)
