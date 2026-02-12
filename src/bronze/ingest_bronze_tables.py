# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Streaming Ingestion Pipeline
# MAGIC
# MAGIC This notebook implements a robust streaming ingestion pipeline for the Bronze layer of the `shopmetrics_ecommerce` data platform. The pipeline ingests raw CSV data from cloud storage, enriches it with metadata, and appends it to Delta tables for downstream processing.
# MAGIC
# MAGIC ## Workflow Overview
# MAGIC
# MAGIC - **Metadata Tracking:** The pipeline reads ingestion configuration from the `shopmetrics_ecommerce.metadata.bronze_ingestion_tracking` table, which specifies source paths and table names.
# MAGIC - **Schema Management:** Schemas are inferred and stored per table, supporting schema evolution and rescue mode for unexpected columns.
# MAGIC - **Streaming Ingestion:** For each table, a streaming job is launched using Databricks Auto Loader (`cloudFiles`), reading CSV files and writing to the corresponding Bronze Delta table.
# MAGIC - **Enrichment:** Each batch is enriched with ingestion timestamp and file metadata (path, name, modification time).
# MAGIC - **Checkpointing & Archiving:** Checkpoints and archive paths are managed per table to ensure reliable and recoverable ingestion.
# MAGIC
# MAGIC ## Key Features
# MAGIC
# MAGIC - **Scalable ingestion** across multiple tables using a loop and parameterized batch processor.
# MAGIC - **Metadata enrichment** for auditability and traceability.
# MAGIC - **Schema evolution** support for flexible data onboarding.
# MAGIC - **Automated checkpointing** for fault tolerance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **Note:** This notebook is designed for use in Databricks and leverages PySpark and Delta Lake features for efficient streaming ingestion.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, input_file_name
from pyspark.sql import DataFrame

# COMMAND ----------

def create_batch_processor(target_table):
    """Returns a foreachBatch-compatible function with params baked in."""
    
    def batch_processor(batch_df: DataFrame, batch_id: int):
        enriched_df = (batch_df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file_path", batch_df["_metadata.file_path"])
            .withColumn("source_file_name", batch_df["_metadata.file_name"])
            .withColumn("source_file_modified_at", batch_df["_metadata.file_modification_time"])
            .drop("_metadata")
        )
        enriched_df.write.mode("append").saveAsTable(target_table)
    
    return batch_processor

# COMMAND ----------

bronze_mdb_df = spark.table('shopmetrics_ecommerce.metadata.bronze_ingestion_tracking')

for row in bronze_mdb_df.collect():

    table_name = f"{row['table_name']}"
    table_path = row['table_path']
    schema_path = f"/Volumes/shopmetrics_ecommerce/bronze/raw_data/schemas/{row['table_name']}"
    archive_path = f"/Volumes/shopmetrics_ecommerce/bronze/raw_data/archive/{row['table_name']}"
    checkpoint_path = f"/Volumes/shopmetrics_ecommerce/bronze/raw_data/checkpoints/{row['table_name']}"

    print("\n" + "="*60)
    print(f"🚀 Starting ingestion for table: {table_name}")
    print(f"   Source path:      {table_path}")
    print(f"   Schema path:      {schema_path}")
    print(f"   Archive path:     {archive_path}")
    print(f"   Checkpoint path:  {checkpoint_path}")
    print("="*60 + "\n")

    batch_processor = create_batch_processor(
        target_table=f"shopmetrics_ecommerce.bronze.{table_name}"
    )

    (
        spark.readStream.
        format('cloudFiles').
        option('cloudFiles.format', 'csv').
        option("cloudFiles.inferColumnTypes", "true").
        option("cloudFiles.schemaLocation", schema_path).
        option("cloudFiles.schemaEvolutionMode", "rescue").
        load(table_path)
      .writeStream
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .foreachBatch(batch_processor)
        .start()
        .awaitTermination()
    )

    print(f"✅ {table_name} ingestion complete\n")

