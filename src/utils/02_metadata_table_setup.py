# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS shopmetrics_ecommerce.metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS shopmetrics_ecommerce.metadata.bronze_ingestion_tracking (
# MAGIC   table_name STRING,
# MAGIC   table_path STRING,
# MAGIC   last_ingested_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

spark.sql("""
    INSERT INTO shopmetrics_ecommerce.metadata.bronze_ingestion_tracking (table_name, table_path, last_ingested_timestamp)
    VALUES
        ('customers_raw', '/Volumes/shopmetrics_ecommerce/bronze/raw_data/customers/', NULL),
        ('orders_raw', '/Volumes/shopmetrics_ecommerce/bronze/raw_data/orders/', NULL),
        ('products_raw', '/Volumes/shopmetrics_ecommerce/bronze/raw_data/products/', NULL)
""")
