# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Metadata Table Setup
# MAGIC
# MAGIC Creates metadata-driven configuration for Gold layer aggregations.
# MAGIC
# MAGIC **Aggregation Types:**
# MAGIC - `incremental_merge`: Find affected grain keys via `silver_updated_at`, recompute for those keys, MERGE into gold
# MAGIC - `full_recompute`: Recompute entire table from silver on every run (for cross-row metrics like rank)
# MAGIC
# MAGIC **Watermark:**
# MAGIC - `silver_max_watermark` tracks the highest `silver_updated_at` processed per gold table
# MAGIC - Enables late-arriving fact mutations (status changes on old orders) to propagate correctly

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS shopmetrics_ecommerce.metadata.gold_metadata (
# MAGIC   -- Identification
# MAGIC   table_id          STRING NOT NULL COMMENT 'Unique identifier: gold_daily_sales, gold_customer_ltv',
# MAGIC   table_name        STRING NOT NULL COMMENT 'Target gold table name: daily_sales_summary',
# MAGIC   notebook_name     STRING NOT NULL COMMENT 'Filename of table notebook in table_notebooks/ (no .py)',
# MAGIC
# MAGIC   -- Source Config
# MAGIC   source_tables     ARRAY<STRING> COMMENT 'Silver tables this gold table reads from',
# MAGIC
# MAGIC   -- Aggregation Config
# MAGIC   aggregation_type  STRING NOT NULL COMMENT 'incremental_merge | full_recompute',
# MAGIC
# MAGIC   -- Watermark (tracks last silver_updated_at successfully consumed by this gold table)
# MAGIC   silver_max_watermark TIMESTAMP COMMENT 'Last silver_updated_at processed — updated after each successful run',
# MAGIC
# MAGIC   -- Processing Config
# MAGIC   is_active         BOOLEAN DEFAULT TRUE COMMENT 'Enable/disable processing',
# MAGIC   processing_order  INT COMMENT 'Execution sequence (1, 2, 3...)',
# MAGIC
# MAGIC   -- Audit
# MAGIC   created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   created_by        STRING DEFAULT CURRENT_USER(),
# MAGIC
# MAGIC   CONSTRAINT pk_gold_metadata PRIMARY KEY (table_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Metadata configuration for Gold layer aggregation tables'

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE shopmetrics_ecommerce.metadata.gold_metadata
# MAGIC SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert metadata for all gold tables
# MAGIC INSERT INTO shopmetrics_ecommerce.metadata.gold_metadata
# MAGIC (table_id, table_name, notebook_name, source_tables, aggregation_type, is_active, processing_order, silver_max_watermark)
# MAGIC VALUES
# MAGIC   -- 1. Daily Sales Summary (FR-006)
# MAGIC   --    Grain: (order_date, category) — past dates are immutable, only recent dates change
# MAGIC   (
# MAGIC     'gold_daily_sales',
# MAGIC     'daily_sales_summary',
# MAGIC     'daily_sales_summary',
# MAGIC     array('shopmetrics_ecommerce.silver.orders_clean', 'shopmetrics_ecommerce.silver.dim_products'),
# MAGIC     'incremental_merge',
# MAGIC     TRUE,
# MAGIC     1,
# MAGIC     NULL
# MAGIC   )

# COMMAND ----------

# - 2. Customer LTV (FR-007)
#   --    Grain: customer_id — cumulative, only recompute affected customers
#   (
#     'gold_customer_ltv',
#     'customer_ltv',
#     'customer_ltv',
#     array('shopmetrics_ecommerce.silver.orders_clean'),
#     'incremental_merge',
#     array('customer_id'),
#     TRUE,
#     2,
#     NULL
#   ),

#   -- 3. Product Performance (FR-008)
#   --    Full recompute: category_rank is cross-product relative metric
#   (
#     'gold_product_performance',
#     'product_performance',
#     'product_performance',
#     array('shopmetrics_ecommerce.silver.orders_clean', 'shopmetrics_ecommerce.silver.dim_products'),
#     'full_recompute',
#     array('product_id'),
#     TRUE,
#     3,
#     NULL
#   )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify
# MAGIC SELECT table_id, table_name, aggregation_type, grain_columns, processing_order, silver_max_watermark
# MAGIC FROM shopmetrics_ecommerce.metadata.gold_metadata
# MAGIC ORDER BY processing_order
