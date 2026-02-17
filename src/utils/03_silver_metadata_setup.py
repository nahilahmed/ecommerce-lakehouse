# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Metadata Table Setup
# MAGIC
# MAGIC Creates metadata-driven configuration for batch Silver layer transformations.
# MAGIC
# MAGIC **Transform Types:**
# MAGIC - `fact`: Fact tables with deduplication (e.g., orders_clean)
# MAGIC - `scd_type1`: Simple dimensions with overwrite, no history (e.g., dim_products)
# MAGIC - `scd_type2`: Dimensions with full history tracking (e.g., dim_customers)
# MAGIC
# MAGIC **Dimension Joins (for Fact Tables):**
# MAGIC - Maps natural keys → surrogate keys from dimension tables
# MAGIC - For SCD Type 2 dimensions: Uses `effective_date_col` for point-in-time joins
# MAGIC - Example: Order on 2024-01-15 joins with customer record effective on that date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create metadata schema if it doesn't exist
# MAGIC CREATE SCHEMA IF NOT EXISTS shopmetrics_ecommerce.metadata;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Silver metadata table
# MAGIC CREATE OR REPLACE TABLE shopmetrics_ecommerce.metadata.silver_metadata (
# MAGIC   -- Identification
# MAGIC   table_id STRING NOT NULL COMMENT 'Unique identifier: silver_orders, silver_dim_customers',
# MAGIC   table_name STRING NOT NULL COMMENT 'Target Silver table name: orders_clean, dim_customers',
# MAGIC   source_table STRING NOT NULL COMMENT 'Source Bronze table: bronze.orders_raw',
# MAGIC
# MAGIC   -- Transformation Type
# MAGIC   transform_type STRING NOT NULL COMMENT 'fact | scd_type1 | scd_type2',
# MAGIC
# MAGIC   -- Deduplication Config (for transform_type = 'fact' or 'scd_type1')
# MAGIC   dedup_columns ARRAY<STRING> COMMENT 'Columns to deduplicate on: ["order_id"]',
# MAGIC   dedup_order_by STRING COMMENT 'Order logic for keeping records: "ingested_at DESC"',
# MAGIC
# MAGIC   -- SCD Type 2 Config (for transform_type = 'scd_type2')
# MAGIC   scd_business_key ARRAY<STRING> COMMENT 'Business key for SCD: ["customer_id"]',
# MAGIC   scd_date_col STRING COMMENT 'Date column for SCD: "order_date"',
# MAGIC   scd_compare_columns ARRAY<STRING> COMMENT 'Columns to track changes: ["email", "region"]',
# MAGIC   surrogate_key_column STRING COMMENT 'Surrogate key column name: customer_sk',
# MAGIC
# MAGIC   -- Column Transformations (JSON for flexibility)
# MAGIC   column_mappings STRING COMMENT 'JSON: {"bronze_col": "silver_col"} or pass-through',
# MAGIC   derived_columns STRING COMMENT 'JSON: {"new_col": "expression"} for calculated fields',
# MAGIC
# MAGIC   -- Dimension Joins (for fact tables)
# MAGIC   dimension_joins STRING COMMENT 'JSON array: [{"dim_table": "silver.dim_customers", "natural_key": "customer_id", "surrogate_key": "customer_sk", "effective_date_col": "order_date", "join_type": "left"}]',
# MAGIC
# MAGIC   -- Data Quality Rules
# MAGIC   dq_rules STRING COMMENT 'JSON array: [{"column": "order_id", "rule": "not_null", "threshold": 0.01}]',
# MAGIC
# MAGIC   -- Business Rules
# MAGIC   business_rules STRING COMMENT 'JSON: {"status": ["pending", "completed", "cancelled", "refunded"]}',
# MAGIC
# MAGIC   -- Processing Config
# MAGIC   is_active BOOLEAN DEFAULT TRUE COMMENT 'Enable/disable processing',
# MAGIC   processing_order INT COMMENT 'Execution sequence (1, 2, 3...)',
# MAGIC   dependencies ARRAY<STRING> COMMENT 'Table IDs that must complete first: ["silver_dim_customers"]',
# MAGIC
# MAGIC   -- Bronze Watermark Store
# MAGIC   bronze_max_watermark TIMESTAMP COMMENT 'Latest ingestion timestamp for source table',
# MAGIC
# MAGIC   -- Audit
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Metadata creation timestamp',
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Last metadata update timestamp',
# MAGIC   created_by STRING DEFAULT CURRENT_USER() COMMENT 'User who created this metadata',
# MAGIC
# MAGIC   CONSTRAINT pk_silver_metadata PRIMARY KEY (table_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Metadata configuration for batch Silver layer transformations (excludes streaming tables)';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE shopmetrics_ecommerce.metadata.silver_metadata
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.feature.allowColumnDefaults' = 'enabled'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert metadata for batch Silver tables
# MAGIC INSERT INTO shopmetrics_ecommerce.metadata.silver_metadata
# MAGIC (
# MAGIC   table_id,
# MAGIC   table_name,
# MAGIC   source_table,
# MAGIC   transform_type,
# MAGIC   dedup_columns,
# MAGIC   dedup_order_by,
# MAGIC   scd_business_key,
# MAGIC   scd_date_col,
# MAGIC   scd_compare_columns,
# MAGIC   surrogate_key_column,
# MAGIC   column_mappings,
# MAGIC   derived_columns,
# MAGIC   dimension_joins,
# MAGIC   dq_rules,
# MAGIC   business_rules,
# MAGIC   is_active,
# MAGIC   processing_order,
# MAGIC   dependencies,
# MAGIC   bronze_max_watermark
# MAGIC )
# MAGIC VALUES
# MAGIC   -- 1. Orders (Fact Table with Deduplication)
# MAGIC   (
# MAGIC     'silver_orders',
# MAGIC     'orders_clean',
# MAGIC     'shopmetrics_ecommerce.bronze.orders_raw',
# MAGIC     'fact',
# MAGIC     array('order_id'),
# MAGIC     'ingested_at DESC',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     '{"order_id": "order_id", "customer_id": "customer_id", "product_id": "product_id", "order_date": "order_date", "total_amount": "total_amount", "status": "status"}',
# MAGIC     NULL,
# MAGIC     '[{"dim_table": "shopmetrics_ecommerce.silver.dim_customers", "natural_key": "customer_id", "surrogate_key": "customer_sk", "effective_date_col": "order_date", "join_type": "left"}, {"dim_table": "shopmetrics_ecommerce.silver.dim_products", "natural_key": "product_id", "surrogate_key": "product_sk", "join_type": "left"}]',
# MAGIC     '[{"column": "order_id", "rule": "not_null", "threshold": 0.01}, {"column": "total_amount", "rule": "positive"}]',
# MAGIC     '{"status": ["pending", "completed", "cancelled", "refunded"]}',
# MAGIC     TRUE,
# MAGIC     3,
# MAGIC     array('silver_dim_customers', 'silver_dim_products'),
# MAGIC     NULL
# MAGIC   ),
# MAGIC
# MAGIC   -- 2. Customers (SCD Type 2 Dimension)
# MAGIC   (
# MAGIC     'silver_dim_customers',
# MAGIC     'dim_customers',
# MAGIC     'shopmetrics_ecommerce.bronze.customers_raw',
# MAGIC     'scd_type2',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     array('customer_id'),
# MAGIC     'customer_modified_at',
# MAGIC     array('email', 'region'),
# MAGIC     'customer_sk',
# MAGIC     '{"customer_id": "customer_id", "email": "email", "region": "region", "signup_date": "signup_date"}',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     '[{"column": "customer_id", "rule": "not_null", "threshold": 0}]',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     1,
# MAGIC     array(),
# MAGIC     NULL
# MAGIC   ),
# MAGIC
# MAGIC   -- 3. Products (SCD Type 1 Dimension - Simple Overwrite)
# MAGIC   (
# MAGIC     'silver_dim_products',
# MAGIC     'dim_products',
# MAGIC     'shopmetrics_ecommerce.bronze.products_raw',
# MAGIC     'scd_type1',
# MAGIC     array('product_id'),
# MAGIC     'ingested_at DESC',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'products_sk',
# MAGIC     '{"product_id": "product_id", "product_name": "product_name", "category": "category", "price": "price"}',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     '[{"column": "product_id", "rule": "not_null", "threshold": 0}]',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     2,
# MAGIC     array(),
# MAGIC     NULL
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the metadata table
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM shopmetrics_ecommerce.metadata.silver_metadata
# MAGIC ORDER BY processing_order;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata Table Created ✅
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Create generic Silver transformation notebook that reads this metadata
# MAGIC 2. Implement transform logic for each `transform_type`:
# MAGIC    - `fact`: Deduplication + DQ checks + business rules validation
# MAGIC    - `scd_type1`: Deduplication + merge (overwrite existing records)
# MAGIC    - `scd_type2`: Full history tracking with effective dates
# MAGIC 3. Orchestrate via Databricks Job using `processing_order`
