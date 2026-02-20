# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("silver_max_watermark", "")
dbutils.widgets.text("table_name", "")

silver_max_timestamp = dbutils.widgets.get("silver_max_watermark")
full_table_name = dbutils.widgets.get("table_name")

silver_max_timestamp = silver_max_timestamp if silver_max_timestamp != '' else '1900-01-01 00:00:00'
print(f"Current silver_max_timestamp: {silver_max_timestamp}")

# COMMAND ----------

# DBTITLE 1,Incremental ETL: Detect new/changed silver records
orders_fact_df = (spark.table('shopmetrics_ecommerce.silver.orders_clean')
                  .filter(f'silver_updated_at > "{silver_max_timestamp}"')
                )

if orders_fact_df.limit(1).count() == 0:
    print("No new or changed silver records. Skipping.")
    dbutils.notebook.exit("NO_NEW_DATA")

# Capture the new high-watermark BEFORE any joins/filters that might rename the column
new_watermark = orders_fact_df.agg(F.max("silver_updated_at")).first()[0]
print(f"New watermark will be: {new_watermark}")

# COMMAND ----------

# DBTITLE 1,Find affected categories (category_rank must be recomputed across full category)
dim_products_df = spark.table('shopmetrics_ecommerce.silver.dim_products')

# Identify which categories had any changed products
affected_categories = (
    orders_fact_df.select('products_sk').distinct()
    .join(dim_products_df.alias('p'), on='products_sk')
    .select('p.category')
    .distinct()
)

# All products_sk that belong to those affected categories
products_in_affected_cats = (
    dim_products_df.alias('p')
    .join(affected_categories.alias('c'), on='category')
    .select('p.products_sk')
)

# Full order history for every product in affected categories
full_orders_fact_df = spark.table('shopmetrics_ecommerce.silver.orders_clean')
affected_orders_df = (
    full_orders_fact_df.alias('o')
    .join(products_in_affected_cats.alias('p'), on='products_sk')
    .select('o.*')
)

# COMMAND ----------

# DBTITLE 1,Aggregate and rank
agg_df = (
    affected_orders_df.alias('o')
    .join(dim_products_df.alias('p'), on='products_sk')
    .filter(F.lower('o.status') == 'completed')
    .groupBy('p.product_id', 'p.product_name', 'p.category')
    .agg(
        F.countDistinct('o.order_id').alias('units_sold'),
        F.sum('o.total_amount').alias('total_revenue'),
    )
    .withColumn('total_revenue', F.round('total_revenue', 2))
    .withColumn('updated_at', F.current_timestamp())
)

# Rank products within each category by total revenue (highest = rank 1)
category_window = Window.partitionBy('category').orderBy(F.desc('total_revenue'))
join_df = agg_df.withColumn('category_rank', F.rank().over(category_window))

display(join_df)

# COMMAND ----------

# DBTITLE 1,Merge incremental results into gold table
if spark.catalog.tableExists(full_table_name):
    join_df.createOrReplaceTempView('incremental_source')

    merge_query = f"""
        MERGE INTO {full_table_name} t
        USING incremental_source s
        ON t.product_id = s.product_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """

    spark.sql(merge_query).show()
else:
    join_df.write.mode('overwrite').saveAsTable(full_table_name)

# COMMAND ----------

dbutils.notebook.exit(str(new_watermark))
