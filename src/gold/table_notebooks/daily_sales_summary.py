# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("silver_max_watermark", "")
dbutils.widgets.text("table_name", "")

silver_max_timestamp = dbutils.widgets.get("silver_max_watermark")
full_table_name = dbutils.widgets.get("table_name")

silver_max_timestamp = silver_max_timestamp if silver_max_timestamp != '' else '1900-01-01 00:00:00'
print(f"Current silver_max_timestamp: {silver_max_timestamp}")

# COMMAND ----------

orders_fact_df = (spark.table('shopmetrics_ecommerce.silver.orders_clean')
                  .filter(f'silver_updated_at > "{silver_max_timestamp}"')
                )

if orders_fact_df.limit(1).count() == 0:
    print("No new or changed silver records. Skipping.")
    dbutils.notebook.exit("NO_NEW_DATA")

# Capture the new high-watermark BEFORE any joins/filters that might rename the column
new_watermark = orders_fact_df.agg(F.max("silver_updated_at")).first()[0]
print(f"New watermark will be: {new_watermark}")

new_or_changed_dates = (
  orders_fact_df
    .select('order_date')
    .distinct()
)

affected_orders_df = (
  orders_fact_df.alias('o')
    .join(new_or_changed_dates.alias('c'), on='order_date')
    .select('o.*')
)

# COMMAND ----------

dim_products_df = spark.table('shopmetrics_ecommerce.silver.dim_products')

join_df = (
  affected_orders_df.alias('o')
    .join(dim_products_df.alias('p'), on='products_sk')
    .filter(F.lower('o.status') == 'completed')
    .select('o.order_date', dim_products_df['category'].alias('product_category'), 'o.total_amount', 'o.order_id')
    .groupBy('order_date', 'product_category')
    .agg(F.countDistinct('order_id').alias('total_orders'), F.sum('total_amount').alias('total_revenue'), F.avg('total_amount').alias('avg_revenue_per_order'))
    .withColumn('total_revenue', F.round('total_revenue', 2))
    .withColumn('avg_revenue_per_order', F.round('avg_revenue_per_order', 2))
    .withColumn('updated_at', F.current_timestamp())
)

if spark.catalog.tableExists(full_table_name):
  join_df.createOrReplaceTempView('incremental_source')

  merge_query = f"""
    MERGE INTO {full_table_name} t
    USING incremental_source s
    ON t.order_date = s.order_date AND t.product_category = s.product_category
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
  """

  spark.sql(merge_query)
else:
  join_df.write.mode('overwrite').saveAsTable(full_table_name)


# COMMAND ----------

dbutils.notebook.exit(str(new_watermark))
