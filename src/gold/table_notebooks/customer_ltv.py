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

new_or_changed_customers = (
  orders_fact_df
    .select('customer_sk')
    .distinct()
)


# Read full fact table for affected customer_sk values
full_orders_fact_df = spark.table('shopmetrics_ecommerce.silver.orders_clean')
affected_orders_cust_df = (
  full_orders_fact_df.alias('o')
    .join(new_or_changed_customers.alias('c'), on='customer_sk')
    .select('o.*')
)

# COMMAND ----------

dim_customers_df = spark.table('shopmetrics_ecommerce.silver.dim_customers')

join_df = (
  affected_orders_cust_df.alias('o')
    .join(dim_customers_df.alias('c'), on='customer_sk')
    .filter(F.lower('o.status') == 'completed')
    .groupBy('o.customer_sk', 'o.customer_id', 'c.name', 'c.email', 'c.region')
    .agg(
      F.countDistinct('o.order_id').alias('total_orders'),
      F.sum('o.total_amount').alias('total_revenue'),
      F.min('o.order_date').alias('first_order_date'),
      F.max('o.order_date').alias('last_order_date')
    )
    .withColumn('customer_ltv', F.col('total_revenue'))
    .withColumn(
      'ltv_segment',
      F.when(F.col('customer_ltv') >= 1000, 'High')
       .when(F.col('customer_ltv') >= 500, 'Medium')
       .otherwise('Low')
    )
    .withColumn('updated_at', F.current_timestamp())
    .drop('customer_ltv')
)

display(join_df)


if spark.catalog.tableExists(full_table_name):
  join_df.createOrReplaceTempView('incremental_source')

  merge_query = f"""
    MERGE INTO {full_table_name} t
    USING incremental_source s
    ON t.customer_sk = s.customer_sk
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
