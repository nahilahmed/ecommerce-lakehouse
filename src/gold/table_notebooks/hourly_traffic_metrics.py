# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Hourly Traffic Metrics
# MAGIC **FR-009** | Aggregates `clickstream_sessions` into per-hour traffic metrics.
# MAGIC Called by `ingest_gold_tables.py` — do not run standalone.
# MAGIC
# MAGIC ## Metrics
# MAGIC | Metric | Definition |
# MAGIC |--------|------------|
# MAGIC | `page_views` | Sum of page_view events across all sessions in the hour |
# MAGIC | `unique_visitors` | Distinct customers who had a session starting in the hour |
# MAGIC | `total_sessions` | Count of sessions starting in the hour |
# MAGIC | `add_to_cart_rate` | Sessions with ≥1 add_to_cart / total sessions |
# MAGIC | `purchase_rate` | Sessions that converted / total sessions |
# MAGIC
# MAGIC ## Watermark logic
# MAGIC Identifies hours affected by new/updated Silver sessions (`ingested_at > watermark`),
# MAGIC then re-aggregates ALL sessions for those hours to produce correct totals.
# MAGIC This handles open sessions that were extended since the last run.

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets — passed from Gold master orchestration notebook

# COMMAND ----------

dbutils.widgets.text("silver_max_watermark", "")
dbutils.widgets.text("table_name", "")

silver_max_watermark = dbutils.widgets.get("silver_max_watermark")
full_table_name      = dbutils.widgets.get("table_name")

# Default to epoch so first run processes everything
silver_max_watermark = silver_max_watermark if silver_max_watermark != "" else "1900-01-01 00:00:00"

print(f"silver_max_watermark : {silver_max_watermark}")
print(f"target table         : {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Find sessions updated since watermark
# MAGIC
# MAGIC `ingested_at` on `clickstream_sessions` is updated on every MERGE from Silver,
# MAGIC so it reliably reflects sessions that are new or were extended since last run.

# COMMAND ----------

sessions = spark.table("shopmetrics_ecommerce.silver.clickstream_sessions")

new_sessions = sessions.filter(f'ingested_at > "{silver_max_watermark}"')

if new_sessions.limit(1).count() == 0:
    print("No new or updated sessions since last watermark. Skipping.")
    dbutils.notebook.exit("NO_NEW_DATA")

# Capture new watermark before any downstream transforms
new_watermark = new_sessions.agg(F.max("ingested_at")).first()[0]
print(f"New watermark will be: {new_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Find affected hours
# MAGIC
# MAGIC `event_hour` = the hour bucket a session started in (truncated to the hour).
# MAGIC We only re-aggregate hours that have at least one new/updated session.
# MAGIC Then we pull ALL sessions for those hours for correct totals — same pattern
# MAGIC as `daily_sales_summary` re-aggregating all orders for affected dates.

# COMMAND ----------

affected_hours = (
    new_sessions
    .withColumn("event_hour", F.date_trunc("hour", F.col("first_event_ts")))
    .select("event_hour")
    .distinct()
)

print(f"Affected hour buckets: {affected_hours.count()}")

# All sessions whose hour bucket is in the affected set
all_sessions_for_affected_hours = (
    sessions
    .withColumn("event_hour", F.date_trunc("hour", F.col("first_event_ts")))
    .join(affected_hours, on="event_hour", how="inner")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Aggregate into hourly metrics
# MAGIC
# MAGIC All rates are session-based (not raw event counts) so they reflect
# MAGIC actual user intent rather than bot/refresh noise:
# MAGIC - `add_to_cart_rate` = sessions where someone actually added something / total sessions
# MAGIC - `purchase_rate`    = sessions that completed a purchase / total sessions
# MAGIC
# MAGIC Both are rounded to 4 decimal places (e.g. 0.1234 = 12.34%).

# COMMAND ----------

hourly_metrics = (
    all_sessions_for_affected_hours
    .groupBy("event_hour")
    .agg(
        # Raw volume
        F.sum("page_view_count").alias("page_views"),
        F.countDistinct("customer_id").alias("unique_visitors"),
        F.count("session_id").alias("total_sessions"),

        # Funnel metrics — session-based rates
        F.round(
            F.sum(F.when(F.col("add_to_cart_count") > 0, 1).otherwise(0)) /
            F.count("session_id"),
            4
        ).alias("add_to_cart_rate"),

        F.round(
            F.sum(F.when(F.col("converted") == True, 1).otherwise(0)) /
            F.count("session_id"),
            4
        ).alias("purchase_rate"),

        # Avg session duration for the hour
        F.round(F.avg("session_duration_secs"), 1).alias("avg_session_duration_secs"),
    )
    .withColumn("updated_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — MERGE into Gold table
# MAGIC
# MAGIC Merge key: `event_hour` — one row per hour, updated on each run.
# MAGIC `WHEN MATCHED` overwrites all metrics for that hour (re-aggregation is full, not incremental).
# MAGIC `WHEN NOT MATCHED` inserts brand new hours.

# COMMAND ----------

if spark.catalog.tableExists(full_table_name):
    hourly_metrics.createOrReplaceTempView("incremental_source")

    spark.sql(f"""
        MERGE INTO {full_table_name} t
        USING incremental_source s
        ON t.event_hour = s.event_hour
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    print(f"✅ MERGE complete into {full_table_name}")

else:
    # First run — create the table from the aggregation result
    hourly_metrics.write.mode("overwrite").saveAsTable(full_table_name)
    print(f"✅ Table created and populated: {full_table_name}")

hourly_metrics.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Return new watermark to master notebook

# COMMAND ----------

dbutils.notebook.exit(str(new_watermark))
