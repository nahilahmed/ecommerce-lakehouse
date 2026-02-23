# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Clickstream Sessionization
# MAGIC **FR-010** | Reads `ecommerce.bronze.clickstream_raw`, reconstructs user sessions
# MAGIC using a 30-minute inactivity window, and MERGEs results into
# MAGIC `ecommerce.silver.clickstream_sessions`.
# MAGIC
# MAGIC ## Algorithm
# MAGIC 1. Read watermark — last `ingested_at` from Bronze already processed
# MAGIC 2. Find customers with new Bronze events since the watermark
# MAGIC 3. Read ALL events for those customers (needed to correctly detect session boundaries)
# MAGIC 4. Apply LAG window function to detect 30-min gaps → session boundaries
# MAGIC 5. Assign deterministic session IDs via cumulative sum + MD5 hash
# MAGIC 6. Aggregate events into session metrics
# MAGIC 7. MERGE into `clickstream_sessions` on `session_id`
# MAGIC 8. Update watermark
# MAGIC
# MAGIC ## Why MERGE over partition overwrite
# MAGIC Sessions can span day boundaries and are mutable while open (new events extend them).
# MAGIC MERGE on `session_id` correctly updates open sessions and inserts new ones
# MAGIC without touching unaffected rows. Partition overwrite would require overwriting
# MAGIC a rolling window of uncertain size to catch cross-day sessions.
# MAGIC
# MAGIC ## Idempotency (NFR-007)
# MAGIC Session IDs are deterministic — derived from `MD5(customer_id || session_number)`.
# MAGIC Re-running for the same customers produces the same session IDs, so MERGE
# MAGIC updates existing rows rather than creating duplicates.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SOURCE_TABLE  = "ecommerce.bronze.clickstream_raw"
TARGET_TABLE  = "ecommerce.silver.clickstream_sessions"
WATERMARK_TABLE = "ecommerce.metadata.streaming_watermarks"

# 30-minute session inactivity threshold (BRD §7)
SESSION_GAP_MINUTES = 30

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermark setup
# MAGIC
# MAGIC `streaming_watermarks` is a small Delta table shared by all streaming Silver jobs.
# MAGIC It stores one row per table_id with the last processed `ingested_at` from Bronze.
# MAGIC On first run the table is created and the watermark is NULL (process everything).

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
        table_id    STRING  NOT NULL,
        last_watermark TIMESTAMP,
        updated_at  TIMESTAMP
    ) USING DELTA
    COMMENT 'Watermark tracking for streaming Silver jobs'
""")

watermark_rows = (
    spark.table(WATERMARK_TABLE)
    .filter(F.col("table_id") == "silver_clickstream_sessions")
    .collect()
)

last_watermark = watermark_rows[0]["last_watermark"] if watermark_rows else None

if last_watermark:
    print(f"Watermark: processing Bronze events ingested after {last_watermark}")
else:
    print("Watermark: first run — processing all Bronze events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Find affected customers
# MAGIC
# MAGIC Only customers who have new events since the watermark need reprocessing.
# MAGIC We identify them first so we can then pull ALL their events (not just the new ones)
# MAGIC for correct session boundary detection.

# COMMAND ----------

bronze = spark.table(SOURCE_TABLE)

# New events since last watermark
new_events = (
    bronze.filter(F.col("ingested_at") > last_watermark)
    if last_watermark
    else bronze
)

new_events_count = new_events.count()

if new_events_count == 0:
    print("No new Bronze events since last watermark. Nothing to process.")
    dbutils.notebook.exit("NO_NEW_DATA")

print(f"New Bronze events to process: {new_events_count}")

# Distinct customers who have new events
affected_customers = new_events.select("customer_id").distinct()
affected_count = affected_customers.count()
print(f"Affected customers: {affected_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Read ALL events for affected customers
# MAGIC
# MAGIC A new event for customer #4201 might extend their existing open session or start
# MAGIC a new one. To determine which, we need to see ALL their previous events too.
# MAGIC We join the full Bronze table against the affected customer list.

# COMMAND ----------

all_events_for_affected = (
    bronze
    .join(affected_customers, on="customer_id", how="inner")
    .select("customer_id", "event_id", "event_type", "page", "event_ts")
    # Drop any rows with null event_ts — can't sessionize without a timestamp
    .filter(F.col("event_ts").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Detect session boundaries with LAG
# MAGIC
# MAGIC For each customer, sort events by time. Compare each event's timestamp to the
# MAGIC previous event's timestamp using LAG(). If the gap exceeds 30 minutes (or there
# MAGIC is no previous event), mark it as the start of a new session.
# MAGIC
# MAGIC ```
# MAGIC customer | event_ts | prev_ts  | gap_min | is_session_start
# MAGIC 1042     | 10:00    | null     | null    | 1  ← first event ever
# MAGIC 1042     | 10:05    | 10:00    | 5       | 0
# MAGIC 1042     | 10:20    | 10:05    | 15      | 0
# MAGIC 1042     | 10:52    | 10:20    | 32      | 1  ← gap > 30 min
# MAGIC ```

# COMMAND ----------

customer_time_window = Window.partitionBy("customer_id").orderBy("event_ts")

events_with_gap = (
    all_events_for_affected
    .withColumn(
        "prev_event_ts",
        F.lag("event_ts").over(customer_time_window)
    )
    .withColumn(
        "gap_minutes",
        (
            F.col("event_ts").cast("long") - F.col("prev_event_ts").cast("long")
        ) / 60
    )
    .withColumn(
        "is_session_start",
        F.when(
            F.col("prev_event_ts").isNull() |       # first event for this customer
            (F.col("gap_minutes") > SESSION_GAP_MINUTES),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Assign deterministic session IDs
# MAGIC
# MAGIC Cumulative sum of `is_session_start` gives each session a monotonically
# MAGIC increasing integer per customer (1, 2, 3 ...).
# MAGIC
# MAGIC MD5(customer_id || session_number) produces a stable, deterministic session_id.
# MAGIC Re-running for the same events produces the same IDs → safe for MERGE.
# MAGIC
# MAGIC ```
# MAGIC customer | session_num | session_id
# MAGIC 1042     | 1           | md5("1042_1") = "a3f..."
# MAGIC 1042     | 1           | md5("1042_1") = "a3f..."  (same session)
# MAGIC 1042     | 2           | md5("1042_2") = "b7c..."  (new session after gap)
# MAGIC ```

# COMMAND ----------

events_with_session = (
    events_with_gap
    .withColumn(
        "session_num",
        F.sum("is_session_start").over(
            customer_time_window.rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )
        )
    )
    .withColumn(
        "session_id",
        F.md5(F.concat_ws("_", F.col("customer_id").cast("string"), F.col("session_num")))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Aggregate events into sessions
# MAGIC
# MAGIC Group by (customer_id, session_id) to compute session-level metrics.
# MAGIC `session_duration_secs` = time from first to last event in the session.
# MAGIC `converted` = True if a purchase event occurred in the session.

# COMMAND ----------

sessions = (
    events_with_session
    .groupBy("customer_id", "session_id")
    .agg(
        F.min("event_ts").alias("first_event_ts"),
        F.max("event_ts").alias("last_event_ts"),
        F.count("*").alias("event_count"),
        F.sum(
            F.when(F.col("event_type") == "page_view",   1).otherwise(0)
        ).alias("page_view_count"),
        F.sum(
            F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
        ).alias("add_to_cart_count"),
        F.sum(
            F.when(F.col("event_type") == "purchase",    1).otherwise(0)
        ).alias("purchase_count"),
    )
    .withColumn(
        "session_duration_secs",
        F.col("last_event_ts").cast("long") - F.col("first_event_ts").cast("long")
    )
    .withColumn(
        "converted",
        F.col("purchase_count") > 0
    )
    .withColumn("session_date", F.to_date(F.col("first_event_ts")))
    .withColumn("ingested_at",  F.current_timestamp())
)

session_count = sessions.count()
print(f"Sessions to merge: {session_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — MERGE into clickstream_sessions
# MAGIC
# MAGIC `WHEN MATCHED` — session already exists (was open, now has more events) → update metrics
# MAGIC `WHEN NOT MATCHED` — brand new session → insert
# MAGIC
# MAGIC Sessions belonging to customers with no new events are untouched.

# COMMAND ----------

# Create target table on first run
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        session_id              STRING  NOT NULL,
        customer_id             INT     NOT NULL,
        session_date            DATE,
        first_event_ts          TIMESTAMP,
        last_event_ts           TIMESTAMP,
        session_duration_secs   LONG,
        event_count             INT,
        page_view_count         INT,
        add_to_cart_count       INT,
        purchase_count          INT,
        converted               BOOLEAN,
        ingested_at             TIMESTAMP
    ) USING DELTA
    COMMENT 'Sessionized clickstream events — 30-min inactivity window (FR-010)'
""")

target = DeltaTable.forName(spark, TARGET_TABLE)

(
    target.alias("tgt")
    .merge(
        sessions.alias("src"),
        "tgt.session_id = src.session_id"
    )
    .whenMatchedUpdate(set={
        "last_event_ts":          "src.last_event_ts",
        "session_duration_secs":  "src.session_duration_secs",
        "event_count":            "src.event_count",
        "page_view_count":        "src.page_view_count",
        "add_to_cart_count":      "src.add_to_cart_count",
        "purchase_count":         "src.purchase_count",
        "converted":              "src.converted",
        "ingested_at":            "src.ingested_at",
    })
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"✅ MERGE complete — {session_count} sessions processed into {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Update watermark

# COMMAND ----------

new_watermark = new_events.agg(F.max("ingested_at")).collect()[0][0]

(
    DeltaTable.forName(spark, WATERMARK_TABLE).alias("tgt")
    .merge(
        spark.createDataFrame(
            [("silver_clickstream_sessions", new_watermark, __import__("datetime").datetime.utcnow())],
            ["table_id", "last_watermark", "updated_at"]
        ).alias("src"),
        "tgt.table_id = src.table_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"✅ Watermark updated to {new_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("\n=== Run Summary ===")
print(f"New Bronze events processed : {new_events_count}")
print(f"Affected customers          : {affected_count}")
print(f"Sessions merged             : {session_count}")
print(f"New watermark               : {new_watermark}")
print(f"\nVerify with:")
print(f"  SELECT COUNT(*), SUM(CAST(converted AS INT)) FROM {TARGET_TABLE}")
print(f"  SELECT * FROM {TARGET_TABLE} ORDER BY last_event_ts DESC LIMIT 10")
