# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Streaming — Clickstream Ingestion
# MAGIC **FR-010** | Reads `clickstream-events` from Confluent Kafka and writes raw events
# MAGIC to `ecommerce.bronze.clickstream_raw` as an append-only Delta table.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC 1. Reads a stream of JSON messages from Kafka (bytes)
# MAGIC 2. Parses each message against the declared schema
# MAGIC 3. Routes malformed messages to a dead-letter table instead of crashing
# MAGIC 4. Adds audit columns: `kafka_offset`, `ingested_at`
# MAGIC 5. Appends valid events to `ecommerce.bronze.clickstream_raw`
# MAGIC
# MAGIC ## Free Edition constraints applied
# MAGIC - `trigger(availableNow=True)` — only supported trigger type
# MAGIC - Explicit `checkpointLocation` — implicit temp paths not allowed
# MAGIC
# MAGIC ## How to run
# MAGIC Fill in the three credential widgets and click Run All.
# MAGIC Schedule this notebook as a Databricks Job to control latency (NFR-002).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("kafka_bootstrap_server", "", "Bootstrap Server")
dbutils.widgets.text("kafka_api_key",          "", "API Key")
dbutils.widgets.text("kafka_api_secret",        "", "API Secret")

bootstrap  = dbutils.widgets.get("kafka_bootstrap_server")
api_key    = dbutils.widgets.get("kafka_api_key")
api_secret = dbutils.widgets.get("kafka_api_secret")

assert bootstrap,  "kafka_bootstrap_server widget is empty"
assert api_key,    "kafka_api_key widget is empty"
assert api_secret, "kafka_api_secret widget is empty"

# Target tables (Unity Catalog)
TARGET_TABLE      = "ecommerce.bronze.clickstream_raw"
DEAD_LETTER_TABLE = "ecommerce.bronze.clickstream_dead_letter"

# Checkpoint stored in Unity Catalog volume (NFR-007 — survives restarts)
CHECKPOINT_PATH = "/Volumes/ecommerce/bronze/raw_data/checkpoints/clickstream_bronze"

TOPIC = "clickstream-events"

print(f"Bootstrap : {bootstrap}")
print(f"API Key   : {api_key[:6]}{'*' * (len(api_key) - 6)}")
print(f"Topic     : {TOPIC}")
print(f"Target    : {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema declaration
# MAGIC
# MAGIC Spark needs to know the shape of the JSON **before** it reads any data.
# MAGIC This must exactly match what `produce_clickstream.py` sends.
# MAGIC If the producer adds a new field, add it here and reset the checkpoint.

# COMMAND ----------

# JSON payload schema — mirrors produce_clickstream.py event dict
# product_id is nullable because search events don't have a product
EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),  nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("product_id",  IntegerType(), nullable=True),
    StructField("event_type",  StringType(),  nullable=False),
    StructField("session_id",  StringType(),  nullable=False),
    StructField("page",        StringType(),  nullable=False),
    StructField("event_ts",    StringType(),  nullable=False),  # parsed to timestamp below
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read stream from Kafka
# MAGIC
# MAGIC `readStream` returns a streaming DataFrame — nothing is read yet.
# MAGIC Spark only contacts Kafka when the query starts (`.start()`).
# MAGIC
# MAGIC At this point the DataFrame has Kafka's raw schema:
# MAGIC   key (binary), value (binary), topic, partition, offset, timestamp

# COMMAND ----------

jaas = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{api_key}" password="{api_secret}";'
)

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",  bootstrap)
    .option("kafka.security.protocol",  "SASL_SSL")
    .option("kafka.sasl.mechanism",     "PLAIN")
    .option("kafka.sasl.jaas.config",   jaas)
    .option("subscribe",                TOPIC)
    # Start from earliest on first run; checkpoint tracks offset on subsequent runs
    .option("startingOffsets",          "earliest")
    # Kafka's broker-side timestamp — used for watermarking late events
    .option("kafka.metadata.max.age.ms", "300000")
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse JSON and add audit columns
# MAGIC
# MAGIC Step 1 — cast value bytes → string
# MAGIC Step 2 — parse JSON string → struct using EVENT_SCHEMA
# MAGIC Step 3 — flatten struct columns to top level
# MAGIC Step 4 — cast event_ts string → timestamp
# MAGIC Step 5 — apply 10-min watermark on event_ts (BRD §5, NFR-003)
# MAGIC           Watermark tells Spark: discard events arriving >10 min late.
# MAGIC           Required for correct sessionization in Silver (Day 13).
# MAGIC Step 6 — add kafka_offset and ingested_at audit columns

# COMMAND ----------

parsed = (
    raw_stream
    # Step 1 — bytes to string
    .withColumn("value_str", F.col("value").cast("string"))
    # Step 2 — parse JSON; fields that don't match schema land in _corrupt_record
    .withColumn("data", F.from_json(F.col("value_str"), EVENT_SCHEMA))
    # Step 3 — split into valid vs malformed rows
    # A row is malformed if from_json couldn't populate the required fields
    .withColumn(
        "_is_valid",
        F.col("data.event_id").isNotNull() & F.col("data.customer_id").isNotNull()
    )
)

# Split here — valid rows go to clickstream_raw, bad rows go to dead letter
valid_stream = (
    parsed
    .filter(F.col("_is_valid"))
    # Step 3 — flatten struct
    .select(
        F.col("data.event_id").alias("event_id"),
        F.col("data.customer_id").alias("customer_id"),
        F.col("data.product_id").alias("product_id"),
        F.col("data.event_type").alias("event_type"),
        F.col("data.session_id").alias("session_id"),
        F.col("data.page").alias("page"),
        # Step 4 — parse ISO 8601 string to timestamp
        F.to_timestamp(F.col("data.event_ts")).alias("event_ts"),
        # Step 6 — Kafka metadata audit columns
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
        F.current_timestamp().alias("ingested_at"),
    )
    # Step 5 — 10-min watermark for late event handling (BRD requirement)
    # Silver sessionization depends on this being set correctly here
    .withWatermark("event_ts", "10 minutes")
)

dead_letter_stream = (
    parsed
    .filter(~F.col("_is_valid"))
    .select(
        F.col("value_str").alias("raw_payload"),
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.lit("parse_failure").alias("failure_reason"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write valid events to Bronze Delta table
# MAGIC
# MAGIC `foreachBatch` gives us a plain batch DataFrame on each micro-batch,
# MAGIC which lets us use `.write.mode("append").saveAsTable()` — the same
# MAGIC pattern as the batch bronze ingestion script.
# MAGIC
# MAGIC `trigger(availableNow=True)` — Free Edition constraint.
# MAGIC Reads all unprocessed Kafka offsets, writes them, then stops.
# MAGIC The checkpoint ensures the next run picks up exactly where this one left off.

# COMMAND ----------

def write_batch(batch_df, batch_id):
    """Append micro-batch to the Bronze Delta table."""
    row_count = batch_df.count()
    if row_count == 0:
        print(f"  Batch {batch_id}: no new events.")
        return
    batch_df.write.mode("append").saveAsTable(TARGET_TABLE)
    print(f"  Batch {batch_id}: wrote {row_count} rows to {TARGET_TABLE}")


main_query = (
    valid_stream
    .writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write malformed events to dead-letter table
# MAGIC
# MAGIC Instead of silently dropping bad messages or crashing the stream,
# MAGIC we route them to a separate table for investigation.
# MAGIC This is a standard production pattern.

# COMMAND ----------

dead_letter_query = (
    dead_letter_stream
    .writeStream
    .foreachBatch(
        lambda df, id: df.write.mode("append").saveAsTable(DEAD_LETTER_TABLE)
        if df.count() > 0 else None
    )
    .option("checkpointLocation", CHECKPOINT_PATH + "_dead_letter")
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for completion and report

# COMMAND ----------

main_query.awaitTermination()
dead_letter_query.awaitTermination()

main_progress      = main_query.lastProgress
dead_letter_progress = dead_letter_query.lastProgress

print("\n=== Run complete ===")
print(f"Valid events   — numInputRows: {main_progress.get('numInputRows') if main_progress else 'n/a'}")
print(f"Dead-letter    — numInputRows: {dead_letter_progress.get('numInputRows') if dead_letter_progress else 'n/a'}")
print(f"\nVerify with:")
print(f"  SELECT COUNT(*) FROM {TARGET_TABLE}")
print(f"  SELECT COUNT(*) FROM {DEAD_LETTER_TABLE}")
