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
TARGET_TABLE      = "shopmetrics_ecommerce.bronze.clickstream_raw"
DEAD_LETTER_TABLE = "shopmetrics_ecommerce.bronze.clickstream_dead_letter"

# Checkpoint stored in Unity Catalog volume (NFR-007 — survives restarts)
CHECKPOINT_PATH = "/Volumes/shopmetrics_ecommerce/bronze/raw_data/checkpoints/clickstream_bronze"

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

raw_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse JSON
# MAGIC
# MAGIC Step 1 — cast value bytes → string
# MAGIC Step 2 — parse JSON string → struct using EVENT_SCHEMA
# MAGIC Step 3 — tag each row as valid or malformed
# MAGIC
# MAGIC Note on watermark: Bronze just appends — no windowed aggregation happens here.
# MAGIC The 10-min watermark belongs in Silver's sessionization query (Day 13),
# MAGIC applied when Silver reads from this Delta table.

# COMMAND ----------

parsed = (
    raw_stream
    # Step 1 — bytes to string
    .withColumn("value_str", F.col("value").cast("string"))
    # Step 2 — parse JSON; unrecognised fields are silently dropped,
    # required fields that are missing produce null
    .withColumn("data", F.from_json(F.col("value_str"), EVENT_SCHEMA))
    # Step 3 — validity flag: both required fields must be non-null
    .withColumn(
        "_is_valid",
        F.col("data.event_id").isNotNull() & F.col("data.customer_id").isNotNull()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Route and write — single foreachBatch
# MAGIC
# MAGIC **Why one query instead of two:**
# MAGIC Two separate writeStream queries on the same readStream source would each
# MAGIC open an independent Kafka consumer, reading every message twice.
# MAGIC Instead, a single foreachBatch reads Kafka once per micro-batch and routes
# MAGIC valid rows to clickstream_raw and malformed rows to the dead-letter table
# MAGIC inside the same function — one Kafka read, two Delta writes.
# MAGIC
# MAGIC Inside foreachBatch the DataFrame is a plain batch (not streaming),
# MAGIC so standard `.write.mode("append").saveAsTable()` works directly.

# COMMAND ----------

def route_batch(batch_df, batch_id):
    """
    Route one micro-batch to valid or dead-letter table.
    Called once per micro-batch by Spark — batch_df is a regular batch DataFrame.
    """
    # Split the batch on the validity flag computed in the parsed stream
    valid_df = (
        batch_df
        .filter(F.col("_is_valid"))
        .select(
            F.col("data.event_id").alias("event_id"),
            F.col("data.customer_id").alias("customer_id"),
            F.col("data.product_id").alias("product_id"),
            F.col("data.event_type").alias("event_type"),
            F.col("data.session_id").alias("session_id"),
            F.col("data.page").alias("page"),
            F.to_timestamp(F.col("data.event_ts")).alias("event_ts"),
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.current_timestamp().alias("ingested_at"),
        )
    )

    dead_df = (
        batch_df
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

    valid_count = valid_df.count()
    dead_count  = dead_df.count()

    if valid_count > 0:
        valid_df.write.mode("append").saveAsTable(TARGET_TABLE)

    if dead_count > 0:
        dead_df.write.mode("append").saveAsTable(DEAD_LETTER_TABLE)

    print(f"  Batch {batch_id}: valid={valid_count}, dead_letter={dead_count}")


query = (
    parsed
    .writeStream
    .foreachBatch(route_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for completion and report

# COMMAND ----------

query.awaitTermination()

progress = query.lastProgress
print("\n=== Run complete ===")
print(f"numInputRows : {progress.get('numInputRows') if progress else 'n/a'}")
print(f"\nVerify with:")
print(f"  SELECT COUNT(*) FROM {TARGET_TABLE}")
print(f"  SELECT COUNT(*) FROM {DEAD_LETTER_TABLE}")
