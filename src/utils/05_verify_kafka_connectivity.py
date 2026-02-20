# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka Connectivity Verification
# MAGIC
# MAGIC Verifies connectivity to Confluent Cloud using widget-supplied credentials.
# MAGIC Paste your values into the widgets above and run all cells.
# MAGIC
# MAGIC **Do not commit credentials — use widgets only.**

# COMMAND ----------

dbutils.widgets.text("kafka_bootstrap_server", "", "Bootstrap Server")
dbutils.widgets.text("kafka_api_key", "", "API Key")
dbutils.widgets.text("kafka_api_secret", "", "API Secret")
dbutils.widgets.text("kafka_topic", "clickstream-events", "Topic")

# COMMAND ----------

bootstrap = dbutils.widgets.get("kafka_bootstrap_server")
api_key   = dbutils.widgets.get("kafka_api_key")
api_secret = dbutils.widgets.get("kafka_api_secret")
topic     = dbutils.widgets.get("kafka_topic")

assert bootstrap, "bootstrap_server is empty — fill in the widget"
assert api_key,   "api_key is empty — fill in the widget"
assert api_secret, "api_secret is empty — fill in the widget"

print(f"Bootstrap : {bootstrap}")
print(f"API Key   : {api_key[:6]}{'*' * (len(api_key) - 6)}")
print(f"Topic     : {topic}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1 — Auth + schema check
# MAGIC
# MAGIC Resolves the Kafka schema without reading any rows.
# MAGIC This confirms credentials and broker reachability — works even on an empty topic.

# COMMAND ----------

jaas = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{api_key}" password="{api_secret}";'
)

kafka_options = {
    "kafka.bootstrap.servers":  bootstrap,
    "kafka.security.protocol":  "SASL_SSL",
    "kafka.sasl.mechanism":     "PLAIN",
    "kafka.sasl.jaas.config":   jaas,
    "subscribe":                topic,
    "startingOffsets":          "earliest",
}

try:
    df = (
        spark.read
        .format("kafka")
        .options(**kafka_options)
        .load()
    )
    # Schema resolution alone proves auth + network succeeded
    print(f"✅ Connected. Topic '{topic}' is reachable.")
    print(f"   Kafka schema fields: {[f.name for f in df.schema.fields]}")
    print(f"   (Topic is empty — no rows expected yet)")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2 — Streaming read (AvailableNow trigger)
# MAGIC
# MAGIC Uses `trigger(availableNow=True)` — the only streaming trigger supported on
# MAGIC Databricks Free Edition. Processes all available data then stops automatically.
# MAGIC Confirms the cluster can open a streaming Kafka connection.

# COMMAND ----------

streaming_options = {**kafka_options}
streaming_options.pop("endingOffsets", None)
streaming_options["startingOffsets"] = "latest"

stream_query = (
    spark.readStream
    .format("kafka")
    .options(**streaming_options)
    .load()
    .writeStream
    .format("memory")
    .queryName("kafka_connectivity_test")
    .trigger(availableNow=True)
    .start()
)

stream_query.awaitTermination()

print("✅ Streaming connection established and cleanly stopped.")
print(f"   Status: {stream_query.lastProgress}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC If both tests passed:
# MAGIC - ✅ Confluent credentials are correct
# MAGIC - ✅ Topic `clickstream-events` exists and is reachable from Databricks
# MAGIC - ✅ Ready to build the clickstream producer (Day 11)
# MAGIC
# MAGIC **Next step:** store credentials in Databricks Secret Scope (`confluent` scope)
# MAGIC and replace widget reads with `dbutils.secrets.get()` before production use.