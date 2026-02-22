# Kafka Concepts — ShopMetrics Reference

Practical notes on Kafka concepts as they apply to this project.
Written alongside Day 10–12 implementation.

---

## What Kafka Does

Kafka is a **persistent message highway** between systems. It decouples the
producer (your website/script generating events) from the consumer (Databricks
processing them).

Without Kafka:
```
Website → Database (direct write — slow, loses events if DB is down)
```

With Kafka:
```
Website → Kafka → Databricks
           (stores)  (reads at its own pace)
```

The producer fires events and moves on. Databricks reads them when it's ready.
If Databricks is slow or down, events queue up in Kafka and nothing is lost
(within the retention window — 24 hours for this project).

---

## Topics

A **topic** is a named stream of events — like a table in a database, but for
messages. This project uses one topic:

```
clickstream-events
```

Producers write to a topic. Consumers read from it. Multiple consumers can
read the same topic independently without affecting each other.

---

## Partitions

A topic is split into **partitions** — parallel lanes that allow multiple
readers to work simultaneously.

```
clickstream-events (3 partitions)
├── Partition 0: [event_A, event_D, event_G, ...]
├── Partition 1: [event_B, event_E, event_H, ...]
└── Partition 2: [event_C, event_F, event_I, ...]
```

Key properties:
- Events **within** a partition are strictly ordered (append-only log)
- Events **across** partitions have no ordering guarantee
- Each partition can be read by one Spark task simultaneously → parallelism

This project uses **3 partitions** matching the BRD spec. At ~1 event/sec the
throughput doesn't need it, but it's the right pattern for production readiness.

---

## Offsets

An **offset** is Kafka's internal sequence number for each message within a
partition. Think of it as a line number in the partition's log.

```
Partition 0:
  offset 0  → {"event_id": "abc", "event_type": "page_view", ...}
  offset 1  → {"event_id": "def", "event_type": "add_to_cart", ...}
  offset 2  → {"event_id": "ghi", "event_type": "purchase", ...}
  ...
  offset 1042 → (latest)
```

Offsets are:
- **Per-partition** — partition 0 and partition 1 each have their own offset 0
- **Immutable** — once written, an offset never changes
- **The consumer's bookmark** — Spark tracks which offset it has read up to per
  partition, so it knows where to resume

In the `clickstream_raw` Bronze table, `kafka_offset` is stored as an audit
column so you can trace any row back to its exact position in Kafka.

---

## Message Keys

Every Kafka message has an optional **key** alongside its value. The key
controls which partition the message lands in — Kafka hashes the key and maps
it to a partition deterministically.

In this project the producer uses `customer_id` as the key:

```python
key = str(event["customer_id"]).encode("utf-8")
```

**Why this matters:** all events for customer `#4201` always route to the same
partition, in arrival order. When the Silver sessionization layer groups events
into sessions, it can rely on events for the same customer being ordered — no
events arriving out of sequence on the same partition.

If no key is set, Kafka round-robins across partitions and ordering across a
customer's events is not guaranteed.

---

## Checkpoints

A **checkpoint** is a directory that Spark Structured Streaming writes to
track its progress. It is what makes streaming restartable and exactly-once.

### What's inside

```
/Volumes/ecommerce/bronze/raw_data/checkpoints/clickstream_bronze/
├── offsets/        ← "what I plan to read this batch"
│   ├── 0
│   ├── 1
│   └── 2
├── commits/        ← "what I successfully wrote"
│   ├── 0
│   └── 1
└── metadata        ← query configuration
```

An `offsets/` file looks like:
```json
{
  "clickstream-events": {
    "0": 1042,
    "1": 987,
    "2": 1105
  }
}
```

One offset per partition — exactly where Spark will start next run.

### The two-phase commit

Spark uses a two-phase design to survive crashes:

```
Batch N:
  Step 1 → Write offsets/N   ("planning to read up to these offsets")
  Step 2 → Read from Kafka
  Step 3 → Transform data
  Step 4 → Write to Delta    ("done — write commits/N")
```

If a crash happens between Step 1 and Step 4:
- `offsets/N` exists, `commits/N` does NOT
- On recovery, Spark replays the exact same batch from the same offsets
- Delta's atomic writes handle deduplication → no double rows

If both exist, the batch succeeded and Spark advances to the next offsets.

### Exactly-once semantics

The combination of checkpoint-tracked offsets + Delta Lake's transactional
writes gives **exactly-once** delivery — even if the cluster dies mid-run,
every event ends up in the Delta table exactly once.

### Resetting the checkpoint

To reprocess everything from scratch (e.g. after a schema change):
1. Delete the checkpoint directory
2. Set `startingOffsets: earliest` in the stream config
3. Spark treats it as a fresh start and replays all Kafka history

---

## How Spark Reads Kafka

When Spark connects to Kafka, the raw DataFrame schema is always:

| Column      | Type      | Description                              |
|-------------|-----------|------------------------------------------|
| `key`       | binary    | Message key (customer_id in this project)|
| `value`     | binary    | Message payload (your JSON)              |
| `topic`     | string    | Topic name                               |
| `partition` | int       | Which partition                          |
| `offset`    | long      | Offset within that partition             |
| `timestamp` | timestamp | When Kafka received the message          |

`value` is raw bytes. The first transformation in every streaming notebook is:

```python
# Cast bytes → string → parse JSON into columns
from pyspark.sql import functions as F

parsed = (
    raw_stream
    .withColumn("value", F.col("value").cast("string"))
    .withColumn("data", F.from_json(F.col("value"), EVENT_SCHEMA))
    .select("data.*", "offset", "timestamp")
)
```

---

## Timestamps in the Pipeline

Three different timestamps flow through the clickstream pipeline:

| Column        | Set by       | Meaning                                      |
|---------------|--------------|----------------------------------------------|
| `event_ts`    | Producer     | When the event happened on the user's browser |
| `timestamp`   | Kafka broker | When Kafka received the message               |
| `ingested_at` | Databricks   | When Spark wrote the row to Delta            |

Bronze preserves all three. Silver uses `event_ts` for sessionization logic
(the 30-minute inactivity window).

---

## Free Edition Constraints

Databricks Free Edition imposes two streaming constraints relevant to this
project:

| Constraint | Behaviour | Workaround |
|------------|-----------|------------|
| No infinite triggers | `ProcessingTime` trigger not supported | Use `trigger(availableNow=True)` |
| No implicit checkpoints | Temp checkpoint paths not allowed | Always specify `checkpointLocation` explicitly |

`availableNow=True` makes streaming behave like a scheduled batch — it reads
all unprocessed events, writes them, then stops. Latency (NFR-002) is
therefore determined by how frequently the Databricks Job is scheduled.
