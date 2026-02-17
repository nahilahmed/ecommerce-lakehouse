# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Orders — ShopMetrics Inc.
# MAGIC **FR-001** — Synthetic order data for batch ingestion.
# MAGIC
# MAGIC | Mode | Behaviour |
# MAGIC |------|-----------|
# MAGIC | `historical` | Generates 100K orders (2022–2026) referencing existing customers and products |
# MAGIC | `incremental` | Generates `num_new` new orders + status transitions on existing pending orders |
# MAGIC
# MAGIC Status transitions in incremental mode simulate real order lifecycle:
# MAGIC `pending → completed`, `pending → cancelled`, `completed → refunded`
# MAGIC
# MAGIC **Depends on:** `customers_historical.csv` and `products_historical.csv` must exist in their
# MAGIC respective volume paths so orders reference valid foreign keys.
# MAGIC
# MAGIC **Output:** `/Volumes/ecommerce/bronze/raw_data/orders/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("mode", "historical", "Mode (historical / incremental)")
dbutils.widgets.text("num_new", "5000", "New orders to add (incremental only)")
dbutils.widgets.text("num_status_changes", "2000", "Status transitions on existing orders (incremental only)")

mode = dbutils.widgets.get("mode").strip().lower()
num_new = int(dbutils.widgets.get("num_new"))
num_status_changes = int(dbutils.widgets.get("num_status_changes"))

HISTORICAL_COUNT = 100_000
ORDERS_VOLUME = "/Volumes/shopmetrics_ecommerce/bronze/raw_data/orders"
CUSTOMERS_VOLUME = "/Volumes/shopmetrics_ecommerce/bronze/raw_data/customers"
PRODUCTS_VOLUME = "/Volumes/shopmetrics_ecommerce/bronze/raw_data/products"
FIELDNAMES = ["order_id", "customer_id", "product_id", "order_date", "total_amount", "status"]

# BRD-defined statuses
VALID_STATUSES = ["pending", "completed", "cancelled", "refunded"]

print(f"Mode: {mode}")
if mode == "incremental":
    print(f"  New orders: {num_new:,} | Status transitions: {num_status_changes:,}")
else:
    print(f"  Records: {HISTORICAL_COUNT:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import csv
import random
from datetime import date, timedelta
from collections import Counter

# Historical order date range: 2022-01-01 to 2026-01-31
HIST_START = date(2022, 1, 1)
HIST_END = date(2026, 1, 31)
HIST_RANGE_DAYS = (HIST_END - HIST_START).days

# Status distribution for historical load (realistic e-commerce)
HISTORICAL_STATUS_WEIGHTS = {
    "completed": 0.65,
    "pending": 0.15,
    "cancelled": 0.12,
    "refunded": 0.08,
}

# Realistic status transitions for incremental mode
# Maps: current_status -> [(new_status, probability), ...]
STATUS_TRANSITIONS = {
    "pending": [("completed", 0.70), ("cancelled", 0.30)],
    "completed": [("refunded", 1.0)],
    # cancelled and refunded are terminal states — no transitions
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def load_ids_from_volume(volume_path: str, id_column: str) -> list[str]:
    """Extract a list of IDs from all CSVs in a volume path."""
    ids = []
    try:
        files = dbutils.fs.ls(volume_path)
    except Exception:
        return []

    for f in files:
        if not f.name.endswith(".csv"):
            continue
        local_path = f"{volume_path}/{f.name}"
        with open(local_path, "r") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                val = row.get(id_column, "")
                if val:
                    ids.append(val)
    return list(set(ids))  # deduplicate


def load_existing_orders(volume_path: str) -> list[dict]:
    """Read all existing order records from CSVs in the volume."""
    all_orders = []
    try:
        files = dbutils.fs.ls(volume_path)
    except Exception:
        return []

    for f in files:
        if not f.name.endswith(".csv"):
            continue
        local_path = f"{volume_path}/{f.name}"
        with open(local_path, "r") as fh:
            reader = csv.DictReader(fh)
            all_orders.extend(list(reader))
    return all_orders


def get_max_seq(orders: list[dict]) -> int:
    """Find the highest order sequence number."""
    max_seq = 0
    for row in orders:
        oid = row.get("order_id", "")
        if oid.startswith("ORD-"):
            max_seq = max(max_seq, int(oid.split("-")[1]))
    return max_seq


def weighted_status() -> str:
    """Pick a random status based on historical distribution weights."""
    r = random.random()
    cumulative = 0.0
    for status, weight in HISTORICAL_STATUS_WEIGHTS.items():
        cumulative += weight
        if r <= cumulative:
            return status
    return "completed"


def generate_order(seq: int, customer_ids: list[str], product_ids: list[str], date_start: date, date_end: date) -> dict:
    """Generate a single order referencing valid customer and product IDs."""
    days_range = (date_end - date_start).days
    order_date = date_start + timedelta(days=random.randint(0, days_range))

    # Total amount: realistic range $5 to $2000, weighted toward lower values
    amount = round(random.lognormvariate(3.5, 1.0), 2)
    amount = max(5.00, min(amount, 2000.00))

    return {
        "order_id": f"ORD-{seq:07d}",
        "customer_id": random.choice(customer_ids),
        "product_id": random.choice(product_ids),
        "order_date": order_date.isoformat(),
        "total_amount": f"{amount:.2f}",
        "status": weighted_status(),
    }


def transition_status(order: dict) -> dict | None:
    """
    Apply a realistic status transition to an order.
    Returns the updated record (as a source extract would), or None if no transition is possible.
    """
    current = order["status"]
    transitions = STATUS_TRANSITIONS.get(current)
    if not transitions:
        return None  # Terminal state

    r = random.random()
    cumulative = 0.0
    new_status = transitions[-1][0]  # default to last option
    for status, prob in transitions:
        cumulative += prob
        if r <= cumulative:
            new_status = status
            break

    record = dict(order)
    record["status"] = new_status
    return record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Foreign Keys

# COMMAND ----------

customer_ids = load_ids_from_volume(CUSTOMERS_VOLUME, "customer_id")
product_ids = load_ids_from_volume(PRODUCTS_VOLUME, "product_id")

if not customer_ids:
    raise ValueError(f"No customers found in {CUSTOMERS_VOLUME}. Run generate_customers.py first.")
if not product_ids:
    raise ValueError(f"No products found in {PRODUCTS_VOLUME}. Run generate_products.py first.")

print(f"Loaded {len(customer_ids):,} customer IDs and {len(product_ids):,} product IDs as foreign keys")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Data

# COMMAND ----------

import datetime

rows = []
def get_timestamp():
    return datetime.datetime.now().isoformat(timespec='seconds').replace(':', '-')

if mode == "historical":
    for i in range(1, HISTORICAL_COUNT + 1):
        rows.append(generate_order(i, customer_ids, product_ids, HIST_START, HIST_END))
    filename = "orders_historical.csv"
    print(f"Generated {len(rows):,} orders (ORD-0000001 to ORD-{HISTORICAL_COUNT:07d})")

    # Status distribution summary
    status_counts = Counter(r["status"] for r in rows)
    print("\nStatus distribution:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count:,} ({count / len(rows) * 100:.1f}%)")

elif mode == "incremental":
    existing = load_existing_orders(ORDERS_VOLUME)
    max_seq = get_max_seq(existing)
    print(f"Found {len(existing):,} existing orders (max ID: ORD-{max_seq:07d})")

    if max_seq == 0:
        raise ValueError("No existing orders found. Run historical mode first.")

    # 1) New orders (recent dates — last 7 days)
    new_start = date.today() - timedelta(days=7)
    new_end = date.today()
    for i in range(max_seq + 1, max_seq + 1 + num_new):
        order = generate_order(i, customer_ids, product_ids, new_start, new_end)
        # New orders are more likely pending or completed
        order["status"] = random.choices(["pending", "completed"], weights=[0.6, 0.4])[0]
        rows.append(order)
    print(f"  New orders: {num_new:,} (ORD-{max_seq + 1:07d} to ORD-{max_seq + num_new:07d})")

    # 2) Status transitions on existing orders
    # Filter to orders that CAN transition (pending, completed)
    transitionable = [o for o in existing if o["status"] in STATUS_TRANSITIONS]
    sample_size = min(num_status_changes, len(transitionable))

    if sample_size > 0:
        orders_to_transition = random.sample(transitionable, sample_size)
        transition_count = 0
        for order in orders_to_transition:
            updated = transition_status(order)
            if updated:
                rows.append(updated)
                transition_count += 1
        print(f"  Status transitions: {transition_count:,} orders updated")

        # Transition summary
        trans_statuses = Counter(r["status"] for r in rows[num_new:])
        print("    Transition breakdown:")
        for status, count in sorted(trans_statuses.items()):
            print(f"      → {status}: {count:,}")
    else:
        print("  No transitionable orders found (all in terminal state)")

    timestamp = get_timestamp()
    filename = f"orders_incremental_{timestamp}.csv"

else:
    raise ValueError(f"Invalid mode: '{mode}'. Use 'historical' or 'incremental'.")

print(f"\nTotal records in delta file: {len(rows):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Volume

# COMMAND ----------

output_path = f"{ORDERS_VOLUME}/{filename}"

with open(output_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()
    writer.writerows(rows)

print(f"Written → {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Output

# COMMAND ----------

df = spark.read.option("header", True).csv(output_path)
print(f"Row count: {df.count():,}")
df.show(10, truncate=False)

if mode == "incremental":
    print("\nNew vs changed records:")
    print(f"  New orders: {num_new:,}")
    print(f"  Status transitions: {len(rows) - num_new:,}")
