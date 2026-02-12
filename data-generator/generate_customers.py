# Databricks notebook source

# MAGIC %md
# MAGIC # Generate Customers — ShopMetrics Inc.
# MAGIC **FR-001** — Synthetic customer data for batch ingestion.
# MAGIC
# MAGIC | Mode | Behaviour |
# MAGIC |------|-----------|
# MAGIC | `historical` | Generates 10K customers (full initial load) |
# MAGIC | `incremental` | Generates new signups + attribute changes (email/region) on existing customers |
# MAGIC
# MAGIC Incremental files contain both new and changed records — mimicking a real source system
# MAGIC daily delta extract. The silver layer SCD Type 2 (FR-004) detects and handles the changes.
# MAGIC
# MAGIC **Output:** `/Volumes/ecommerce/bronze/raw_data/customers/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("mode", "historical", "Mode (historical / incremental)")
dbutils.widgets.text("num_new", "500", "New customers to add (incremental only)")
dbutils.widgets.text("num_updates", "200", "Existing customers to modify (incremental only)")

mode = dbutils.widgets.get("mode").strip().lower()
num_new = int(dbutils.widgets.get("num_new"))
num_updates = int(dbutils.widgets.get("num_updates"))

HISTORICAL_COUNT = 10_000
VOLUME_PATH = "/Volumes/ecommerce/bronze/raw_data/customers"
FIELDNAMES = ["customer_id", "email", "region", "signup_date"]

print(f"Mode: {mode}")
if mode == "incremental":
    print(f"  New customers: {num_new:,} | Updates to existing: {num_updates:,}")
else:
    print(f"  Records: {HISTORICAL_COUNT:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import csv
import random
import uuid
from datetime import date, timedelta

REGIONS = [
    "North America", "Europe", "Asia Pacific", "Latin America",
    "Middle East", "Africa", "Australia", "South Asia",
]

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Daniel",
    "Lisa", "Matthew", "Nancy", "Anthony", "Betty", "Mark", "Margaret",
    "Donald", "Sandra", "Steven", "Ashley", "Paul", "Dorothy", "Andrew",
    "Kimberly", "Joshua", "Emily", "Kenneth", "Donna", "Kevin", "Michelle",
    "Brian", "Carol", "George", "Amanda", "Timothy", "Melissa", "Ronald",
    "Deborah", "Raj", "Priya", "Amit", "Neha", "Vikram", "Ananya",
    "Carlos", "Maria", "Ahmed", "Fatima", "Wei", "Mei", "Yuki", "Hana",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Patel", "Sharma", "Kumar", "Singh", "Mehta", "Gupta", "Verma",
    "Chen", "Wang", "Li", "Zhang", "Tanaka", "Sato", "Kim", "Park",
]

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
    "icloud.com", "proton.me", "mail.com", "aol.com",
]

SIGNUP_START = date(2020, 1, 1)
SIGNUP_END = date(2026, 1, 31)
SIGNUP_RANGE_DAYS = (SIGNUP_END - SIGNUP_START).days

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def generate_email(first: str, last: str) -> str:
    """Generate a unique email address."""
    uid = uuid.uuid4().hex[:6]
    return f"{first.lower()}.{last.lower()}{uid}@{random.choice(EMAIL_DOMAINS)}"


def generate_customer(seq: int) -> dict:
    """Generate a single new customer record."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return {
        "customer_id": f"CUST-{seq:06d}",
        "email": generate_email(first, last),
        "region": random.choice(REGIONS),
        "signup_date": (SIGNUP_START + timedelta(days=random.randint(0, SIGNUP_RANGE_DAYS))).isoformat(),
    }


def load_existing_customers(volume_path: str) -> list[dict]:
    """Read all existing customer records from CSVs in the volume."""
    all_customers = []
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
            all_customers.extend(list(reader))
    return all_customers


def get_max_seq(customers: list[dict]) -> int:
    """Find the highest customer sequence number from a list of records."""
    max_seq = 0
    for row in customers:
        cid = row.get("customer_id", "")
        if cid.startswith("CUST-"):
            max_seq = max(max_seq, int(cid.split("-")[1]))
    return max_seq


def mutate_customer(customer: dict) -> dict:
    """
    Simulate a real source system update — change email, region, or both.
    Returns the full record with current state (as a source extract would).
    """
    record = dict(customer)
    change_type = random.choice(["email", "region", "both"])

    if change_type in ("email", "both"):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        record["email"] = generate_email(first, last)

    if change_type in ("region", "both"):
        old_region = record["region"]
        new_region = random.choice([r for r in REGIONS if r != old_region])
        record["region"] = new_region

    return record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Data

# COMMAND ----------

rows = []

if mode == "historical":
    for i in range(1, HISTORICAL_COUNT + 1):
        rows.append(generate_customer(i))
    filename = "customers_historical.csv"
    print(f"Generated {len(rows):,} customers (CUST-000001 to CUST-{HISTORICAL_COUNT:06d})")

elif mode == "incremental":
    existing = load_existing_customers(VOLUME_PATH)
    max_seq = get_max_seq(existing)
    print(f"Found {len(existing):,} existing customers (max ID: CUST-{max_seq:06d})")

    if max_seq == 0:
        raise ValueError("No existing customers found. Run historical mode first.")

    # 1) New customer signups
    for i in range(max_seq + 1, max_seq + 1 + num_new):
        rows.append(generate_customer(i))
    print(f"  New signups: {num_new:,} (CUST-{max_seq + 1:06d} to CUST-{max_seq + num_new:06d})")

    # 2) Attribute changes on existing customers (email/region updates)
    sample_size = min(num_updates, len(existing))
    customers_to_update = random.sample(existing, sample_size)
    for cust in customers_to_update:
        rows.append(mutate_customer(cust))
    print(f"  Attribute changes: {sample_size:,} existing customers modified")

    filename = f"customers_incremental_{date.today().isoformat()}.csv"

else:
    raise ValueError(f"Invalid mode: '{mode}'. Use 'historical' or 'incremental'.")

print(f"Total records in delta file: {len(rows):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Volume

# COMMAND ----------

output_path = f"{VOLUME_PATH}/{filename}"

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
    # Show a few of the changed records for inspection
    changed_ids = [r["customer_id"] for r in rows[num_new:]]
    if changed_ids:
        print(f"\nSample changed customer IDs: {changed_ids[:5]}")
