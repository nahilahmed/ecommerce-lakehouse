# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Products — ShopMetrics Inc.
# MAGIC **FR-001** — Synthetic product data for batch ingestion.
# MAGIC
# MAGIC | Mode | Behaviour |
# MAGIC |------|-----------|
# MAGIC | `historical` | Generates 1K products across 8 categories (full initial load) |
# MAGIC | `incremental` | Generates new products + price/category changes on existing products |
# MAGIC
# MAGIC Incremental delta files contain both new and changed records — the silver layer
# MAGIC detects what changed for dimension updates.
# MAGIC
# MAGIC **Output:** `/Volumes/ecommerce/bronze/raw_data/products/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("mode", "historical", "Mode (historical / incremental)")
dbutils.widgets.text("num_new", "50", "New products to add (incremental only)")
dbutils.widgets.text("num_updates", "100", "Existing products to modify (incremental only)")

mode = dbutils.widgets.get("mode").strip().lower()
num_new = int(dbutils.widgets.get("num_new"))
num_updates = int(dbutils.widgets.get("num_updates"))

HISTORICAL_COUNT = 1_000
VOLUME_PATH = "/Volumes/shopmetrics_ecommerce/bronze/raw_data/products"
FIELDNAMES = ["product_id", "product_name", "category", "price"]

print(f"Mode: {mode}")
if mode == "incremental":
    print(f"  New products: {num_new:,} | Updates to existing: {num_updates:,}")
else:
    print(f"  Records: {HISTORICAL_COUNT:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import csv
import random
from datetime import date
from pathlib import Path

# 8 categories per BRD with realistic product name prefixes
CATEGORIES = {
    "Electronics": [
        "Wireless Headphones", "Bluetooth Speaker", "USB-C Hub", "Portable Charger",
        "Smart Watch", "Webcam HD", "Mechanical Keyboard", "Gaming Mouse",
        "Tablet Stand", "LED Desk Lamp", "Noise Canceller", "Streaming Mic",
    ],
    "Clothing": [
        "Cotton T-Shirt", "Slim Fit Jeans", "Hoodie Pullover", "Denim Jacket",
        "Cargo Shorts", "Polo Shirt", "Wool Sweater", "Linen Pants",
        "Rain Jacket", "Athletic Joggers", "Flannel Shirt", "Chino Trousers",
    ],
    "Home & Kitchen": [
        "Air Fryer", "Blender Pro", "Coffee Maker", "Toaster Oven",
        "Cast Iron Skillet", "Knife Set", "Cutting Board", "Spice Rack",
        "Rice Cooker", "Food Processor", "Tea Kettle", "Mixing Bowl Set",
    ],
    "Books": [
        "Python Handbook", "Data Engineering Guide", "SQL Deep Dive", "Cloud Architecture",
        "ML Fundamentals", "Spark Definitive Guide", "Kafka In Action", "System Design",
        "Clean Code", "Refactoring Patterns", "DevOps Handbook", "Site Reliability",
    ],
    "Sports & Outdoors": [
        "Yoga Mat", "Resistance Bands", "Dumbbell Set", "Jump Rope",
        "Hiking Backpack", "Water Bottle", "Running Shoes", "Cycling Gloves",
        "Foam Roller", "Pull-Up Bar", "Tennis Racket", "Camping Tent",
    ],
    "Beauty & Health": [
        "Sunscreen SPF50", "Moisturizer Cream", "Vitamin C Serum", "Hair Oil",
        "Face Wash Gel", "Lip Balm Set", "Body Lotion", "Hand Cream",
        "Eye Cream", "Shampoo Organic", "Conditioner Pro", "Face Mask Pack",
    ],
    "Toys & Games": [
        "Building Blocks", "Board Game Classic", "Puzzle 1000pc", "RC Car",
        "Card Game Pack", "Action Figure", "Art Supply Kit", "Science Kit",
        "Chess Set", "Drone Mini", "Plush Toy", "Trivia Game",
    ],
    "Grocery": [
        "Organic Honey", "Mixed Nuts", "Olive Oil Extra", "Protein Bars",
        "Green Tea Pack", "Dark Chocolate", "Almond Butter", "Granola Mix",
        "Quinoa Bag", "Coconut Water", "Dried Fruit Mix", "Energy Bites",
    ],
}

# Price ranges per category (min, max) in USD
PRICE_RANGES = {
    "Electronics": (15.99, 299.99),
    "Clothing": (12.99, 149.99),
    "Home & Kitchen": (9.99, 199.99),
    "Books": (9.99, 59.99),
    "Sports & Outdoors": (7.99, 179.99),
    "Beauty & Health": (5.99, 89.99),
    "Toys & Games": (8.99, 129.99),
    "Grocery": (3.99, 49.99),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def generate_product(seq: int) -> dict:
    """Generate a single product with a realistic name and price for its category."""
    category = random.choice(list(CATEGORIES.keys()))
    base_name = random.choice(CATEGORIES[category])
    variant = random.choice(["", " V2", " Pro", " Lite", " XL", " Mini", " Plus", " Max"])
    product_name = f"{base_name}{variant}"

    price_min, price_max = PRICE_RANGES[category]
    price = round(random.uniform(price_min, price_max), 2)

    return {
        "product_id": f"PROD-{seq:06d}",
        "product_name": product_name,
        "category": category,
        "price": f"{price:.2f}",
    }


def load_existing_products(volume_path: str) -> list[dict]:
    """Read all existing product records from CSVs in the volume."""
    all_products = []
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
            all_products.extend(list(reader))
    return all_products


def get_max_seq(products: list[dict]) -> int:
    """Find the highest product sequence number from a list of records."""
    max_seq = 0
    for row in products:
        pid = row.get("product_id", "")
        if pid.startswith("PROD-"):
            max_seq = max(max_seq, int(pid.split("-")[1]))
    return max_seq


def mutate_product(product: dict) -> dict:
    """
    Simulate a source system update — price change, category reassignment, or both.
    Returns the full record with current state (as a source extract would).
    """
    record = dict(product)
    change_type = random.choice(["price", "price", "price", "category"])  # Price changes are more common

    if change_type == "price":
        # Adjust price by -20% to +30% (sales, inflation, repricing)
        old_price = float(record["price"])
        multiplier = random.uniform(0.80, 1.30)
        record["price"] = f"{round(old_price * multiplier, 2):.2f}"

    elif change_type == "category":
        old_cat = record["category"]
        new_cat = random.choice([c for c in CATEGORIES.keys() if c != old_cat])
        record["category"] = new_cat
        # Also adjust price to fit the new category range
        price_min, price_max = PRICE_RANGES[new_cat]
        record["price"] = f"{round(random.uniform(price_min, price_max), 2):.2f}"

    return record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Data

# COMMAND ----------

rows = []

if mode == "historical":
    for i in range(1, HISTORICAL_COUNT + 1):
        rows.append(generate_product(i))
    filename = "products_historical.csv"
    print(f"Generated {len(rows):,} products (PROD-000001 to PROD-{HISTORICAL_COUNT:06d})")

    # Summary by category
    from collections import Counter
    cat_counts = Counter(r["category"] for r in rows)
    print("\nCategory distribution:")
    for cat, count in sorted(cat_counts.items()):
        print(f"  {cat}: {count}")

elif mode == "incremental":
    existing = load_existing_products(VOLUME_PATH)
    max_seq = get_max_seq(existing)
    print(f"Found {len(existing):,} existing products (max ID: PROD-{max_seq:06d})")

    if max_seq == 0:
        raise ValueError("No existing products found. Run historical mode first.")

    # 1) New products
    for i in range(max_seq + 1, max_seq + 1 + num_new):
        rows.append(generate_product(i))
    print(f"  New products: {num_new:,} (PROD-{max_seq + 1:06d} to PROD-{max_seq + num_new:06d})")

    # 2) Price/category changes on existing products
    sample_size = min(num_updates, len(existing))
    products_to_update = random.sample(existing, sample_size)
    for prod in products_to_update:
        rows.append(mutate_product(prod))
    print(f"  Attribute changes: {sample_size:,} existing products modified")

    filename = f"products_incremental_{date.today().isoformat()}.csv"

else:
    raise ValueError(f"Invalid mode: '{mode}'. Use 'historical' or 'incremental'.")

print(f"\nTotal records in delta file: {len(rows):,}")

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
    changed_ids = [r["product_id"] for r in rows[num_new:]]
    if changed_ids:
        print(f"\nSample changed product IDs: {changed_ids[:5]}")
