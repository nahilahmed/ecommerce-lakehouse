# Databricks notebook source
# MAGIC %md
# MAGIC # Reset Environment — ShopMetrics Lakehouse
# MAGIC **Purpose:** Clean slate for development/testing — drops all tables, volumes, checkpoints.
# MAGIC
# MAGIC ⚠️ **DESTRUCTIVE OPERATION** — Use only in development environments!
# MAGIC
# MAGIC | What Gets Deleted |
# MAGIC |-------------------|
# MAGIC | All tables in `ecommerce.bronze`, `ecommerce.silver`, `ecommerce.gold` |
# MAGIC | All files in `/Volumes/shopmetrics_ecommerce/bronze/raw_data/*` |
# MAGIC | All streaming checkpoints |
# MAGIC | Metadata tables (if any) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "⚠️ Confirm deletion?")
dbutils.widgets.text("catalog", "shopmetrics_ecommerce", "Catalog name")

confirm = dbutils.widgets.get("confirm").strip().lower()
CATALOG = dbutils.widgets.get("catalog").strip()

if confirm != "yes":
    raise ValueError("❌ Confirmation required. Set 'confirm' widget to 'yes' to proceed.")

print(f"🗑️  Resetting catalog: {CATALOG}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Drop All Tables

# COMMAND ----------

SCHEMAS = ["bronze", "silver", "gold"]

for schema in SCHEMAS:
    schema_fqn = f"{CATALOG}.{schema}"
    print(f"\n📦 Schema: {schema_fqn}")

    # Check if schema exists
    try:
        tables = spark.sql(f"SHOW TABLES IN {schema_fqn}").collect()
    except Exception as e:
        print(f"   ⚠️  Schema does not exist: {e}")
        continue

    if not tables:
        print(f"   ✓ No tables found")
        continue

    # Drop each table
    for row in tables:
        table_name = row.tableName
        table_fqn = f"{schema_fqn}.{table_name}"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
            print(f"   ✓ Dropped: {table_name}")
        except Exception as e:
            print(f"   ✗ Failed to drop {table_name}: {e}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clean Volume Paths

# COMMAND ----------

VOLUME_PATHS = [
    f"/Volumes/{CATALOG}/bronze/raw_data/orders",
    f"/Volumes/{CATALOG}/bronze/raw_data/customers",
    f"/Volumes/{CATALOG}/bronze/raw_data/products",
    f"/Volumes/{CATALOG}/bronze/raw_data/checkpoints",  # All streaming checkpoints
    f"/Volumes/{CATALOG}/bronze/raw_data/schemas",      # All AutoLoader schemas
]

print("\n🗂️  Cleaning volumes (data, checkpoints, schemas)...")

for path in VOLUME_PATHS:
    try:
        files = dbutils.fs.ls(path)
        if files:
            dbutils.fs.rm(path, recurse=True)
            # Recreate the directory
            dbutils.fs.mkdirs(path)
            print(f"   ✓ Cleared: {path} ({len(files)} items removed)")
        else:
            print(f"   ✓ Already empty: {path}")
    except Exception as e:
        print(f"   ⚠️  Path does not exist or error: {path}")
        # Try to create it
        try:
            dbutils.fs.mkdirs(path)
            print(f"   ✓ Created: {path}")
        except Exception as create_err:
            print(f"   ✗ Failed to create: {create_err}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reset Metadata Tables

# COMMAND ----------

print("\n📋 Resetting metadata tables...")

# Bronze ingestion tracking
try:
    spark.sql(f"""
        UPDATE {CATALOG}.metadata.bronze_ingestion_tracking
        SET last_ingested_timestamp = NULL
    """)
    rows_updated = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.metadata.bronze_ingestion_tracking").first()["cnt"]
    print(f"   ✓ Reset: bronze_ingestion_tracking ({rows_updated} tables)")
except Exception as e:
    print(f"   ⚠️  bronze_ingestion_tracking does not exist or error: {e}")

# Silver metadata
try:
    spark.sql(f"""
        UPDATE {CATALOG}.metadata.silver_metadata
        SET bronze_max_watermark = NULL
    """)
    rows_updated = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.metadata.silver_metadata").first()["cnt"]
    print(f"   ✓ Reset: silver_metadata ({rows_updated} configs)")
except Exception as e:
    print(f"   ⚠️  silver_metadata does not exist or error: {e}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("✅ Environment reset complete!")
print("=" * 60)
print("\n📊 What was reset:")
print("   • All Delta tables in bronze/silver/gold schemas")
print("   • All CSV files in volume paths")
print("   • Metadata watermarks (bronze_ingestion_tracking, silver_metadata)")
print("   • AutoLoader schema locations (will re-infer on next run)")
print("   • Streaming checkpoints")
print("\n📝 Next steps:")
print("   1. Run data generators (mode=historical):")
print("      → generate_customers.py")
print("      → generate_products.py")
print("      → generate_orders.py")
print("   2. Run bronze ingestion (AutoLoader will re-infer schemas)")
print("   3. Run silver transformation jobs")
print("   4. Run gold aggregation jobs")
print("\n💡 Tips:")
print("   • AutoLoader schemas cleared → fresh inference with new columns")
print("   • Metadata configs preserved → processing order/rules intact")
