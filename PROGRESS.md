# ShopMetrics — Progress Tracker
**Project Code:** SHOPMETRICS-DATA-001

---

## Recent Updates

### 2026-02-22 — Confluent Kafka Connectivity Verified
- ✅ Signed up for Confluent Cloud, created free Basic cluster
- ✅ Created topic `clickstream-events` (3 partitions, 24-hr retention)
- ✅ Created `src/utils/05_verify_kafka_connectivity.py` — widget-based connectivity notebook
- ✅ Debugged Free Edition constraints: `startingOffsets=latest` on empty topic, infinite trigger not supported, implicit checkpoint not supported
- ✅ Both connectivity tests pass (batch schema check + AvailableNow streaming)
- ⏳ Store credentials in Databricks Secret Scope (CLI setup pending)
- 📋 **Next:** Day 11 — Clickstream producer (`data-generator/produce_clickstream.py`)

### 2026-02-20 — Gold Layer Complete + Daily Sales Dashboard
- ✅ Created Gold master orchestration notebook (`src/gold/ingest_gold_tables.py`) with watermark-based metadata-driven execution
- ✅ Created `src/gold/table_notebooks/daily_sales_summary.py` (FR-006) — date/category revenue aggregations
- ✅ Created `src/gold/table_notebooks/customer_ltv.py` (FR-007) — LTV segmentation (High/Medium/Low)
- ✅ Created `src/gold/table_notebooks/product_performance.py` (FR-008) — units sold, revenue, category rank
- ✅ Fixed bugs in customer_ltv and product_performance notebooks
- ✅ Created Databricks Lakeview dashboard: `dashboards/Shopcommerce - Daily Sales Visualization.lvdash.json`
- ✅ Updated `src/utils/04_gold_metadata_setup.py` with all three Gold table registrations + dashboard config

### 2026-02-19 — Silver Layer Review & Optimization
- ✅ Reviewed unified silver ingestion script (`src/silver/ingest_silver_tables.py`)
- ✅ Fixed critical bug: Main execution loop now respects `processing_order` from metadata
- ✅ Fixed variable scope bug in `process_fact()` function
- ✅ Enhanced dependency validation with detailed status feedback
- ✅ Verified SCD Type 1, SCD Type 2, and Fact processing implementations

---

## Week 1 — Foundation & Batch Data Setup

### Day 1 — Databricks Workspace Setup
- [x] Sign up for Databricks Free Edition
- [x] Create Unity Catalog: catalog=shopmetrics_ecommerce, schemas=bronze/silver/gold
- [x] Create volume: ecommerce.bronze.raw_data
- [x] Verify Unity Catalog lineage tracking enabled

### Day 2 — GitHub Repo Setup
- [x] Create GitHub repo with README, .gitignore
- [x] Create folder structure: src/{bronze,silver,gold,utils}/, tests/, data-generator/, docs/
- [x] Add BRD and implementation plan to docs/
- [x] Add CLAUDE.md for AI-assisted development context

### Day 3 — Data Generators (Customers + Orders)
- [x] `data-generator/generate_customers.py` — 10K customers, historical + incremental with attribute changes
- [x] `data-generator/generate_orders.py` — 100K orders, historical + incremental with status transitions
- [x] Run generate_customers.py (historical) in Databricks
- [x] Run generate_orders.py (historical) in Databricks

### Day 4 — Data Generator (Products) + Bronze Ingestion
- [x] `data-generator/generate_products.py` — 1K products across 8 categories, historical + incremental with price changes
- [x] Run generate_products.py (historical) in Databricks
- [x] Upload/verify all CSVs in ecommerce.bronze.raw_data volume
- [x] Create src/bronze/ingest_orders.py — add ingested_at, source_file audit columns
- [x] Write to ecommerce.bronze.orders_raw — verify schema matches BRD §7.2
- [x] Test with SELECT, COUNT, DESCRIBE

### Day 5 — Bronze Complete + First Silver
- [x] Ingest ecommerce.bronze.customers_raw and products_raw
- [x] Create metadata-driven framework (src/utils/03_silver_metadata_setup.py)
- [x] Create unified silver processing script (src/silver/ingest_silver_tables.py)
- [x] Implement orders_clean processing (fact table with dimension joins)
- [ ] Draft docs/data-model.md

### Day 6 — SCD Type 2
- [x] Create SCD Type 2 implementation in src/silver/ingest_silver_tables.py (dim_customers)
- [x] Create SCD Type 1 implementation in src/silver/ingest_silver_tables.py (dim_products)
- [x] Implement dependency-ordered execution (processing_order column)
- [x] Add dynamic dimension joins for fact tables (SCD2 point-in-time support)
- [x] Review and fix execution ordering bugs
- [ ] Write tests/unit/test_scd2_customers.py
- [ ] Test AC-003: update customer email, verify old record end_date populated

---

## Week 2 — Gold Layer + Confluent Kafka Setup

### Day 7 — Daily Sales Summary
- [x] Create `src/gold/table_notebooks/daily_sales_summary.py` (FR-006)
- [ ] Verify AC-005: revenue within 0.01% of manual calc

### Day 8 — Customer LTV
- [x] Create `src/gold/table_notebooks/customer_ltv.py` (FR-007)
- [ ] Verify AC-006: 3 test profiles segmented correctly

### Day 9 — Product Performance
- [x] Create `src/gold/table_notebooks/product_performance.py` (FR-008)
- [ ] Full batch pipeline end-to-end run
- [ ] Verify NFR-001: pipeline completes within 60 minutes

### Day 10 — Confluent Kafka Cluster
- [x] Sign up for Confluent Cloud, create free Basic cluster
- [x] Create topic: clickstream-events (3 partitions, 24-hr retention)
- [x] Verify connectivity from Databricks (`src/utils/05_verify_kafka_connectivity.py`)
- [ ] Store API key + secret in Databricks Secret Scope (via CLI)

### Day 11 — Clickstream Producer
- [x] Create `data-generator/produce_clickstream.py` — session-aware, ~1 event/sec
- [x] Test locally, verified messages flowing in Confluent Cloud UI

### Day 12 — Streaming Bronze
- [ ] Store API key + secret in Databricks Secret Scope (via CLI)
- [x] Create `src/bronze/stream_clickstream.py` — Structured Streaming from Kafka
- [ ] Run notebook, verify rows in `ecommerce.bronze.clickstream_raw`
- [ ] Verify sub-5-minute latency (NFR-002)

### Day 13 — Streaming Silver + Gold
- [ ] Create src/silver/sessionize_clickstream.py (30-min window)
- [ ] Create src/gold/hourly_traffic_metrics.py (FR-009)
- [ ] Full streaming E2E: producer → Kafka → bronze → silver → gold

---

## Week 3 — CI/CD, Testing & Asset Bundles

### Day 14 — Secrets & PAT
- [ ] Generate Databricks PAT, add GitHub Secrets

### Day 15 — Asset Bundles
- [ ] Create databricks.yml config (dev + prod targets)
- [ ] Validate locally: databricks bundle validate

### Day 16 — Test Suite
- [ ] tests/unit/test_scd2_customers.py (AC-003)
- [ ] tests/unit/test_gold_sales.py (AC-005)
- [ ] tests/unit/test_data_quality.py (AC-004)
- [ ] tests/unit/test_kafka_producer.py

### Day 17 — CI Pipeline
- [ ] .github/workflows/ci.yml (black, flake8, mypy, pytest)
- [ ] .pre-commit-config.yaml

### Day 18 — CD Pipeline
- [ ] .github/workflows/cd.yml (deploy on merge to main)

### Day 19 — Scheduling + DQ
- [ ] .github/workflows/schedule.yml (cron 02:00 UTC)
- [ ] src/utils/data_quality.py (FR-005)
- [ ] docs/deployment-guide.md

### Day 20 — Optimisation
- [ ] OPTIMIZE + ZORDER on gold tables (FR-011)
- [ ] VACUUM with 7-day retention
- [ ] Unity Catalog table tags (FR-012)

---

## Week 4 — Dashboard & Public Launch

### Day 21–27 — Streamlit Dashboard
- [ ] dashboard/streamlit_app.py scaffold
- [ ] dashboard/utils/databricks_connector.py (with Parquet fallback)
- [ ] dashboard/pages/1_Sales_Performance.py (FR-013)
- [ ] dashboard/pages/2_Customer_Analytics.py (FR-014)
- [ ] dashboard/pages/3_Product_Insights.py (FR-015)
- [ ] dashboard/pages/4_Kafka_Traffic.py (FR-016)
- [ ] Deploy to Streamlit Community Cloud
- [ ] Verify AC-009, AC-010, AC-011

---

## Week 5 — Documentation & Launch

### Day 28–30
- [ ] docs/deployment-guide.md (AC-013)
- [ ] README with architecture diagram + live dashboard link
- [ ] Verify AC-012: no credentials in repo

---

## Milestone Tracker

| Milestone | Target Day | Status |
|-----------|-----------|--------|
| Bronze layer complete | 5 | ✅ Complete |
| Batch medallion done | 9 | ✅ Complete (Gold notebooks built; AC verifications pending) |
| Kafka streaming live | 13 | Not started |
| CI/CD operational | 18 | Not started |
| All 4 dashboard pages live | 26 | 🟡 In Progress (Lakeview daily sales dashboard live) |
| All ACs signed off | 27 | Not started |
| Project publicly launched | 30 | Not started |
