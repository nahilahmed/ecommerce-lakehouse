# ShopMetrics OLTP Source Database Schema

> This is the **existing production PostgreSQL database** that the analytics lakehouse replaces
> for reporting purposes. Currently, Sales/Marketing/Finance teams run ad-hoc queries directly
> against this database, causing query contention during business hours (BRD §2.1).

---

## Entity Relationship Overview

```
┌──────────────┐       ┌──────────────────┐       ┌──────────────┐
│  customers   │       │     orders       │       │   products   │
├──────────────┤       ├──────────────────┤       ├──────────────┤
│ PK customer_id│◄─────┤ FK customer_id   │       │ PK product_id│
│    email     │       │ FK shipping_addr │  ┌───►│    name      │
│    first_name│       │ FK billing_addr  │  │    │ FK category_id│──┐
│    last_name │       │    order_date    │  │    │    price     │  │
│    phone     │       │    status        │  │    │    sku       │  │
│    region    │       │    total_amount  │  │    │    description│  │
│    signup_date│      │    created_at    │  │    │    created_at│  │
│    updated_at│       │    updated_at    │  │    │    updated_at│  │
│    is_active │       └───────┬──────────┘  │    └──────────────┘  │
└──────────────┘               │             │                      │
                               │             │    ┌──────────────┐  │
       ┌───────────────┐       │             │    │  categories  │◄─┘
       │   addresses   │       ▼             │    ├──────────────┤
       ├───────────────┤  ┌──────────────┐   │    │ PK category_id│
       │ PK address_id │  │ order_items  │   │    │    name      │
       │ FK customer_id│  ├──────────────┤   │    │    parent_id │
       │    line_1     │  │ PK item_id   │   │    └──────────────┘
       │    line_2     │  │ FK order_id  │───┘
       │    city       │  │ FK product_id│        ┌──────────────┐
       │    state      │  │    quantity  │        │   payments   │
       │    postal_code│  │    unit_price│        ├──────────────┤
       │    country    │  │    discount  │        │ PK payment_id│
       │    type       │  │    line_total│        │ FK order_id  │──►orders
       └───────────────┘  └──────────────┘        │    method    │
                                                  │    amount    │
       ┌───────────────┐  ┌──────────────┐        │    status    │
       │   inventory   │  │   reviews    │        │    paid_at   │
       ├───────────────┤  ├──────────────┤        └──────────────┘
       │ FK product_id │  │ PK review_id │
       │    warehouse  │  │ FK customer_id│
       │    qty_on_hand│  │ FK product_id│
       │    reserved   │  │    rating    │
       │    updated_at │  │    comment   │
       └───────────────┘  │    created_at│
                          └──────────────┘
```

---

## Table Definitions

### customers
The core customer accounts table. When a customer updates their email or region,
the row is updated **in place** — no history is kept (this is the problem SCD Type 2 solves in the lakehouse).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **customer_id** | SERIAL (PK) | NO | Auto-increment primary key |
| email | VARCHAR(255) | NO | Unique, used for login |
| first_name | VARCHAR(100) | NO | |
| last_name | VARCHAR(100) | NO | |
| phone | VARCHAR(20) | YES | Optional contact number |
| region | VARCHAR(50) | NO | Geographic region |
| signup_date | DATE | NO | Account creation date |
| updated_at | TIMESTAMP | NO | Last modification timestamp |
| is_active | BOOLEAN | NO | Soft delete flag (default true) |

> **Lakehouse extract:** customer_id, email, region, signup_date
> **What's lost without SCD2:** Previous email/region values on update

---

### products
Product catalog. Prices change frequently — the OLTP DB only keeps the current price.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **product_id** | SERIAL (PK) | NO | Auto-increment primary key |
| name | VARCHAR(200) | NO | Display name |
| category_id | INTEGER (FK) | NO | References categories |
| price | DECIMAL(10,2) | NO | Current selling price (USD) |
| sku | VARCHAR(50) | NO | Unique stock-keeping unit |
| description | TEXT | YES | Product description |
| created_at | TIMESTAMP | NO | When product was added |
| updated_at | TIMESTAMP | NO | Last modification |
| is_active | BOOLEAN | NO | Whether product is listed |

> **Lakehouse extract:** product_id, product_name (= name), category (denormalized from categories.name), price

---

### categories
Normalised product categories. 8 top-level categories per BRD.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **category_id** | SERIAL (PK) | NO | Auto-increment primary key |
| name | VARCHAR(100) | NO | Category display name |
| parent_id | INTEGER (FK) | YES | Self-reference for subcategories (NULL = top level) |

**Top-level categories:** Electronics, Clothing, Home & Kitchen, Books, Sports & Outdoors, Beauty & Health, Toys & Games, Grocery

> **Lakehouse extract:** Denormalized into products as `category` (the category name string)

---

### orders
Order headers. Status transitions: `pending → completed → refunded` or `pending → cancelled`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **order_id** | SERIAL (PK) | NO | Auto-increment primary key |
| customer_id | INTEGER (FK) | NO | References customers |
| shipping_address_id | INTEGER (FK) | YES | References addresses |
| billing_address_id | INTEGER (FK) | YES | References addresses |
| order_date | DATE | NO | Date the order was placed |
| status | VARCHAR(20) | NO | pending / completed / cancelled / refunded |
| total_amount | DECIMAL(10,2) | NO | Sum of all line items after discounts |
| created_at | TIMESTAMP | NO | Order creation timestamp |
| updated_at | TIMESTAMP | NO | Last status change |

> **Lakehouse extract:** order_id, customer_id, product_id (denormalized from order_items), order_date, total_amount, status

---

### order_items
Line items within an order. One order can have multiple products.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **item_id** | SERIAL (PK) | NO | Auto-increment primary key |
| order_id | INTEGER (FK) | NO | References orders |
| product_id | INTEGER (FK) | NO | References products |
| quantity | INTEGER | NO | Units ordered |
| unit_price | DECIMAL(10,2) | NO | Price at time of purchase (snapshot) |
| discount | DECIMAL(10,2) | NO | Discount applied (default 0.00) |
| line_total | DECIMAL(10,2) | NO | (quantity * unit_price) - discount |

> **Lakehouse note:** The data generators simplify this to 1 product per order for the
> portfolio build. In a real system, the bronze layer would ingest order_items separately
> and the silver layer would handle the join.

---

### payments
Payment records linked to orders. Multiple payments possible (split payments, refunds).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **payment_id** | SERIAL (PK) | NO | Auto-increment primary key |
| order_id | INTEGER (FK) | NO | References orders |
| method | VARCHAR(30) | NO | credit_card / debit_card / paypal / bank_transfer |
| amount | DECIMAL(10,2) | NO | Payment amount (negative for refunds) |
| status | VARCHAR(20) | NO | pending / completed / failed / refunded |
| paid_at | TIMESTAMP | YES | NULL until payment completes |

> **Lakehouse:** Not extracted in Phase 1 (out of scope per BRD §3.2)

---

### addresses
Customer shipping and billing addresses. One customer can have many.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **address_id** | SERIAL (PK) | NO | Auto-increment primary key |
| customer_id | INTEGER (FK) | NO | References customers |
| line_1 | VARCHAR(200) | NO | Street address |
| line_2 | VARCHAR(200) | YES | Apt/suite/unit |
| city | VARCHAR(100) | NO | |
| state | VARCHAR(100) | YES | |
| postal_code | VARCHAR(20) | NO | |
| country | VARCHAR(50) | NO | |
| type | VARCHAR(10) | NO | shipping / billing |

> **Lakehouse:** Not extracted in Phase 1

---

### inventory
Current stock levels per product per warehouse.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **product_id** | INTEGER (FK, PK) | NO | References products |
| **warehouse** | VARCHAR(50) (PK) | NO | Warehouse location code |
| qty_on_hand | INTEGER | NO | Available stock |
| reserved | INTEGER | NO | Reserved for pending orders |
| updated_at | TIMESTAMP | NO | Last stock update |

> **Lakehouse:** Out of scope (BRD §3.2 — real-time inventory is Phase 2+)

---

### reviews
Customer product reviews and ratings.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **review_id** | SERIAL (PK) | NO | Auto-increment primary key |
| customer_id | INTEGER (FK) | NO | References customers |
| product_id | INTEGER (FK) | NO | References products |
| rating | SMALLINT | NO | 1–5 stars |
| comment | TEXT | YES | Review text |
| created_at | TIMESTAMP | NO | When review was posted |

> **Lakehouse:** Not extracted in Phase 1

---

## What the Lakehouse Extracts (Phase 1)

The daily CSV extracts pull a **denormalized subset** of this OLTP schema:

| Source Tables | → | Lakehouse CSV | Simplifications |
|--------------|---|---------------|-----------------|
| customers | → | customers.csv | Only customer_id, email, region, signup_date |
| products + categories | → | products.csv | Category name denormalized in; only id, name, category, price |
| orders + order_items | → | orders.csv | Flattened to 1 product per order; only id, customer_id, product_id, date, amount, status |

The clickstream data (page_view, add_to_cart, purchase, search events) does **not** come from this OLTP database — it flows via Confluent Kafka from the website's event tracking layer.
