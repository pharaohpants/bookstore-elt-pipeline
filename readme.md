# 📚 PacBook Store — End-to-End Data Engineering Pipeline

![Pipeline Architecture](assets/architecture.png)

> An end-to-end ELT pipeline that separates operational workloads from analytical workloads using a modern data stack — enabling the business intelligence team to query insights in seconds without disrupting the live application.

---

## 📌 Table of Contents
- [Project Background](#-project-background)
- [Tech Stack](#️-tech-stack)
- [Data Warehouse Design](#-data-warehouse-design)
- [Pipeline Architecture](#-pipeline-architecture)
- [Engineering Highlights](#-engineering-highlights)
- [Testing Scenarios](#-testing-scenarios)
- [How to Run](#-how-to-run)
- [Project Structure](#-project-structure)

---

## 📖 Project Background

**Problem:**
PacBook's operational database (PostgreSQL) handled both daily transactions and analytical queries on the same server. Every time the analytics team ran monthly reports, it caused heavy compute load — slowing down the live application for customers.

**Solution:**
Build a dedicated **Data Warehouse** using dimensional modeling to separate the two workloads entirely. The ELT pipeline automatically moves and transforms data every night, so analysts can query clean, structured data without touching the production database.

**Business questions this pipeline answers:**
- What are the monthly sales trends?
- Which books have the highest total sales over time?
- How long does it take for customers to make a repeat order on average?
- Which customer segments and products are most profitable?

---

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Source Database (OLTP) | PostgreSQL |
| Orchestration | Apache Airflow |
| Transformation & Testing | dbt (Data Build Tool) |
| Data Warehouse (OLAP) | Snowflake |
| Containerization | Docker, Docker Compose |
| Language | Python, SQL |

---

## 📊 Data Warehouse Design

### Business Process
Book sales transactions at PacBook Store.

### Grain
- `fct_order_line` — one row per book item per order transaction (most granular)
- `fct_daily_book_sales` — one row per book per day (aggregated snapshot)

### Dimension Tables

| Table | Description | SCD Strategy |
|---|---|---|
| `dim_customer` | Customer profile and demographics | SCD Type 2 |
| `dim_book` | Book details, category, and price | SCD Type 2 |
| `dim_date` | Date attributes (year, month, quarter) | Static |

### Fact Tables

| Table | Type | Description |
|---|---|---|
| `fct_order_line` | Transaction Fact | Per-item detail for every order |
| `fct_daily_book_sales` | Periodic Snapshot | Daily aggregated sales per book title |

`fct_daily_book_sales` was added specifically to speed up monthly trend dashboards — pre-aggregating daily totals reduces query time significantly compared to recalculating from raw transaction data every time.

### Data Warehouse Diagram

![ERD Diagram](assets/data_warehouse_erd.png)

---

## 🏗️ Pipeline Architecture

The pipeline follows an **ELT (Extract → Load → Transform)** pattern and runs automatically every night via Airflow.

```
[PostgreSQL - Source DB]
        |
        | Python (extraction)
        ↓
[Snowflake - Staging Layer]
        |
        | dbt (transformation + testing)
        ↓
[Snowflake - Data Warehouse]
   ├── dim_customer (SCD Type 2)
   ├── dim_book (SCD Type 2)
   ├── dim_date
   ├── fct_order_line
   └── fct_daily_book_sales
```

**Airflow DAG:**

![Airflow DAG](assets/dag_graph.png)

**DAG steps:**
1. `extract_source` — Extract tables from PostgreSQL source
2. `load_staging` — Load raw data into Snowflake staging schema
3. `dbt_run` — Execute dbt models: staging → warehouse
4. `dbt_test` — Run data quality tests
5. `notify_success` — Send success alert (or failure alert on error)

---

## ⚡ Engineering Highlights

### Slowly Changing Dimension (SCD Type 2)
`dim_customer` and `dim_book` use SCD Type 2 via **dbt snapshots**. When a customer's address or a book's price changes, the old record is closed and a new version is created — preserving historical accuracy so past sales reports always reflect the conditions at the time of purchase.

Tracking columns: `dbt_valid_from`, `dbt_valid_to`, `dbt_current_flag`

### Automated Data Quality Testing
dbt tests run after every transformation to catch issues before data reaches analysts:

| Test | What it checks |
|---|---|
| `not_null` | No missing values in key columns |
| `unique` | No duplicate primary keys |
| `relationships` | All foreign keys point to valid dimension records |
| Custom: `no_negative_quantity` | Order quantities are logically valid |

### Orphan Record Prevention
Source data occasionally contains order records where the customer or book no longer exists in the master table. Rather than dropping these rows (which would cause data loss), the pipeline substitutes a default surrogate key (`UNKNOWN_CUSTOMER_SK`, `UNKNOWN_BOOK_SK`) — maintaining row count integrity while flagging the anomaly for investigation.

---

## 🧪 Testing Scenarios

### Scenario 1 — Initial Load
**Goal:** Validate that all source data loads correctly on first run.
**Approach:** Run the full DAG and verify row counts in all warehouse tables match the source.
**Expected result:** All tables populated, dbt tests pass with zero failures.

### Scenario 2 — SCD Type 2 Behavior
**Goal:** Verify that attribute changes are tracked correctly without overwriting history.
**Approach:** Update a customer's city in the source database, then re-run the pipeline.
**Expected result:**
- Old record: `dbt_valid_to` = today, `dbt_current_flag` = false
- New record: `dbt_valid_from` = today, `dbt_current_flag` = true
- Historical orders still reference the old dimension version

### Scenario 3 — New Data Ingestion
**Goal:** Confirm the pipeline handles new records without affecting existing data.
**Approach:** Insert new order records into the source database, trigger the DAG.
**Expected result:** Only new rows are appended; existing warehouse data is unchanged.

---

## 🚀 How to Run

**Prerequisites:** Docker Desktop installed and running.

**1. Clone the repository**
```bash
git clone <YOUR_REPO_URL>
cd pacbook_store
```

**2. Set up environment variables**
```bash
cp .env.example .env
```
Open `.env` and fill in your Snowflake credentials.

**3. Start all services**
```bash
docker compose up -d
```
This starts PostgreSQL (source), Airflow webserver, and Airflow scheduler.

**4. Open Airflow UI**

Navigate to `http://localhost:8080` in your browser.
Default credentials: `airflow` / `airflow`

**5. Trigger the pipeline**

Enable and trigger the DAG named `pacbook_elt_pipeline`. You can monitor task progress in the Graph view.

![DAG Running](assets/dag_running.png)

---

## 📁 Project Structure

```
pacbook_store/
├── dags/
│   └── pacbook_pipeline_dag.py    # Airflow DAG definition
├── extraction/
│   └── extract_load.py            # Python extraction scripts
├── pacbook_dbt/
│   ├── models/
│   │   ├── staging/               # Raw → clean staging models
│   │   ├── dimensions/            # dim_customer, dim_book, dim_date
│   │   └── facts/                 # fct_order_line, fct_daily_book_sales
│   ├── snapshots/                 # SCD Type 2 snapshot definitions
│   └── tests/                     # Custom data quality tests
├── assets/                        # Architecture diagrams and screenshots
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## 🎯 Design Decisions

**Why ELT instead of ETL?**
Transformation happens inside Snowflake using dbt — leveraging the warehouse's compute power directly. This is more scalable and easier to debug than transforming data outside the warehouse first.

**Why SCD Type 2 for dim_customer and dim_book?**
Business requirements demand historical accuracy. A customer who moved cities should still show their old city against past orders — not the new one. Type 2 preserves this without overwriting history.

**Why two fact tables?**
`fct_order_line` handles granular analysis (per-item revenue, product mix). `fct_daily_book_sales` is a pre-aggregated snapshot specifically designed to speed up trend dashboards — querying daily totals is significantly faster than re-aggregating millions of line items every time a chart loads.

---

## 📌 Known Limitations & Future Improvements

- Add data freshness monitoring (dbt source freshness)
- Implement column-level data lineage documentation
- Add a BI dashboard layer (Metabase or Looker Studio)
- Extend SCD Type 2 to `dim_date` for fiscal calendar changes