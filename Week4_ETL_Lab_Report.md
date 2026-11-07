# Week 4 Lab Report: Banking Batch ETL Pipeline

## 1. Project Overview & Objective
The goal of this lab is to build an automated **Batch ETL (Extract, Transform, Load) Pipeline** for a simulated banking system. 

- **Source (OLTP):** A live Python generator inserts synthetic customer transactions (Deposits, Withdrawals, Transfers, Payments) every **5 seconds** into a **MariaDB** operational database (`maria`).
- **Orchestrator:** **Apache Airflow** runs a scheduled micro-batch pipeline (`week4_banking_batch_etl`) every **30 seconds**.
- **Target (Data Warehouse / OLAP):** Airflow extracts the new/updated transactions, cleans and validates them, maps them to surrogate keys, and loads them into a **PostgreSQL Star Schema** (`banking_dwh`), while logging batch statistics in an audit table (`audit.etl_batch_log`).

---

## 2. System Architecture

```text
[Python Generator] (1 txn / 5 sec)
        │
        ▼
[MariaDB OLTP (`maria`)]  ──► Tables: customers, accounts, transactions
        │
        │  (Airflow runs every 30 seconds)
        ▼
[Apache Airflow DAG: `week4_banking_batch_etl`]
   1. start_batch      -> Logs batch as RUNNING & gets watermark timestamp
   2. sync_dim_account -> Upserts account dimension (SCD Type 1)
   3. extract          -> Reads transactions where updated_at >= watermark
   4. transform        -> Validates data quality & looks up surrogate keys
   5. load             -> Upserts dim_date & fact_transaction, marks batch SUCCESS
        │
        ▼
[PostgreSQL DWH (`banking_dwh`)]
   ├── Schema `dwh`   : dim_account, dim_date, dim_transaction_type, fact_transaction
   └── Schema `audit` : etl_batch_log
```

---

## 3. Steps Completed to Finish the Lab

### Step 1: Set Up the OLTP Source Database (MariaDB via HeidiSQL)
- Connected to MariaDB (port `3307`) in **HeidiSQL**.
- Created the `maria` database and user `maria`.
- Created the 3 operational tables (`customers`, `accounts`, `transactions`) using `01_mariadb_oltp.sql` and inserted the seed records (4 customers and 5 bank accounts: `ACC100001` to `ACC100005`).

### Step 2: Set Up the Target Data Warehouse (PostgreSQL via pgAdmin 4 / DBeaver)
- Created the `dwh_etl` user and `banking_dwh` database in PostgreSQL (port `5432`).
- Executed `02_postgresql_dwh.sql` to create two schemas:
  - **`dwh` schema (Star Schema):** `dim_date`, `dim_account`, `dim_transaction_type`, and `fact_transaction`.
  - **`audit` schema:** `etl_batch_log` to track `extracted_count`, `accepted_count`, `rejected_count`, `loaded_count`, and batch status (`RUNNING`, `SUCCESS`, `FAILED`).

### Step 3: Run the Live Banking Transaction Generator
- Installed `PyMySQL` (`requirements-generator.txt`) and ran `synthetic_banking_transaction_generator.py`.
- Verified that new synthetic transactions are continuously inserted into MariaDB every 5 seconds.

### Step 4: Configure Database Connections in Apache Airflow
- Opened the Airflow Web UI (`http://localhost:8080` -> **Admin -> Connections**) and configured:
  1. **`maria_oltp`** (MySQL connection to `host.docker.internal:3307`, schema `maria`).
  2. **`postgres_dwh`** (Postgres connection to `host.docker.internal:5432`, schema `banking_dwh`).
- Triggered the `week4_connection_check` DAG and verified both connections succeeded.

### Step 5: Build and Run the Batch ETL DAG (`week4_banking_batch_etl`)
- Replaced the placeholder DAG with the complete 5-task ETL pipeline in `C:\Airflow\dags\week4_banking_batch_etl.py`:
  - **Incremental Extraction:** Uses `MAX(source_updated_at)` from `dwh.fact_transaction` as a watermark so only new or updated rows are extracted each batch.
  - **Data Quality & Transformation:** Trims/standardizes fields, checks business rules (non-negative amount, valid account/type/status/currency, and requires a counterparty for `TRANSFER`), separates accepted vs. rejected rows, and resolves natural IDs to warehouse surrogate keys (`account_key`, `transaction_type_key`, `date_key`).
  - **Idempotent Load:** Automatically inserts missing calendar dates into `dim_date` and upserts `fact_transaction` using `ON CONFLICT (source_transaction_id) DO UPDATE`, preventing duplicates on re-runs.
- Unpaused the DAG and ran it continuously for over 5 minutes (10+ consecutive successful 30-second batch runs).

### Step 6: Verification & Edge-Case Testing
- **Star Schema Verification:** Queried `dwh.fact_transaction` joined with `dim_account`, `dim_date`, and `dim_transaction_type` in **pgAdmin 4** to verify account totals (`money_in`, `money_out`, `txn_count`) and confirmed zero duplicate `source_transaction_id` records.
- **Update & Bad-Data Testing:**
  - Updated 3 `PENDING` transactions to `SUCCESS` in MariaDB and confirmed the next ETL batch updated them in PostgreSQL.
  - Inserted 3 invalid transactions (unknown type `REFUND`, `TRANSFER` without counterparty, and invalid currency `KHR`) and confirmed the ETL rejected all 3 (`rejected_count = 3` in `audit.etl_batch_log`) while loading valid transactions normally.
