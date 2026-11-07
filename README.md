# Week 4 — Banking Batch ETL Pipeline (MariaDB OLTP → Apache Airflow → PostgreSQL DWH)

An end-to-end **Batch ETL Pipeline** for a simulated banking system built with **MariaDB (OLTP)**, **Apache Airflow 3**, **Python**, and **PostgreSQL (Star Schema Data Warehouse)**.

---

## Architecture

```text
[Python Transaction Generator] (1 transaction / 5 seconds)
        │
        ▼
[MariaDB OLTP (`maria`)]  ──► Tables: customers, accounts, transactions
        │
        │  (Scheduled every 30 seconds)
        ▼
[Apache Airflow DAG: `week4_banking_batch_etl`]
   ├── 1. start_batch      : Creates RUNNING audit entry & reads watermark timestamp
   ├── 2. sync_dim_account : Upserts bank accounts into dwh.dim_account (SCD Type 1)
   ├── 3. extract          : Incrementally extracts rows where updated_at >= watermark
   ├── 4. transform        : Cleans data, validates rules & resolves surrogate keys
   └── 5. load             : Upserts dim_date & fact_transaction, marks batch SUCCESS
        │
        ▼
[PostgreSQL Data Warehouse (`banking_dwh`)]
   ├── Schema `dwh`   : dim_account, dim_date, dim_transaction_type, fact_transaction
   └── Schema `audit` : etl_batch_log
```

---

## Repository Structure

```text
├── README.md
├── Week4_ETL_Lab_Report.md
├── A). Tool Setup/
│   ├── airflow/airflow/
│   │   ├── docker-compose.yaml
│   │   ├── airflow_quick_setup_guideline.txt
│   │   ├── create_airflow_database.txt
│   │   └── dags/
│   │       ├── hello_etl.py
│   │       ├── banking_batch_etl.py          # Initial schedule scaffold
│   │       ├── week4_connection_check.py     # MariaDB & PostgreSQL smoke test DAG
│   │       └── week4_banking_batch_etl.py    # Complete 5-task Banking Batch ETL DAG
│   └── minio/minio/
│       ├── docker-compose.yaml
│       └── minio_quick_setup_guide.txt
└── B). Banking Transaction Simulation/
    ├── 01_mariadb_oltp.sql                   # OLTP schema & seed data
    ├── 02_postgresql_dwh.sql                 # Star schema & audit table DDL
    ├── sql/
    │   ├── 00_mariadb_setup.sql              # MariaDB database & user setup
    │   ├── 00_postgres_setup.sql             # PostgreSQL database & user setup
    │   ├── 01_heidisql_mariadb_full_setup.sql# Full MariaDB setup script for HeidiSQL
    │   ├── 90_verify_dwh.sql                 # Star schema & audit verification queries
    │   └── 91_test_scenarios_mariadb.sql     # Update & bad-data rejection test cases
    └── banking_transaction_simulation/
        ├── README.txt
        ├── requirements-generator.txt
        ├── synthetic_banking_transaction_generator.py
        └── run_generator.bat                 # One-click Windows launcher for generator
```

---

## Key Features
- **Incremental Extraction (Watermark):** Reads `MAX(source_updated_at)` from `dwh.fact_transaction` so each 30-second micro-batch only extracts newly inserted or updated OLTP rows.
- **Data Quality & Validation:** Rejects negative amounts, unknown accounts/transaction types, invalid currencies/statuses, and `TRANSFER` records missing a `counterparty_account_id`.
- **Surrogate Key Lookup:** Maps OLTP natural IDs to warehouse surrogate keys (`account_key`, `transaction_type_key`, `transaction_date_key`).
- **Idempotent Upserts:** Uses `INSERT ... ON CONFLICT (source_transaction_id) DO UPDATE` so re-running batches never creates duplicate fact rows.
- **Full Audit Logging:** Records `extracted_count`, `accepted_count`, `rejected_count`, `loaded_count`, execution timestamps, and error details in `audit.etl_batch_log`.

See **[Week4_ETL_Lab_Report.md](./Week4_ETL_Lab_Report.md)** for the full step-by-step lab report.
