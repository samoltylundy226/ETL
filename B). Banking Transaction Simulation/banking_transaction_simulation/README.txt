WEEK 4 BANKING DATA GENERATOR + AIRFLOW SCHEDULE SCAFFOLD
========================================================

1) This generator runs on Windows and connects to mariadb-lab via the published port 3307.
   It requires the OLTP schema/tables from 01_mariadb_oltp.sql to exist first.

2) Install Python dependency:
   py -m pip install -r .\requirements-generator.txt

3) Set the MariaDB password in PowerShell (replace with the password you set):
   $env:MARIA_DB_USER = "maria"
   $env:MARIA_DB_PASSWORD = "maria#123!"

4) Run the generator:
   py .\scripts\generate_transactions.py
   It inserts one synthetic transaction every 5 seconds by default. Stop with Ctrl+C.
   Change cadence for the generator only:
   $env:GENERATOR_INTERVAL_SECONDS = "2"

5) Airflow DAG:
   Copy dags\week4_banking_batch_etl.py into the DAGs folder mounted into your existing
   Airflow services, then check Airflow UI at http://localhost:8080.
   The DAG schedule is 30 seconds, max_active_runs=1, catchup=False.
   IMPORTANT: this is a schedule scaffold only; it prints a tick and does NOT yet
   run Spark ETL. The actual Spark job submission needs to be connected after the
   Spark ETL script and Airflow-to-Spark execution method are configured.

6) Why separate intervals?
   Generator cadence (default 5 seconds) simulates arriving OLTP records.
   Airflow cadence (30 seconds) runs the batch ETL more slowly, so each batch can
   process a group of new transactions. Airflow scheduling is not hard real-time;
   actual start times can be delayed by scheduler/task execution latency.

7) Synthetic data only. Never use real customer or banking data for this lab.
