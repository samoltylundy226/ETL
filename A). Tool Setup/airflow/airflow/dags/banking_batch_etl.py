"""
Week 4 ETL schedule example.

Airflow can schedule a DAG every 30 seconds with timedelta(seconds=30), but the
actual start time depends on scheduler/worker latency. This DAG is intentionally
a safe connectivity-and-schedule scaffold, not the finished Spark ETL: the Spark
job path and submission mechanism must be configured for this lab before adding
the actual ETL task.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def schedule_placeholder():
    # Replace this with the actual Spark ETL submission task once its location
    # and Airflow-to-Spark execution method are configured.
    print("30-second schedule tick: Spark ETL submission not configured yet.")

with DAG(
    dag_id="week4_banking_batch_etl",
    description="Week 4 banking batch ETL schedule (30-second development cadence)",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(seconds=30),
    catchup=False,
    max_active_runs=1,
    tags=["week4", "banking", "batch-etl"],
) as dag:
    scheduled_tick = PythonOperator(
        task_id="scheduled_tick",
        python_callable=schedule_placeholder,
        execution_timeout=timedelta(seconds=20),
    )
