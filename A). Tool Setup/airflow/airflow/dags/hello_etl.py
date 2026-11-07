from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="hello_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["week4", "etl"],
)
def hello_etl():

    @task
    def extract():
        print("Extract: read source data")

    @task
    def transform():
        print("Transform: clean and prepare data")

    @task
    def load():
        print("Load: write data to target database")

    extract() >> transform() >> load()


hello_etl()