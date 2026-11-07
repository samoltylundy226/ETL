"""Week 4 - connectivity smoke test. Trigger manually; both tasks must be green
before you enable the real ETL DAG."""
from datetime import datetime

from airflow.sdk import dag, task


@dag(dag_id="week4_connection_check", start_date=datetime(2026, 1, 1),
     schedule=None, catchup=False, tags=["week4", "check"])
def week4_connection_check():

    @task
    def check_mariadb():
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        hook = MySqlHook(mysql_conn_id="maria_oltp")
        print("MariaDB version :", hook.get_first("SELECT VERSION()")[0])
        print("accounts        :", hook.get_first("SELECT COUNT(*) FROM accounts")[0])
        print("transactions    :", hook.get_first("SELECT COUNT(*) FROM transactions")[0])

    @task
    def check_postgres():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        print("PostgreSQL      :", hook.get_first("SELECT version()")[0])
        print("DWH tables      :", hook.get_records(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables "
            "WHERE table_schema IN ('dwh','audit') ORDER BY 1"))

    check_mariadb()
    check_postgres()


week4_connection_check()
