"""
Week 4 - Banking batch ETL
==========================
Source : MariaDB (OLTP)      -> database `maria`, table `transactions`, `accounts`
Target : PostgreSQL (DWH)    -> database `banking_dwh`, schemas `dwh` + `audit`
Runs   : every 30 seconds (development cadence), one run at a time.

Task flow
---------
    start_batch ──► sync_dim_account ──┐
         │                             ├──► transform ──► load
         └────────► extract ───────────┘

* start_batch      : opens an audit row (status RUNNING) and reads the watermark
                     (latest source_updated_at already in the warehouse).
* sync_dim_account : upserts all accounts from MariaDB into dwh.dim_account (SCD type 1).
* extract          : reads transactions whose updated_at >= watermark (incremental).
* transform        : cleans, validates, looks up surrogate keys, splits accepted/rejected.
* load             : adds missing dim_date rows, upserts fact_transaction, closes the audit row.

Idempotency: fact_transaction.source_transaction_id is UNIQUE and the load uses
INSERT ... ON CONFLICT DO UPDATE, so re-running a batch never creates duplicates.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
MARIA_CONN_ID = "maria_oltp"      # Airflow connection -> MariaDB (OLTP source)
DWH_CONN_ID = "postgres_dwh"      # Airflow connection -> PostgreSQL (DWH target)
PIPELINE_NAME = "week4_banking_batch_etl"
SOURCE_NAME = "mariadb.maria.transactions"

VALID_STATUSES = {"SUCCESS", "PENDING", "FAILED"}
VALID_CURRENCIES = {"USD"}


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _batch_id(context) -> str:
    """Use the Airflow run_id as the ETL batch id (column is VARCHAR(64))."""
    return context["run_id"][:64]


def mark_batch_failed(context):
    """Task failure callback: record the failure in audit.etl_batch_log."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    error = str(context.get("exception") or "unknown error")[:2000]
    task_id = context["task_instance"].task_id
    try:
        PostgresHook(postgres_conn_id=DWH_CONN_ID).run(
            """
            INSERT INTO audit.etl_batch_log
                (batch_id, pipeline_name, source_name, status, finished_at, error_message)
            VALUES (%s, %s, %s, 'FAILED', now(), %s)
            ON CONFLICT (batch_id) DO UPDATE
               SET status = 'FAILED', finished_at = now(), error_message = EXCLUDED.error_message
            """,
            parameters=(_batch_id(context), PIPELINE_NAME, SOURCE_NAME, f"[{task_id}] {error}"),
        )
    except Exception as exc:  # the DWH itself may be the thing that is down
        print(f"Could not write FAILED status to audit log: {exc}")


# --------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------
@dag(
    dag_id="week4_banking_batch_etl",
    description="Week 4 banking batch ETL: MariaDB OLTP -> PostgreSQL star schema",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(seconds=30),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
        "on_failure_callback": mark_batch_failed,
    },
    tags=["week4", "banking", "batch-etl"],
)
def week4_banking_batch_etl():

    # ------------------------------------------------------------------ 1
    @task
    def start_batch() -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        ctx = get_current_context()
        batch_id = _batch_id(ctx)
        pg = PostgresHook(postgres_conn_id=DWH_CONN_ID)

        pg.run(
            """
            INSERT INTO audit.etl_batch_log (batch_id, pipeline_name, source_name, status)
            VALUES (%s, %s, %s, 'RUNNING')
            ON CONFLICT (batch_id) DO UPDATE
               SET status = 'RUNNING', started_at = now(), finished_at = NULL, error_message = NULL
            """,
            parameters=(batch_id, PIPELINE_NAME, SOURCE_NAME),
        )

        # Watermark = newest source change already loaded into the warehouse.
        watermark = pg.get_first(
            "SELECT COALESCE(MAX(source_updated_at), TIMESTAMP '1970-01-01') FROM dwh.fact_transaction"
        )[0]
        print(f"batch_id={batch_id} watermark={watermark}")
        return {"batch_id": batch_id, "watermark": watermark.strftime("%Y-%m-%d %H:%M:%S")}

    # ------------------------------------------------------------------ 2
    @task
    def sync_dim_account(batch: dict) -> int:
        """Full upsert of the (small) accounts table -> dwh.dim_account (SCD type 1)."""
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from psycopg2.extras import execute_values

        rows = MySqlHook(mysql_conn_id=MARIA_CONN_ID).get_records(
            """
            SELECT account_id, customer_id, account_number, account_type, currency, account_status
            FROM accounts
            """
        )
        conn = PostgresHook(postgres_conn_id=DWH_CONN_ID).get_conn()
        try:
            with conn, conn.cursor() as cur:  # `with conn` = commit on success / rollback on error
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.dim_account
                        (source_account_id, source_customer_id, account_number,
                         account_type, currency, account_status)
                    VALUES %s
                    ON CONFLICT (source_account_id) DO UPDATE SET
                        source_customer_id = EXCLUDED.source_customer_id,
                        account_number     = EXCLUDED.account_number,
                        account_type       = EXCLUDED.account_type,
                        currency           = EXCLUDED.currency,
                        account_status     = EXCLUDED.account_status
                    """,
                    rows,
                )
        finally:
            conn.close()
        print(f"dim_account synced: {len(rows)} accounts")
        return len(rows)

    # ------------------------------------------------------------------ 3
    @task
    def extract(batch: dict) -> list[dict]:
        """Incremental extract: only rows changed since the watermark."""
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # `>=` (not `>`) because DATETIME has 1-second precision: rows written in the
        # same second as the watermark could otherwise be missed. Re-reading those
        # few rows is harmless because the load step is an idempotent upsert.
        records = MySqlHook(mysql_conn_id=MARIA_CONN_ID).get_records(
            """
            SELECT transaction_id, account_id, transaction_type, transaction_ts, amount,
                   currency, transaction_status, counterparty_account_id, description, updated_at
            FROM transactions
            WHERE updated_at >= %s
            ORDER BY updated_at, transaction_id
            """,
            parameters=(batch["watermark"],),
        )

        cols = ["transaction_id", "account_id", "transaction_type", "transaction_ts", "amount",
                "currency", "transaction_status", "counterparty_account_id", "description", "updated_at"]
        rows = []
        for rec in records:
            r = dict(zip(cols, rec))
            # Make values JSON-safe for XCom (keep money as string -> no float rounding).
            r["transaction_ts"] = r["transaction_ts"].strftime("%Y-%m-%d %H:%M:%S")
            r["updated_at"] = r["updated_at"].strftime("%Y-%m-%d %H:%M:%S")
            r["amount"] = str(r["amount"])
            rows.append(r)

        PostgresHook(postgres_conn_id=DWH_CONN_ID).run(
            "UPDATE audit.etl_batch_log SET extracted_count = %s WHERE batch_id = %s",
            parameters=(len(rows), batch["batch_id"]),
        )
        print(f"Extracted {len(rows)} rows (updated_at >= {batch['watermark']})")
        return rows

    # ------------------------------------------------------------------ 4
    @task
    def transform(rows: list[dict], batch: dict, _accounts_synced: int) -> dict:
        """Clean + validate + surrogate-key lookup. Returns accepted and rejected rows."""
        from decimal import Decimal, InvalidOperation
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        account_keys = dict(pg.get_records("SELECT source_account_id, account_key FROM dwh.dim_account"))
        type_keys = dict(pg.get_records(
            "SELECT transaction_type, transaction_type_key FROM dwh.dim_transaction_type"))

        # De-duplicate inside the batch: keep the latest version of each transaction.
        latest: dict[int, dict] = {}
        for r in rows:
            prev = latest.get(r["transaction_id"])
            if prev is None or r["updated_at"] >= prev["updated_at"]:
                latest[r["transaction_id"]] = r

        accepted, rejected = [], []
        for r in latest.values():
            # --- cleaning / standardisation
            ttype = (r["transaction_type"] or "").strip().upper()
            status = (r["transaction_status"] or "").strip().upper()
            currency = (r["currency"] or "").strip().upper()
            description = (r["description"] or "").strip() or None

            # --- validation rules
            reasons = []
            try:
                amount = Decimal(r["amount"])
                if amount < 0:
                    reasons.append("negative amount")
            except (InvalidOperation, TypeError):
                amount = None
                reasons.append("invalid amount")
            if r["account_id"] not in account_keys:
                reasons.append(f"unknown account_id {r['account_id']}")
            if ttype not in type_keys:
                reasons.append(f"unknown transaction_type '{ttype}'")
            if status not in VALID_STATUSES:
                reasons.append(f"invalid status '{status}'")
            if currency not in VALID_CURRENCIES:
                reasons.append(f"invalid currency '{currency}'")
            if ttype == "TRANSFER" and r["counterparty_account_id"] is None:
                reasons.append("TRANSFER without counterparty")

            if reasons:
                rejected.append({"transaction_id": r["transaction_id"], "reasons": reasons})
                continue

            ts = datetime.strptime(r["transaction_ts"], "%Y-%m-%d %H:%M:%S")
            accepted.append({
                "source_transaction_id": r["transaction_id"],
                "account_key": account_keys[r["account_id"]],
                "transaction_type_key": type_keys[ttype],
                "transaction_date_key": int(ts.strftime("%Y%m%d")),
                "full_date": ts.strftime("%Y-%m-%d"),
                "transaction_ts": r["transaction_ts"],
                "amount": str(amount),
                "currency": currency,
                "transaction_status": status,
                "counterparty_account_id": r["counterparty_account_id"],
                "description": description,
                "source_updated_at": r["updated_at"],
            })

        for rej in rejected:
            print(f"REJECTED transaction_id={rej['transaction_id']}: {', '.join(rej['reasons'])}")
        print(f"Transform: {len(accepted)} accepted, {len(rejected)} rejected")
        return {"accepted": accepted, "rejected": rejected}

    # ------------------------------------------------------------------ 5
    @task
    def load(result: dict, batch: dict, extracted: list[dict]) -> int:
        """Load dim_date + fact_transaction and close the audit row - all in ONE transaction."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from psycopg2.extras import execute_values

        accepted = result["accepted"]
        rejected = result["rejected"]

        # dim_date rows for every date present in this batch (the seed only covers 3 days).
        dates = sorted({datetime.strptime(a["full_date"], "%Y-%m-%d").date() for a in accepted})
        date_rows = [
            (int(d.strftime("%Y%m%d")), d, d.day, d.month, d.strftime("%B"),
             (d.month - 1) // 3 + 1, d.year, d.isoweekday())
            for d in dates
        ]
        fact_rows = [
            (a["source_transaction_id"], a["account_key"], a["transaction_type_key"],
             a["transaction_date_key"], a["transaction_ts"], a["amount"], a["currency"],
             a["transaction_status"], a["counterparty_account_id"], a["description"],
             a["source_updated_at"], batch["batch_id"])
            for a in accepted
        ]

        conn = PostgresHook(postgres_conn_id=DWH_CONN_ID).get_conn()
        try:
            with conn, conn.cursor() as cur:
                if date_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO dwh.dim_date
                            (date_key, full_date, day_of_month, month_number, month_name,
                             quarter_number, year_number, day_of_week)
                        VALUES %s
                        ON CONFLICT (date_key) DO NOTHING
                        """,
                        date_rows,
                    )

                changed = []
                if fact_rows:
                    # Insert new transactions; update existing ones only if the source row
                    # really changed (e.g. PENDING -> SUCCESS). Unchanged re-reads are skipped.
                    changed = execute_values(
                        cur,
                        """
                        INSERT INTO dwh.fact_transaction
                            (source_transaction_id, account_key, transaction_type_key,
                             transaction_date_key, transaction_ts, amount, currency,
                             transaction_status, counterparty_account_id, description,
                             source_updated_at, etl_batch_id)
                        VALUES %s
                        ON CONFLICT (source_transaction_id) DO UPDATE SET
                            account_key             = EXCLUDED.account_key,
                            transaction_type_key    = EXCLUDED.transaction_type_key,
                            transaction_date_key    = EXCLUDED.transaction_date_key,
                            transaction_ts          = EXCLUDED.transaction_ts,
                            amount                  = EXCLUDED.amount,
                            currency                = EXCLUDED.currency,
                            transaction_status      = EXCLUDED.transaction_status,
                            counterparty_account_id = EXCLUDED.counterparty_account_id,
                            description             = EXCLUDED.description,
                            source_updated_at       = EXCLUDED.source_updated_at,
                            etl_batch_id            = EXCLUDED.etl_batch_id,
                            dw_loaded_at            = CURRENT_TIMESTAMP
                        WHERE dwh.fact_transaction.source_updated_at IS DISTINCT FROM EXCLUDED.source_updated_at
                        RETURNING source_transaction_id
                        """,
                        fact_rows,
                        page_size=1000,
                        fetch=True,
                    )
                loaded = len(changed)

                cur.execute(
                    """
                    UPDATE audit.etl_batch_log
                       SET status = 'SUCCESS', finished_at = now(),
                           extracted_count = %s, accepted_count = %s,
                           rejected_count = %s, loaded_count = %s, error_message = NULL
                     WHERE batch_id = %s
                    """,
                    (len(extracted), len(accepted), len(rejected), loaded, batch["batch_id"]),
                )
        finally:
            conn.close()

        print(f"Loaded {loaded} new/changed fact rows; {len(date_rows)} date(s) ensured")
        return loaded

    # ------------------------------------------------------------------ wiring
    batch = start_batch()
    accounts_synced = sync_dim_account(batch)
    raw_rows = extract(batch)
    result = transform(raw_rows, batch, accounts_synced)
    load(result, batch, raw_rows)


week4_banking_batch_etl()
