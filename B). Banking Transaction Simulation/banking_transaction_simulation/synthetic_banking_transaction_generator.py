"""
Synthetic banking transaction generator for the Week 4 lab.

Runs on Windows and inserts one synthetic transaction every N seconds into:
  MariaDB host: 127.0.0.1
  Port: 3307 (published by mariadb-lab)
  Database: maria

Install:
  py -m pip install PyMySQL

Set credentials in PowerShell before running:
  $env:MARIA_DB_USER = "maria"
  $env:MARIA_DB_PASSWORD = "maria#123!"

Run:
  py .\generate_transactions.py

Stop with Ctrl+C. Synthetic data only; never use real banking data.
"""
import os
import random
import time
from datetime import datetime
import pymysql

HOST = os.getenv("MARIA_DB_HOST", "127.0.0.1")
PORT = int(os.getenv("MARIA_DB_PORT", "3307"))
DATABASE = os.getenv("MARIA_DB_NAME", "maria")
USER = os.getenv("MARIA_DB_USER", "maria")
PASSWORD = os.getenv("MARIA_DB_PASSWORD")
INTERVAL_SECONDS = float(os.getenv("GENERATOR_INTERVAL_SECONDS", "5"))

if not PASSWORD:
    raise SystemExit("Set MARIA_DB_PASSWORD in PowerShell before running this script.")

SAMPLE_ACCOUNTS = [2001, 2002, 2003, 2004, 2005]
TRANSACTION_TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"]
STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "PENDING", "FAILED"]
DESCRIPTIONS = {
    "DEPOSIT": ["Branch deposit", "Cash deposit", "Salary deposit"],
    "WITHDRAWAL": ["ATM withdrawal", "Cash withdrawal"],
    "TRANSFER": ["Internal transfer", "Transfer to another account"],
    "PAYMENT": ["Utility payment", "Merchant payment", "Supplier payment"],
}

def connect():
    return pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        autocommit=True,
        connect_timeout=10,
        charset="utf8mb4",
    )

def make_transaction():
    now = datetime.now().replace(microsecond=0)
    # Millisecond timestamp multiplied by 1000 plus a small random suffix.
    # Fits in BIGINT and avoids collisions for this single local generator.
    transaction_id = int(time.time() * 1000) * 1000 + random.randint(0, 999)
    account_id = random.choice(SAMPLE_ACCOUNTS)
    transaction_type = random.choice(TRANSACTION_TYPES)
    counterparty = random.choice([a for a in SAMPLE_ACCOUNTS if a != account_id]) if transaction_type == "TRANSFER" else None
    amount = round(random.uniform(5, 1500), 2)
    status = random.choice(STATUSES)
    description = random.choice(DESCRIPTIONS[transaction_type])
    return (
        transaction_id, account_id, transaction_type, now, amount, "USD",
        status, counterparty, description, now
    )

def main():
    print(f"Generating synthetic transactions into {HOST}:{PORT}/{DATABASE} every {INTERVAL_SECONDS}s. Ctrl+C to stop.")
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM accounts")
            count = cur.fetchone()[0]
            if count == 0:
                raise SystemExit("No accounts found. Run 01_mariadb_oltp.sql first.")
        while True:
            row = make_transaction()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO transactions
                        (transaction_id, account_id, transaction_type, transaction_ts,
                         amount, currency, transaction_status, counterparty_account_id,
                         description, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        row,
                    )
                print(f"Inserted transaction_id={row[0]} account_id={row[1]} "
                      f"type={row[2]} amount={row[4]:.2f} status={row[6]}")
            except pymysql.err.IntegrityError as exc:
                print(f"Skipped transaction due to integrity error: {exc}")
            except pymysql.MySQLError as exc:
                print(f"Database error: {exc}; reconnecting...")
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(2)
                conn = connect()
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
