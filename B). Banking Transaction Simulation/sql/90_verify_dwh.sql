-- Run against PostgreSQL database banking_dwh (user dwh_etl).

-- 1) Latest batches
SELECT batch_id, status, extracted_count, accepted_count, rejected_count, loaded_count,
       started_at, finished_at, finished_at - started_at AS duration, error_message
FROM audit.etl_batch_log
ORDER BY started_at DESC
LIMIT 10;

-- 2) Totals in the warehouse (compare with MariaDB: SELECT COUNT(*) FROM transactions;)
SELECT COUNT(*) AS fact_rows,
       COUNT(DISTINCT source_transaction_id) AS distinct_source_ids,   -- must equal fact_rows
       MAX(source_updated_at) AS watermark
FROM dwh.fact_transaction;

-- 3) Latency: how long after a transaction happens does it reach the DWH?
SELECT ROUND(AVG(EXTRACT(EPOCH FROM (dw_loaded_at AT TIME ZONE current_setting('TimeZone')) - transaction_ts))) AS avg_seconds,
       ROUND(MAX(EXTRACT(EPOCH FROM (dw_loaded_at AT TIME ZONE current_setting('TimeZone')) - transaction_ts))) AS max_seconds
FROM dwh.fact_transaction
WHERE dw_loaded_at > now() - INTERVAL '10 minutes';

-- 4) Business query on the star schema: daily volume by type and status
SELECT d.full_date, t.transaction_type, f.transaction_status,
       COUNT(*) AS txn_count, SUM(f.amount) AS total_amount
FROM dwh.fact_transaction f
JOIN dwh.dim_date d             ON d.date_key = f.transaction_date_key
JOIN dwh.dim_transaction_type t ON t.transaction_type_key = f.transaction_type_key
GROUP BY d.full_date, t.transaction_type, f.transaction_status
ORDER BY d.full_date DESC, t.transaction_type, f.transaction_status;

-- 5) Business query: balance-style summary per account (successful only)
SELECT a.account_number, a.account_type,
       SUM(CASE WHEN t.transaction_type = 'DEPOSIT' THEN f.amount ELSE 0 END)  AS money_in,
       SUM(CASE WHEN t.transaction_type <> 'DEPOSIT' THEN f.amount ELSE 0 END) AS money_out,
       COUNT(*) AS txn_count
FROM dwh.fact_transaction f
JOIN dwh.dim_account a          ON a.account_key = f.account_key
JOIN dwh.dim_transaction_type t ON t.transaction_type_key = f.transaction_type_key
WHERE f.transaction_status = 'SUCCESS'
GROUP BY a.account_number, a.account_type
ORDER BY a.account_number;
