-- Run against MariaDB database `maria` to prove the pipeline behaves correctly.

-- TEST A - Updates are picked up (incremental on updated_at + upsert)
-- updated_at changes automatically (ON UPDATE CURRENT_TIMESTAMP).
UPDATE transactions SET transaction_status = 'SUCCESS'
WHERE transaction_status = 'PENDING'
ORDER BY transaction_id
LIMIT 3;
-- -> next batch: loaded_count >= 3, and those rows show SUCCESS in dwh.fact_transaction

-- TEST B - Bad data is rejected, not loaded
INSERT INTO transactions
  (transaction_id, account_id, transaction_type, transaction_ts, amount, currency,
   transaction_status, counterparty_account_id, description)
VALUES
  (900000000000001, 2001, 'REFUND',   NOW(), 10.00, 'USD', 'SUCCESS', NULL, 'Unknown type test'),
  (900000000000002, 2002, 'TRANSFER', NOW(), 20.00, 'USD', 'SUCCESS', NULL, 'Transfer w/o counterparty'),
  (900000000000003, 2003, 'DEPOSIT',  NOW(), 30.00, 'KHR', 'SUCCESS', NULL, 'Wrong currency test');
-- -> next batch: rejected_count = 3, and the task log of `transform` prints the reasons

-- Clean up test B afterwards (optional)
-- DELETE FROM transactions WHERE transaction_id IN (900000000000001, 900000000000002, 900000000000003);
