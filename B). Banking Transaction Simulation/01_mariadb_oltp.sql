-- Synthetic Week 4 banking OLTP source for MariaDB 11.4
CREATE DATABASE IF NOT EXISTS maria;

-- create tables
CREATE TABLE IF NOT EXISTS customers (
  customer_id BIGINT PRIMARY KEY,
  first_name VARCHAR(80) NOT NULL,
  last_name VARCHAR(80) NOT NULL,
  customer_type VARCHAR(20) NOT NULL DEFAULT 'INDIVIDUAL',
  city VARCHAR(80),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS accounts (
  account_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  account_number VARCHAR(24) NOT NULL UNIQUE,
  account_type VARCHAR(30) NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  account_status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
  opened_at DATE NOT NULL,
  CONSTRAINT fk_accounts_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS transactions (
  transaction_id BIGINT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  transaction_type VARCHAR(30) NOT NULL,
  transaction_ts DATETIME NOT NULL,
  amount DECIMAL(18,2) NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  transaction_status VARCHAR(20) NOT NULL,
  counterparty_account_id BIGINT NULL,
  description VARCHAR(255),
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_transactions_account FOREIGN KEY (account_id) REFERENCES accounts(account_id),
  CONSTRAINT chk_transaction_amount CHECK (amount >= 0)
) ENGINE=InnoDB;


-- insert sample data
INSERT INTO customers
    (customer_id, first_name, last_name, customer_type, city, created_at)
VALUES
    (1001, 'Dara', 'Sok', 'INDIVIDUAL', 'Phnom Penh', NOW()),
    (1002, 'Sophea', 'Chan', 'INDIVIDUAL', 'Siem Reap', NOW()),
    (1003, 'Vanna', 'Lim', 'BUSINESS', 'Phnom Penh', NOW()),
    (1004, 'Borey', 'Kim', 'INDIVIDUAL', 'Battambang', NOW());


INSERT INTO accounts
    (account_id, customer_id, account_number, account_type,
     currency, account_status, opened_at)
VALUES
    (2001, 1001, 'ACC100001', 'SAVINGS', 'USD', 'ACTIVE', CURDATE()),
    (2002, 1001, 'ACC100002', 'CHECKING', 'USD', 'ACTIVE', CURDATE()),
    (2003, 1002, 'ACC100003', 'SAVINGS', 'USD', 'ACTIVE', CURDATE()),
    (2004, 1003, 'ACC100004', 'SAVINGS', 'USD', 'ACTIVE', CURDATE()),
    (2005, 1004, 'ACC100005', 'CHECKING', 'USD', 'ACTIVE', CURDATE());


-- verify results
select * from customers;
select * from accounts;
select * from transactions;