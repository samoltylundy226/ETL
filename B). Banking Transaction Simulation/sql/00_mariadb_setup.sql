-- Run as MariaDB root (port 3307).
-- Creates the OLTP database and the application user used by the generator + Airflow.
CREATE DATABASE IF NOT EXISTS maria;

-- 'localhost' -> used by the Windows generator script
-- '%'         -> used by Airflow, which connects from inside Docker (not localhost)
CREATE USER IF NOT EXISTS 'maria'@'localhost' IDENTIFIED BY 'maria#123!';
CREATE USER IF NOT EXISTS 'maria'@'%'         IDENTIFIED BY 'maria#123!';

GRANT ALL PRIVILEGES ON maria.* TO 'maria'@'localhost';
GRANT ALL PRIVILEGES ON maria.* TO 'maria'@'%';
FLUSH PRIVILEGES;

SELECT user, host FROM mysql.user WHERE user = 'maria';
