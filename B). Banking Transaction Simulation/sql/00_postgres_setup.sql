-- Run as PostgreSQL superuser `postgres`, connected to database `postgres`.
-- Creates the warehouse database and the ETL user that owns it.
CREATE USER dwh_etl WITH PASSWORD 'dwh#123!';
CREATE DATABASE banking_dwh OWNER dwh_etl;
