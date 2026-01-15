-- Initial database setup
-- Creates schema_migrations table for tracking migrations
-- Note: Survey data is stored in Parquet files, not DuckDB tables

CREATE TABLE IF NOT EXISTS schema_migrations (
    name VARCHAR PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
