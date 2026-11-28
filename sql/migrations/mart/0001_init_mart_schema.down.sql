-- Migration mart 0001 (DOWN): gỡ lược đồ mart theo thứ tự phụ thuộc ngược.

DROP TABLE IF EXISTS mart_publish_metadata;
DROP TABLE IF EXISTS fact_reconciliation;
DROP TABLE IF EXISTS fact_daily_balances;
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_transaction_type;
DROP TABLE IF EXISTS dim_channel;
DROP TABLE IF EXISTS dim_account;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS schema_migrations;
