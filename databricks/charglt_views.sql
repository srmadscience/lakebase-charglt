-- Databricks SQL over the table the charglt-export job writes. Run with:
--   ./databricks-sql.sh -f databricks/charglt_views.sql
-- Unqualified names resolve in workspace.charglt (see databricks-sql.sh).

-- Per-minute activity, split by purpose (Create User, Spend, Add Credit).
CREATE OR REPLACE VIEW activity_by_minute AS
SELECT DATE_TRUNC('MINUTE', txn_time) AS txn_minute,
       purpose,
       COUNT(*) AS how_many,
       SUM(approved_amount) AS approved_amount,
       SUM(spent_amount) AS spent_amount
FROM transactions
GROUP BY ALL;

-- How far behind Lakebase the export runs.
CREATE OR REPLACE VIEW export_lag AS
SELECT MAX(txn_time) AS last_txn_time,
       MAX(exported_at) AS last_exported_at,
       COUNT(*) AS rows_exported
FROM transactions;

-- Spend per user per hour. Materialized, so it is only as fresh as its last refresh:
--   ./databricks-sql.sh 'REFRESH MATERIALIZED VIEW user_hourly_spend'
-- Add SCHEDULE EVERY 1 HOUR to refresh it automatically (each refresh uses serverless compute).
CREATE OR REPLACE MATERIALIZED VIEW user_hourly_spend AS
SELECT userid,
       DATE_TRUNC('HOUR', txn_time) AS txn_hour,
       COUNT(*) AS how_many,
       SUM(spent_amount) AS spent_amount
FROM transactions
GROUP BY userid, DATE_TRUNC('HOUR', txn_time);

-- Each approved credit with its outcome; applied_at IS NULL means not applied yet.
CREATE OR REPLACE VIEW approved_addcredits_status AS
SELECT a.request_id, a.userid, a.add_credit_value, a.approved_at,
       r.status_byte, r.status_string, r.applied_at
FROM approved_addcredits a
LEFT JOIN approved_addcredit_results r ON r.request_id = a.request_id;
