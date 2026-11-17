-- Databricks SQL, run on a SQL warehouse (e.g. with ./databricks-sql.sh -f ddl/databricks/charglt_events.sql)
-- in the catalog and schema that Lakebase Change Data Feed writes to. CDF creates
-- lb_charglt_events_history there once charglt_events has at least one row.

-- One row per charglt transaction. The history table also holds 'delete' rows from
-- PurgeChargltEvents, which are dropped here. The QUALIFY guards against duplicates if
-- CDF re-snapshots the table (which it does after a column change on the source).
CREATE OR REPLACE VIEW charglt_transactions AS
SELECT event_id, userid, user_txn_id, txn_time, sessionid,
       approved_amount, spent_amount, purpose,
       _timestamp AS cdf_time
FROM lb_charglt_events_history
WHERE _pg_change_type = 'insert'
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _sort_by) = 1;

-- Per-minute activity, split like the recent_activity_in / recent_activity_out views in Postgres.
CREATE OR REPLACE VIEW charglt_activity_by_minute AS
SELECT DATE_TRUNC('MINUTE', txn_time) AS txn_minute,
       purpose,
       COUNT(*) AS how_many,
       SUM(approved_amount) AS approved_amount,
       SUM(spent_amount) AS spent_amount
FROM charglt_transactions
GROUP BY ALL;
