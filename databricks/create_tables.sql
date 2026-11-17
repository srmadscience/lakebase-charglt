-- Databricks SQL: the Delta table the charglt-export job writes to. Run once, before the
-- first export:
--   ./databricks-sql.sh -f databricks/create_tables.sql
-- Unqualified names resolve in workspace.charglt (see databricks-sql.sh). If you set
-- CHARGLT_SCHEMA to something else, change the schema name below to match.

CREATE SCHEMA IF NOT EXISTS charglt;

-- One row per charglt transaction, copied from Lakebase charglt_events.
-- CLUSTER BY event_id lets each export MERGE skip files outside the batch's event_id range.
CREATE TABLE IF NOT EXISTS transactions (
  event_id BIGINT,
  userid BIGINT,
  user_txn_id STRING,
  txn_time TIMESTAMP_NTZ,
  sessionid BIGINT,
  approved_amount BIGINT,
  spent_amount BIGINT,
  purpose STRING,
  exported_at TIMESTAMP
)
CLUSTER BY (event_id)
COMMENT 'charglt transactions exported from Lakebase charglt_events';
