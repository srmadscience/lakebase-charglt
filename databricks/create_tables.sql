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

-- Credits approved in Databricks, to be applied in Lakebase by the charglt-apply-credits job
-- (databricks/charglt_apply_credits.py). Add a request with:
--   INSERT INTO approved_addcredits (userid, add_credit_value) VALUES (42, 100);
-- request_id identifies the request to Lakebase, so it is applied at most once. The job
-- fills in the status columns: applied_at IS NULL means not yet applied, and status_byte
-- is AddCredit's (56 = added, 50 = user does not exist).
-- Row tracking and deletion vectors give row-level concurrency, so inserts and the job's
-- status updates don't conflict.
CREATE TABLE IF NOT EXISTS approved_addcredits (
  request_id BIGINT GENERATED ALWAYS AS IDENTITY,
  userid BIGINT NOT NULL,
  add_credit_value BIGINT NOT NULL,
  approved_at TIMESTAMP DEFAULT current_timestamp(),
  status_byte INT,
  status_string STRING,
  applied_at TIMESTAMP
)
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.enableRowTracking' = 'true',
  'delta.enableDeletionVectors' = 'true'
)
COMMENT 'Approved credits for charglt users, applied in Lakebase by AddCredit';
