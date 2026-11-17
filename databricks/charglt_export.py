# Databricks notebook source
# MAGIC %md
# MAGIC # charglt export
# MAGIC
# MAGIC Moves rows from the Lakebase `charglt_events` table (written by `SendToKafka`) into a Delta table.
# MAGIC Each batch is locked in Postgres, merged into Delta, then deleted from Postgres in the same
# MAGIC Postgres transaction. A run that fails part-way leaves the batch in Postgres; the next run merges
# MAGIC it again and the merge skips rows that are already there.
# MAGIC
# MAGIC Deployed and scheduled by `databricks-job.sh`.

# COMMAND ----------

# MAGIC %pip install --quiet "psycopg[binary]==3.2.9"

# COMMAND ----------

dbutils.widgets.text("lakebase_project", "charglt")
dbutils.widgets.text("lakebase_branch", "production")
dbutils.widgets.text("lakebase_endpoint", "primary")
dbutils.widgets.text("database", "charglt")
dbutils.widgets.text("source_schema", "public")
dbutils.widgets.text("target_table", "workspace.charglt.transactions")
dbutils.widgets.text("batch_size", "50000")
dbutils.widgets.text("max_seconds", "600")

arg = dbutils.widgets.get
endpoint = f"projects/{arg('lakebase_project')}/branches/{arg('lakebase_branch')}/endpoints/{arg('lakebase_endpoint')}"
target = arg("target_table")
batch_size = int(arg("batch_size"))
max_seconds = int(arg("max_seconds"))

# COMMAND ----------

# Target table. CLUSTER BY event_id lets each MERGE skip files outside the batch's event_id range.
catalog, schema, _ = target.split(".")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target} (
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
COMMENT 'charglt transactions exported from Lakebase charglt_events'
""")

# COMMAND ----------

# Lakebase login: the job's own identity and a one-hour OAuth token, as in lakebase-env.sh.
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
pg_host = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint}")["status"]["hosts"]["host"]
pg_password = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint})["token"]
pg_user = w.current_user.me().user_name

# COMMAND ----------

import time
import psycopg
from psycopg import sql
from pyspark.sql.types import (StructType, StructField, LongType, StringType, TimestampNTZType)

columns = ["event_id", "userid", "user_txn_id", "txn_time", "sessionid",
           "approved_amount", "spent_amount", "purpose"]
batch_schema = StructType([
    StructField("event_id", LongType(), False),
    StructField("userid", LongType(), False),
    StructField("user_txn_id", StringType(), False),
    StructField("txn_time", TimestampNTZType(), False),
    StructField("sessionid", LongType(), True),
    StructField("approved_amount", LongType(), True),
    StructField("spent_amount", LongType(), True),
    StructField("purpose", StringType(), True),
])

source = sql.Identifier(arg("source_schema"), "charglt_events")
select_batch = sql.SQL(
    "SELECT {cols} FROM {src} ORDER BY event_id LIMIT %s FOR UPDATE SKIP LOCKED"
).format(cols=sql.SQL(", ").join(map(sql.Identifier, columns)), src=source)
delete_batch = sql.SQL("DELETE FROM {src} WHERE event_id = ANY(%s)").format(src=source)

# event_id restarts if charglt_events is recreated, so the merge also matches on the
# transaction itself. The event_id range lets Delta skip files outside the batch.
merge = f"""
MERGE INTO {target} t
USING charglt_batch s
ON t.event_id BETWEEN {{lo}} AND {{hi}}
   AND t.event_id = s.event_id AND t.userid = s.userid
   AND t.user_txn_id = s.user_txn_id AND t.txn_time = s.txn_time
WHEN NOT MATCHED THEN INSERT ({", ".join(columns)}, exported_at)
  VALUES ({", ".join("s." + c for c in columns)}, current_timestamp())
"""

started = time.monotonic()
exported = 0
with psycopg.connect(host=pg_host, dbname=arg("database"), user=pg_user,
                     password=pg_password, sslmode="require", autocommit=True) as conn:
    while time.monotonic() - started < max_seconds:
        with conn.transaction():
            rows = conn.execute(select_batch, (batch_size,)).fetchall()
            if not rows:
                break
            spark.createDataFrame(rows, batch_schema).createOrReplaceTempView("charglt_batch")
            ids = [r[0] for r in rows]
            spark.sql(merge.format(lo=min(ids), hi=max(ids)))
            conn.execute(delete_batch, (ids,))
        exported += len(rows)
        print(f"{exported:,} rows exported")
        if len(rows) < batch_size:
            break

print(f"Done: {exported:,} rows in {time.monotonic() - started:.0f}s")
dbutils.notebook.exit(str(exported))
