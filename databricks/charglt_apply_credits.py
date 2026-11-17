# Databricks notebook source
# MAGIC %md
# MAGIC # charglt apply credits
# MAGIC
# MAGIC Applies rows from the Delta table `approved_addcredits` to Lakebase by calling `AddCredit`.
# MAGIC
# MAGIC Each batch goes to Lakebase as one call to `ApplyApprovedAddCredits`, in one Postgres
# MAGIC transaction: every credit in the batch is applied and logged in `approved_addcredit_log`, or
# MAGIC none is. The log is keyed on `request_id`, so a request is never applied twice. The statuses
# MAGIC it returns are then merged into `approved_addcredit_results`. If a run fails after Postgres has
# MAGIC committed but before that merge, the next run sends the batch again, Postgres skips it, and
# MAGIC the merge writes the statuses from the log.
# MAGIC
# MAGIC The job only reads `approved_addcredits`: its table-update trigger watches that table, so a
# MAGIC write there would start another run.
# MAGIC
# MAGIC Deployed and triggered by `databricks-job.sh apply-credits`.

# COMMAND ----------

# MAGIC %pip install --quiet "psycopg[binary]==3.2.9"

# COMMAND ----------

dbutils.widgets.text("lakebase_project", "charglt")
dbutils.widgets.text("lakebase_branch", "production")
dbutils.widgets.text("lakebase_endpoint", "primary")
dbutils.widgets.text("database", "charglt")
dbutils.widgets.text("source_table", "workspace.charglt.approved_addcredits")
dbutils.widgets.text("results_table", "workspace.charglt.approved_addcredit_results")
dbutils.widgets.text("batch_size", "1000")
dbutils.widgets.text("max_seconds", "600")

arg = dbutils.widgets.get
endpoint = f"projects/{arg('lakebase_project')}/branches/{arg('lakebase_branch')}/endpoints/{arg('lakebase_endpoint')}"
source = arg("source_table")
results_table = arg("results_table")
batch_size = int(arg("batch_size"))
max_seconds = int(arg("max_seconds"))

# The tables are created by databricks/create_tables.sql.
for table in (source, results_table):
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table} does not exist: run ./databricks-sql.sh -f databricks/create_tables.sql")

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
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampNTZType

result_schema = StructType([
    StructField("request_id", LongType(), False),
    StructField("status_byte", IntegerType(), True),
    StructField("status_string", StringType(), True),
    StructField("applied_at", TimestampNTZType(), False),
])

# applied_at in the log is a Postgres TIMESTAMP without time zone, in Lakebase's UTC.
spark.conf.set("spark.sql.session.timeZone", "UTC")
merge = f"""
MERGE INTO {results_table} t
USING charglt_credit_results s
ON t.request_id = s.request_id
WHEN MATCHED THEN UPDATE SET
  status_byte = s.status_byte,
  status_string = s.status_string,
  applied_at = CAST(s.applied_at AS TIMESTAMP)
WHEN NOT MATCHED THEN INSERT (request_id, status_byte, status_string, applied_at)
  VALUES (s.request_id, s.status_byte, s.status_string, CAST(s.applied_at AS TIMESTAMP))
"""

started = time.monotonic()
applied = 0
with psycopg.connect(host=pg_host, dbname=arg("database"), user=pg_user,
                     password=pg_password, sslmode="require", autocommit=True) as conn:
    while time.monotonic() - started < max_seconds:
        pending = spark.sql(f"""
            SELECT a.request_id, a.userid, a.add_credit_value FROM {source} a
            LEFT ANTI JOIN {results_table} r ON r.request_id = a.request_id
            ORDER BY a.request_id LIMIT {batch_size}
        """).collect()
        if not pending:
            break

        # One Postgres transaction per batch.
        with conn.transaction():
            results = conn.execute(
                "SELECT * FROM ApplyApprovedAddCredits(%s, %s, %s)",
                ([r.request_id for r in pending],
                 [r.userid for r in pending],
                 [r.add_credit_value for r in pending])).fetchall()

        spark.createDataFrame(results, result_schema).createOrReplaceTempView("charglt_credit_results")
        spark.sql(merge)

        applied += len(pending)
        failed = sum(1 for r in results if r[1] != 56)
        print(f"{applied:,} requests processed ({failed} in this batch not added)")
        if len(pending) < batch_size:
            break

print(f"Done: {applied:,} requests in {time.monotonic() - started:.0f}s")
dbutils.notebook.exit(str(applied))
