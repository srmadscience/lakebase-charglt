#!/bin/bash
# Runs SQL on a Databricks SQL warehouse through the Statement Execution API.
#
#   ./databricks-sql.sh 'SELECT COUNT(*) FROM charglt_transactions'
#   ./databricks-sql.sh -f ddl/databricks/charglt_events.sql
#
# A file may hold several statements, each ending with ';' at the end of a line.
#
# Reads databricks.host and databricks.token from DATABRICKS_PROPERTIES (default
# ../databricks-faadata-demo/databricks.properties; DATABRICKS_HOST and DATABRICKS_TOKEN
# override). DATABRICKS_WAREHOUSE_ID defaults to the Serverless Starter Warehouse.
# CHARGLT_CATALOG and CHARGLT_SCHEMA (required) are the catalog and schema that
# Lakebase Change Data Feed writes to; unqualified names resolve there.

set -euo pipefail

props=${DATABRICKS_PROPERTIES:-../databricks-faadata-demo/databricks.properties}
prop() { grep "^$1=" "$props" 2>/dev/null | cut -d= -f2-; }

host=${DATABRICKS_HOST:-$(prop databricks.host)}
token=${DATABRICKS_TOKEN:-$(prop databricks.token)}
warehouse=${DATABRICKS_WAREHOUSE_ID:-8682c92f3ae0c2a8}
: "${CHARGLT_CATALOG:?set CHARGLT_CATALOG to the CDF destination catalog}"
: "${CHARGLT_SCHEMA:?set CHARGLT_SCHEMA to the CDF destination schema}"

if [ -z "$host" ] || [ -z "$token" ]; then
    echo "databricks-sql.sh: no databricks.host/databricks.token in $props" >&2
    exit 1
fi

if [ "${1:-}" = "-f" ]; then
    sql=$(cat "$2")
else
    sql=${1:?usage: databricks-sql.sh 'SQL' | -f file.sql}
fi

export DBX_HOST=$host DBX_TOKEN=$token DBX_WAREHOUSE=$warehouse DBX_SQL=$sql

python3 - <<'EOF'
import json, os, re, sys, time, urllib.request

host, token = os.environ["DBX_HOST"], os.environ["DBX_TOKEN"]

def call(method, path, body=None):
    req = urllib.request.Request(
        f"https://{host}/api/2.0/sql/statements{path}", method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"HTTP {e.code}: {e.read().decode()}")

# Drop '--' comment lines, then split on ';' at end of line.
text = "\n".join(l for l in os.environ["DBX_SQL"].splitlines() if not l.lstrip().startswith("--"))
statements = [s.strip() for s in re.split(r";\s*$", text, flags=re.M) if s.strip()]

for stmt in statements:
    lines = stmt.splitlines()
    print("> " + lines[0] + (" ..." if len(lines) > 1 else ""))
    res = call("POST", "", {
        "warehouse_id": os.environ["DBX_WAREHOUSE"],
        "catalog": os.environ["CHARGLT_CATALOG"],
        "schema": os.environ["CHARGLT_SCHEMA"],
        "statement": stmt,
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
    })
    # A serverless warehouse may need to start, so keep polling.
    while res["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(2)
        res = call("GET", f"/{res['statement_id']}")
    state = res["status"]["state"]
    if state != "SUCCEEDED":
        sys.exit(f"{state}: {res['status'].get('error', {}).get('message', '')}")
    cols = [c["name"] for c in res.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = res.get("result", {}).get("data_array", []) or []
    if cols:
        print("\t".join(cols))
        for row in rows:
            print("\t".join("" if v is None else str(v) for v in row))
        if res["manifest"].get("truncated") or res["manifest"].get("total_chunk_count", 1) > 1:
            print("(more rows not shown)")
EOF
