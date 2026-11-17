#!/bin/bash
# Deploys the charglt export job: uploads databricks/charglt_export.py as a notebook and
# creates (or updates) a serverless job named charglt-export that runs it every 5 minutes.
#
#   ./databricks-job.sh                 deploy; the schedule stays paused
#   ./databricks-job.sh --run           deploy and run once now, waiting for the result
#   ./databricks-job.sh --schedule on   deploy and unpause the 5-minute schedule
#   ./databricks-job.sh --schedule off  deploy and pause it again
#
# Reads databricks.host and databricks.token from DATABRICKS_PROPERTIES (default
# ../databricks-faadata-demo/databricks.properties; DATABRICKS_HOST and DATABRICKS_TOKEN
# override). CHARGLT_TARGET_TABLE (default workspace.charglt.transactions) and
# LAKEBASE_PROJECT / LAKEBASE_BRANCH / LAKEBASE_ENDPOINT are passed to the notebook.

set -euo pipefail
cd "$(dirname "$0")"

props=${DATABRICKS_PROPERTIES:-../databricks-faadata-demo/databricks.properties}
prop() { grep "^$1=" "$props" 2>/dev/null | cut -d= -f2-; }

export DBX_HOST=${DATABRICKS_HOST:-$(prop databricks.host)}
export DBX_TOKEN=${DATABRICKS_TOKEN:-$(prop databricks.token)}
if [ -z "$DBX_HOST" ] || [ -z "$DBX_TOKEN" ]; then
    echo "databricks-job.sh: no databricks.host/databricks.token in $props" >&2
    exit 1
fi

export JOB_RUN=no JOB_SCHEDULE=
while [ $# -gt 0 ]; do
    case $1 in
        --run) JOB_RUN=yes ;;
        --schedule) JOB_SCHEDULE=${2:?--schedule on|off}; shift ;;
        *) echo "usage: databricks-job.sh [--run] [--schedule on|off]" >&2; exit 1 ;;
    esac
    shift
done

python3 - <<'EOF'
import base64, json, os, sys, time, urllib.request

host, token = os.environ["DBX_HOST"], os.environ["DBX_TOKEN"]
JOB_NAME = "charglt-export"

def call(method, path, body=None):
    req = urllib.request.Request(
        f"https://{host}/api{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path}: HTTP {e.code}: {e.read().decode()}")

me = call("GET", "/2.0/preview/scim/v2/Me")["userName"]
folder = f"/Users/{me}/charglt"
notebook = f"{folder}/charglt_export"

call("POST", "/2.0/workspace/mkdirs", {"path": folder})
with open("databricks/charglt_export.py", "rb") as f:
    call("POST", "/2.0/workspace/import", {
        "path": notebook, "format": "SOURCE", "language": "PYTHON",
        "overwrite": True, "content": base64.b64encode(f.read()).decode()})
print(f"Notebook: {notebook}")

jobs = call("GET", f"/2.2/jobs/list?name={JOB_NAME}").get("jobs", [])
existing = jobs[0] if jobs else None

schedule = os.environ["JOB_SCHEDULE"]
if schedule:
    pause = {"on": "UNPAUSED", "off": "PAUSED"}[schedule]
elif existing:
    pause = existing["settings"].get("schedule", {}).get("pause_status", "PAUSED")
else:
    pause = "PAUSED"

settings = {
    "name": JOB_NAME,
    "max_concurrent_runs": 1,
    "timeout_seconds": 1800,
    # No cluster spec: notebook tasks run on serverless compute.
    "tasks": [{
        "task_key": "export",
        "notebook_task": {
            "notebook_path": notebook,
            "source": "WORKSPACE",
            "base_parameters": {
                "lakebase_project": os.environ.get("LAKEBASE_PROJECT", "charglt"),
                "lakebase_branch": os.environ.get("LAKEBASE_BRANCH", "production"),
                "lakebase_endpoint": os.environ.get("LAKEBASE_ENDPOINT", "primary"),
                "target_table": os.environ.get("CHARGLT_TARGET_TABLE", "workspace.charglt.transactions"),
            },
        },
    }],
    "schedule": {"quartz_cron_expression": "0 0/5 * * * ?", "timezone_id": "UTC", "pause_status": pause},
}

if existing:
    job_id = existing["job_id"]
    call("POST", "/2.2/jobs/reset", {"job_id": job_id, "new_settings": settings})
else:
    job_id = call("POST", "/2.2/jobs/create", settings)["job_id"]
print(f"Job: {JOB_NAME} ({job_id}), schedule every 5 minutes, {pause.lower()}")
print(f"     https://{host}/jobs/{job_id}")

if os.environ["JOB_RUN"] == "yes":
    run_id = call("POST", "/2.2/jobs/run-now", {"job_id": job_id})["run_id"]
    print(f"Run {run_id} started", end="", flush=True)
    while True:
        time.sleep(10)
        run = call("GET", f"/2.2/jobs/runs/get?run_id={run_id}")
        state = run["status"]["state"]
        if state == "TERMINATED":
            break
        print(".", end="", flush=True)
    print()
    details = run["status"].get("termination_details", {})
    print(f"{details.get('code')}: {details.get('message', '')}")
    task_run = run["tasks"][0]["run_id"]
    out = call("GET", f"/2.2/jobs/runs/get-output?run_id={task_run}")
    if "notebook_output" in out:
        print(f"Rows exported: {out['notebook_output'].get('result')}")
    if out.get("error"):
        print(out["error"])
        print(out.get("error_trace", ""))
        sys.exit(1)
EOF
