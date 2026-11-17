#!/bin/bash
# Deploys a charglt Databricks job: uploads its notebook from databricks/ and creates (or
# updates) a serverless job that runs it every 5 minutes.
#
#   export          charglt-export: databricks/charglt_export.py moves charglt_events
#                   from Lakebase into workspace.charglt.transactions (the default)
#   apply-credits   charglt-apply-credits: databricks/charglt_apply_credits.py applies
#                   workspace.charglt.approved_addcredits in Lakebase with AddCredit
#
#   ./databricks-job.sh [job]                 deploy; the schedule stays paused
#   ./databricks-job.sh [job] --run           deploy and run once now, waiting for the result
#   ./databricks-job.sh [job] --schedule on   deploy and unpause the 5-minute schedule
#   ./databricks-job.sh [job] --schedule off  deploy and pause it again
#
# Reads databricks.host and databricks.token from DATABRICKS_PROPERTIES (default
# ../databricks-faadata-demo/databricks.properties; DATABRICKS_HOST and DATABRICKS_TOKEN
# override). LAKEBASE_PROJECT / LAKEBASE_BRANCH / LAKEBASE_ENDPOINT are passed to the
# notebook, as is CHARGLT_TARGET_TABLE (export, default workspace.charglt.transactions) or
# CHARGLT_CREDITS_TABLE (apply-credits, default workspace.charglt.approved_addcredits).

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

export JOB_KEY=export JOB_RUN=no JOB_SCHEDULE=
while [ $# -gt 0 ]; do
    case $1 in
        export|apply-credits) JOB_KEY=$1 ;;
        --run) JOB_RUN=yes ;;
        --schedule) JOB_SCHEDULE=${2:?--schedule on|off}; shift ;;
        *) echo "usage: databricks-job.sh [export|apply-credits] [--run] [--schedule on|off]" >&2; exit 1 ;;
    esac
    shift
done

python3 - <<'EOF'
import base64, json, os, sys, time, urllib.request

host, token = os.environ["DBX_HOST"], os.environ["DBX_TOKEN"]
env = os.environ.get

lakebase = {
    "lakebase_project": env("LAKEBASE_PROJECT", "charglt"),
    "lakebase_branch": env("LAKEBASE_BRANCH", "production"),
    "lakebase_endpoint": env("LAKEBASE_ENDPOINT", "primary"),
}
JOBS = {
    "export": ("charglt-export", "charglt_export",
               {"target_table": env("CHARGLT_TARGET_TABLE", "workspace.charglt.transactions")}),
    "apply-credits": ("charglt-apply-credits", "charglt_apply_credits",
                      {"source_table": env("CHARGLT_CREDITS_TABLE", "workspace.charglt.approved_addcredits")}),
}
JOB_NAME, notebook_name, job_params = JOBS[os.environ["JOB_KEY"]]

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
notebook = f"{folder}/{notebook_name}"

call("POST", "/2.0/workspace/mkdirs", {"path": folder})
with open(f"databricks/{notebook_name}.py", "rb") as f:
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
        "task_key": os.environ["JOB_KEY"].replace("-", "_"),
        "notebook_task": {
            "notebook_path": notebook,
            "source": "WORKSPACE",
            "base_parameters": {**lakebase, **job_params},
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
        print(f"Rows processed: {out['notebook_output'].get('result')}")
    if out.get("error"):
        print(out["error"])
        print(out.get("error_trace", ""))
        sys.exit(1)
EOF
