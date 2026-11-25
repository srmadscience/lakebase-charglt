#!/bin/bash
# A/B/C test of how ReportQuotaUsage, AddCredit and UpsertUser lock the user's row: see
# lock-ab-block.sh for the variants.
#
#   bench/lock-ab.sh <output directory>
#
# Runs every variant at every user count, AB_REPS times. Within a repetition the variants run
# back to back, and their order changes from one repetition to the next. The functions in
# ddl/create_procs.sql are put back after every block. Don't run anything else that uses the
# database meanwhile: for a few seconds at a time it has other versions of the functions.
#
# Environment:
#   AB_USERS      default "10000 1000 100 10"
#   AB_REPS       default 3
#   AB_SECONDS    length of each run, default 120
#   AB_WORKERS    default 150
#   AB_HOST       run the client over ssh on this host (e.g. ec2-user@1.2.3.4), which needs
#                 Java 21 and psql. Default: run it here
#   AB_SSH_KEY    ssh key for AB_HOST
#   AB_LAKEBASE   yes: before each block get a new Lakebase token (they last an hour) with
#                 lakebase-env.sh. Default: use the PG_* and PGHOST etc. already set
#   AB_EXPORT     yes: after each block run the charglt-export job, to keep the database under
#                 the Free Edition limit of 512 MB. Its schedule should be paused
#   AB_LOCK_TIMEOUT_MS, AB_TIMEOUT_RETRIES, AB_VARIANTS, AB_MAX_DB_MB: see lock-ab-block.sh
#
# A block that already has all its results is skipped, so a test that stopped part of the way
# through can be resumed by running it again with the same directory.

set -euo pipefail

out=${1:?usage: lock-ab.sh <output directory>}
root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$out"
out=$(cd "$out" && pwd)
cd "$root"

users_list=${AB_USERS:-10000 1000 100 10}
reps=${AB_REPS:-3}
secs=${AB_SECONDS:-120}
workers=${AB_WORKERS:-150}
variants=${AB_VARIANTS:-wait nowait timeout}
host=${AB_HOST:-}

ssh_opts=(-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o StrictHostKeyChecking=accept-new)
[ -z "${AB_SSH_KEY:-}" ] || ssh_opts+=(-i "$AB_SSH_KEY")

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

mvn -q -o compile >"$work/mvn.log" 2>&1 || { cat "$work/mvn.log" >&2; exit 1; }
mvn -q -o dependency:build-classpath -Dmdep.outputFile="$work/cp.txt" >"$work/mvn.log" 2>&1 || { cat "$work/mvn.log" >&2; exit 1; }

if [ -n "$host" ]; then
    mkdir -p "$work/deploy/lib"
    cp -R target/classes "$work/deploy/"
    cp bench/lock-ab-block.sh ddl/create_procs.sql "$work/deploy/"
    tr ':' '\n' < "$work/cp.txt" | while read -r jar; do cp "$jar" "$work/deploy/lib/"; done
    COPYFILE_DISABLE=1 tar -C "$work/deploy" -czf - . |
        ssh "${ssh_opts[@]}" "$host" 'rm -rf lock-ab-client && mkdir lock-ab-client && tar -C lock-ab-client -xzf - 2>/dev/null'
fi

# Settings for the block script, as shell assignments.
settings() {
    local v
    for v in PG_HOST PG_USER PG_PASSWORD PG_PORT PG_DATABASE PG_SSLMODE \
             PGHOST PGUSER PGPASSWORD PGPORT PGSSLMODE \
             AB_LOCK_TIMEOUT_MS AB_TIMEOUT_RETRIES AB_VARIANTS AB_MAX_DB_MB AB_PAUSE_SECONDS; do
        [ -z "${!v:-}" ] || printf 'export %s=%q\n' "$v" "${!v}"
    done
}

run_block() {
    if [ "${AB_LAKEBASE:-no}" = yes ]; then
        set +eu
        # With no argument it would see this function's arguments as its own
        . ./lakebase-env.sh "${DATABRICKS_PROPERTIES:-../databricks-faadata-demo/databricks.properties}" > /dev/null
        set -eu
    fi
    [ -n "${PG_HOST:-}" ] && [ -n "${PG_PASSWORD:-}" ] || { echo "PG_HOST and PG_PASSWORD aren't set" >&2; return 1; }

    if [ -z "$host" ]; then
        AB_CP="target/classes:$(cat "$work/cp.txt")" AB_OUT=$out bench/lock-ab-block.sh "$@"
        return
    fi

    # The password goes over ssh's stdin, not its command line.
    settings | ssh "${ssh_opts[@]}" "$host" 'umask 077; cat > lock-ab-client/pg.env'
    local rc=0
    ssh "${ssh_opts[@]}" "$host" "cd lock-ab-client && . ./pg.env && rm -f pg.env &&
        AB_PROCS=create_procs.sql AB_CP='classes:lib/*' AB_OUT=out bash lock-ab-block.sh $*" || rc=$?
    ssh "${ssh_opts[@]}" "$host" 'cd lock-ab-client && mkdir -p out && tar -C out -czf - .' | tar -C "$out" -xzf -
    return $rc
}

block_done() {
    local v
    for v in $variants; do
        grep -qs '^exit=' "$out/$v-$2-users-rep$1.log" && grep -qs 'TPS = ' "$out/$v-$2-users-rep$1.log" || return 1
    done
}

for ((rep = 1; rep <= reps; rep++)); do
    for users in $users_list; do
        if block_done "$rep" "$users"; then
            echo "rep $rep, $users users: already done"
            continue
        fi
        echo "== rep $rep of $reps, $users users"
        if ! run_block "$rep" "$users" "$secs" "$workers"; then
            echo "lock-ab.sh: block failed; trying to restore the functions" >&2
            run_block restore || true
            exit 1
        fi
        if [ "${AB_EXPORT:-no}" = yes ]; then
            ./databricks-job.sh export --run > "$work/export.log" 2>&1 || { cat "$work/export.log" >&2; exit 1; }
        fi
    done
done

python3 bench/lock-ab-report.py "$out"
