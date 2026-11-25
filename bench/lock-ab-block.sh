#!/bin/bash
# Runs one block of the row-lock A/B test: one repetition at one user count, with each lock
# variant in turn. Normally called by lock-ab.sh.
#
#   lock-ab-block.sh <rep> <usercount> <seconds> <workers>
#   lock-ab-block.sh restore         put back the functions in create_procs.sql and exit
#
# Variants:
#   wait      SELECT ... FOR UPDATE
#   nowait    SELECT ... FOR UPDATE NOWAIT (as in create_procs.sql)
#   timeout   SELECT ... FOR UPDATE, with lock_timeout set by the client
#
# The variants replace UpsertUser, ReportQuotaUsage and AddCredit in the database being
# tested. The functions in create_procs.sql are put back when the script ends.
#
# Environment:
#   PG_* and PGHOST etc.   connection for the Java client and for psql
#   AB_CP                  Java classpath
#   AB_PROCS               create_procs.sql (default: ../ddl beside this script)
#   AB_OUT                 directory for logs (default ./lock-ab)
#   AB_VARIANTS            default "wait nowait timeout"
#   AB_LOCK_TIMEOUT_MS     for the timeout variant (default 300)
#   AB_TIMEOUT_RETRIES     client retries for the timeout variant (default 2)
#   AB_MAX_DB_MB           don't start a run if the database is bigger than this (default 380)

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
procs=${AB_PROCS:-$here/../ddl/create_procs.sql}
out=${AB_OUT:-./lock-ab}
read -r -a variants <<< "${AB_VARIANTS:-wait nowait timeout}"
lock_timeout=${AB_LOCK_TIMEOUT_MS:-300}
timeout_retries=${AB_TIMEOUT_RETRIES:-2}
max_db_mb=${AB_MAX_DB_MB:-380}
# -X: ignore ~/.psqlrc
sql() { psql -X -d "${PG_DATABASE:-charglt}" -v ON_ERROR_STOP=1 -At "$@"; }
die() { echo "lock-ab-block.sh: $*" >&2; exit 1; }

# The three functions that lock the user's row, exactly as in create_procs.sql.
locking_functions() {
    awk '/^CREATE OR REPLACE FUNCTION (UpsertUser|ReportQuotaUsage|AddCredit)\(/ {p=1}
         p {print}
         /^\$\$ LANGUAGE plpgsql;/ {p=0}' "$procs"
}

# Install a variant in one transaction, then check that the database has it.
install_variant() {
    local variant=$1 body expect n
    body=$(locking_functions)
    n=$(grep -c 'FOR UPDATE NOWAIT;' <<< "$body" || true)
    [ "$n" = 3 ] || die "expected 3 FOR UPDATE NOWAIT in $procs, found $n"
    expect=3
    if [ "$variant" != nowait ]; then
        body=${body//FOR UPDATE NOWAIT;/FOR UPDATE;}
        expect=0
    fi
    sql -q -1 <<< "$body"
    n=$(sql -c "SELECT count(*) FROM pg_proc WHERE proname IN ('upsertuser','reportquotausage','addcredit') AND pronamespace = 'public'::regnamespace AND prosrc LIKE '%FOR UPDATE NOWAIT;%'")
    [ "$n" = "$expect" ] || die "$variant: expected $expect NOWAIT functions in the database, found $n"
}

sampler=
java_pid=
finish() {
    local rc=$?
    trap - EXIT
    trap '' INT TERM HUP PIPE
    [ -z "$sampler" ] || kill "$sampler" 2>/dev/null || true
    [ -z "$java_pid" ] || kill "$java_pid" 2>/dev/null || true
    if install_variant nowait; then
        echo "$(date +%T) restored NOWAIT functions" || true
    else
        echo "lock-ab-block.sh: FAILED TO RESTORE: run psql -f ddl/create_procs.sql" >&2
        rc=1
    fi
    exit $rc
}

if [ "${1:-}" = restore ]; then
    trap finish EXIT
    exit 0
fi

[ $# = 4 ] || die "usage: lock-ab-block.sh <rep> <usercount> <seconds> <workers>"
rep=$1 users=$2 secs=$3 workers=$4
cp=${AB_CP:?set AB_CP to the Java classpath}
mkdir -p "$out"
have=$(sql -c "SELECT count(*) FROM user_table WHERE userid >= 0 AND userid < $users")
[ "$have" = "$users" ] || die "only $have of users 0..$((users - 1)) are loaded"

trap finish EXIT
trap 'exit 130' INT TERM HUP PIPE

count=${#variants[@]}
for ((i = 0; i < count; i++)); do
    # Rotate, so that over <count> repetitions each variant runs once in each position.
    variant=${variants[$(((i + rep - 1) % count))]}
    name=$variant-$users-users-rep$rep

    mb=$(sql -c "SELECT pg_database_size(current_database()) / 1048576")
    [ "$mb" -lt "$max_db_mb" ] || die "database is $mb MB (limit $max_db_mb): export charglt_events first"

    install_variant "$variant"

    # Start every run with the dead rows of the last one cleared away.
    sql -q -c "VACUUM (ANALYZE) user_table" -c "VACUUM (ANALYZE) user_usage_table" \
        -c "VACUUM (ANALYZE) user_recent_transactions"

    unset PG_LOCK_TIMEOUT_MS CHARGLT_MAX_LOCK_RETRIES
    if [ "$variant" = timeout ]; then
        export PG_LOCK_TIMEOUT_MS=$lock_timeout CHARGLT_MAX_LOCK_RETRIES=$timeout_retries
    fi

    echo "$(date +%T) $name start (position $((i + 1)), database $mb MB)"
    {
        echo "AB variant=$variant users=$users rep=$rep position=$((i + 1)) seconds=$secs workers=$workers" \
             "lock_timeout_ms=${PG_LOCK_TIMEOUT_MS:-0} max_retries=${CHARGLT_MAX_LOCK_RETRIES:-default} db_mb=$mb"
    } > "$out/$name.log"

    # User and password come from PG_USER and PG_PASSWORD, so they aren't in the log.
    # tpms is 4: the runs in results/2026-11-24-lock-ab used 2, which then meant 4 calls per ms
    CHARGLT_SAME_USER_CONCURRENCY=true java -cp "$cp" ie.rolfe.postgres.chargingdemo.ChargingDemoTransactions \
        "$PG_HOST" "$users" 4 "$secs" 10 env env "$workers" >> "$out/$name.log" 2>&1 &
    java_pid=$!

    # Sample sessions once a second: waiting on any lock, transactionid, tuple, active, and
    # the count for each kind of lock.
    ( while kill -0 "$java_pid" 2>/dev/null; do
          sql -c "WITH s AS (SELECT * FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid())
                  SELECT clock_timestamp()::time(0), count(*) FILTER (WHERE wait_event_type = 'Lock'),
                         count(*) FILTER (WHERE wait_event = 'transactionid'),
                         count(*) FILTER (WHERE wait_event = 'tuple'),
                         count(*) FILTER (WHERE state = 'active'),
                         (SELECT string_agg(wait_event || '=' || n, ' ') FROM
                            (SELECT wait_event, count(*) AS n FROM s WHERE wait_event_type = 'Lock' GROUP BY 1) e)
                  FROM s" 2>/dev/null || true
          sleep 1
      done ) > "$out/$name.locks" &
    sampler=$!

    # A worker that hits an unexpected error dies and can leave the client blocked for ever.
    deadline=$((SECONDS + secs + 180))
    while kill -0 "$java_pid" 2>/dev/null; do
        if [ "$SECONDS" -gt "$deadline" ]; then
            echo "lock-ab-block.sh: $name overran, killing it" >&2
            kill -9 "$java_pid" 2>/dev/null || true
        fi
        sleep 1
    done

    # Exit 1 is also how the client says UNABLE_TO_MEET_REQUESTED_TPS, so isn't a failure.
    rc=0
    wait "$java_pid" || rc=$?
    java_pid=
    wait "$sampler" 2>/dev/null || true
    sampler=
    echo "exit=$rc" >> "$out/$name.log"
    grep -q 'TPS = ' "$out/$name.log" || die "$name printed no TPS; see $out/$name.log"
    echo "$(date +%T) $name done:$(grep -m1 'TPS = ' "$out/$name.log" | sed 's/.*TPS =/ TPS/')"
    sleep "${AB_PAUSE_SECONDS:-5}"
done
