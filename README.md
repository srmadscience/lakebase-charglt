# postgres-charglt
PostgreSQL implementation of [Charglt](https://github.com/srmadscience/voltdb-charglt)

It has the same basic functionality as charglt, adapted for PostgreSQL — see TODO for known gaps.

Far more documentation about the benchmark itself is available at the link above.

## See Also:

* [aerospike-charglt](https://github.com/srmadscience/aerospike-charglt)
* [mongodb-charglt](https://github.com/srmadscience/mongodb-charglt)
* [redis-charglt](https://github.com/srmadscience/redis-charglt)
* [voltdb-charglt](https://github.com/srmadscience/voltdb-charglt)
* [oracle-charglt](https://github.com/srmadscience/oracle-charglt)

## Prerequisites

* PostgreSQL 12 or later (generated column support required)
* Java 21
* Maven

## Installation

1. Create the database:
```sql
CREATE DATABASE charglt;
```

2. Connect to the `charglt` database and run the schema DDL:
```
psql -d charglt -f ddl/create_db.sql
```

3. Run the stored functions DDL:
```
psql -d charglt -f ddl/create_procs.sql
```


## Configuration

Connection details are set via environment variables:

| Variable | Default (fallback) | Description |
|---|---|---|
| `PG_HOST` | first CLI argument | PostgreSQL host (single host or comma-separated list for failover) |
| `PG_USER` | CLI argument | Database user |
| `PG_PASSWORD` | CLI argument | Database password |
| `PG_PORT` | `5432` | Port |
| `PG_DATABASE` | `charglt` | Database name |
| `PG_SSLMODE` | driver default | JDBC `sslmode`, e.g. `require` |

## Databricks Lakebase

The benchmark runs unchanged on a Databricks Lakebase Autoscaling project. `lakebase-env.sh` looks up the endpoint's
host, generates a one-hour OAuth token to use as the password, and exports the `PG_*` variables (and `PGHOST` etc.
for `psql`):

```
. ./lakebase-env.sh path/to/databricks.properties   # needs databricks.host and databricks.token
psql -d databricks_postgres -c 'CREATE DATABASE charglt'
psql -d charglt -f ddl/create_db.sql
psql -d charglt -f ddl/create_procs.sql
```

`LAKEBASE_PROJECT` (default `charglt`), `LAKEBASE_BRANCH` (default `production`) and `LAKEBASE_ENDPOINT`
(default `primary`) select the endpoint. The hostname, username and password CLI arguments are then ignored.
Re-source the script before each run: new logins need an unexpired token.

### Exporting transactions to Delta

Every successful `UpsertUser`, `ReportQuotaUsage` and `AddCredit` calls `SendToKafka` (named after the VoltDB
original's Kafka export), which copies the new `user_recent_transactions` row into the `charglt_events` table.
A serverless Databricks job, `charglt-export`, moves those rows into the Delta table
`workspace.charglt.transactions` every 5 minutes. It locks a batch of up to 50,000 rows in Lakebase, merges it into
Delta, then deletes it from Lakebase in the same Postgres transaction, so a failed run is simply redone by the next
one. The job logs in to Lakebase the same way `lakebase-env.sh` does, as the job owner.

```
./databricks-sql.sh -f databricks/create_tables.sql   # create workspace.charglt.transactions
./databricks-job.sh --run            # upload databricks/charglt_export.py, create the job, run it once
./databricks-job.sh --schedule on    # export every 5 minutes (--schedule off to pause)
./databricks-sql.sh -f databricks/charglt_views.sql
./databricks-sql.sh 'SELECT * FROM export_lag'
```

The schedule starts paused: on Free Edition each run counts against the daily compute quota, so turn it on for a
benchmark run and off afterwards. `databricks-sql.sh` runs SQL on the Serverless Starter Warehouse
(`8682c92f3ae0c2a8`; set `DATABRICKS_WAREHOUSE_ID` to use another). To discard a backlog without exporting it, run
`SELECT PurgeChargltEvents(INTERVAL '1 hour')`.

Every call is a network round trip, so when running far from the endpoint use plenty of loader threads and
benchmark workers.

### Example results

> **Disclaimer:** these numbers come from a smoke test, run only to show that the benchmark works on Lakebase.
> They are not a performance benchmark, and should not be quoted or used to compare Lakebase with other databases.

These runs used a Free Edition workspace (Postgres 17, 1 CU, AWS us-east-2), with the client on a Mac about
130 ms away (round-trip time). They show the benchmark working, not what Lakebase can do: throughput is limited by
round trips, and grows almost in step with the number of workers.

Loading 10,000 users (`CreateChargingDemoData`):

| Threads | Users per second |
|---|---|
| 1 | ~4 |
| 16 | 58 |

`ChargingDemoTransactions` with 10,000 users, 60 seconds, and 2 tpms requested (so it reports
`UNABLE_TO_MEET_REQUESTED_TPS`):

| Workers | Transactions | TPS | ReportQuotaUsage median / p99 (ms) | AddCredit median / p99 (ms) |
|---|---|---|---|---|
| 16 | 6,018 | 99 | 588 / 1,703 | 571 / 1,691 |
| 100 | 34,698 | 569 | 428 / 1,683 | 417 / 1,658 |
| 150 | 50,998 | 836 | 421 / 1,634 | 404 / 1,614 |
| 200 | 71,544 | 1,173 | 445 / 1,616 | 443 / 1,674 |

Latencies are measured at the client and include time waiting in the client's queue (a median of 443 ms with
16 workers and 279 ms with 100).

Throughput stays at about 5.7 TPS per worker from 100 to 200 workers, and median latency does not rise, so the
1 CU endpoint is not saturated: the round trip is the limit. The endpoint allows 225 connections (7 reserved for
superusers), so about 200 workers is as far as this client can go. Finding the endpoint's real limit would need a
client in the same region.

`ChargingDemoTransactions` with 200 workers for 30 minutes:

| Workers | Duration | Transactions | TPS | ReportQuotaUsage median / p99 (ms) | AddCredit median / p99 (ms) |
|---|---|---|---|---|---|
| 200 | 30 min | 2,063,974 | 1,146 | 422 / 1,574 | 420 / 1,574 |

Averaged over 30 minutes, throughput and latency are close to the 60-second run with 200 workers.

#### With the export job running

`ChargingDemoTransactions` with 10,000 users, 60 seconds, 2 tpms and 16 workers, after `SendToKafka` started writing
to `charglt_events`, with the `charglt-export` job on its 5-minute schedule (same workspace and client as above):

| Workers | Transactions | TPS | ReportQuotaUsage median / p99 (ms) | AddCredit median / p99 (ms) |
|---|---|---|---|---|
| 16 | 6,213 | 102 | 693 / 1,702 | 786 / 1,711 |

Throughput matches the earlier 16-worker run (99 TPS). Medians are higher, but so is the time spent waiting in the
client's queue (554 ms against 443 ms), and there were only 41 AddCredit calls, so a 60-second run can't separate the
cost of the extra insert from run-to-run variation.

The export kept up. A scheduled run fired during the benchmark and exported 2,830 rows while transactions were
still arriving; the next exported the remaining 3,329 in 26 seconds. `workspace.charglt.transactions` ended with
6,159 rows (6,118 Spend, 41 Add Credit), the same as the benchmark's ReportQuotaUsage and AddCredit counts, with no
duplicates and nothing left in `charglt_events`.

### For comparison: a local PostgreSQL server

> **Disclaimer:** also a smoke test, not a benchmark. The gap below is almost all network distance (1.6 ms
> versus 130 ms round trip), not a measure of Lakebase against PostgreSQL.

The same runs against PostgreSQL 18.6 on an untuned Ubuntu server on the client's LAN (about 1.6 ms round
trip, default `shared_buffers` of 128 MB, `max_connections` of 100):

| Threads | Users per second (10,000 users) |
|---|---|
| 1 | 1,604 |
| 16 | 11,210 |

| Workers | Transactions | TPS | ReportQuotaUsage median / p99 (ms) | AddCredit median / p99 (ms) |
|---|---|---|---|---|
| 16 | 106,095 | 1,768 | 29 / 94 | 30 / 94 |
| 64 | 238,744 | 3,979 | 8 / 56 | 8 / 60 |

100 workers is not possible there: with 3 of the 100 connections reserved for superusers, the run cannot open
enough connections.

## Running the benchmark

### Load data — CreateChargingDemoData

```
hostname usercount tpms maxinitialcredit username password [threadcount]
```

Example — load 100,000 users at up to 10 transactions per millisecond, each starting with up to 1,000 units of credit:
```
10.13.1.101 100000 10 1000 postgres mypassword
```

`threadcount` (default 1) splits the load across that many threads, each with its own connection.

### Transaction benchmark — ChargingDemoTransactions

```
hostname usercount tpms durationseconds queryseconds username password workercount
```

Example — run against 100,000 users at 10 tpms for 120 seconds, printing global stats every 30 seconds, using a single thread:
```
10.13.1.101 100000 10 120 30 postgres mypassword 0
```

### Key-Value store benchmark — ChargingDemoKVStore

```
hostname usercount tpms durationseconds queryseconds jsonsize deltaproportion username password workercount
```

Example — 100 users, 1 tpms, 60 seconds, stats every 15 seconds, 100-byte JSON payloads, 50% delta updates:
```
10.13.1.101 100 1 60 15 100 50 postgres mypassword 0
```

### Delete data — DeleteChargingDemoData

```
hostname usercount tpms username password
```

## Schema notes

* `user_table.user_json_object` is stored as `JSONB`.
* `user_json_cardid` is a generated column (`BIGINT`) computed from `user_json_object->>'loyaltySchemeNumber'`, automatically indexed.
* All stored procedures are implemented as PL/pgSQL **functions** and are called with `SELECT * FROM FunctionName(...)`.
* `SendToKafka` writes each transaction to `charglt_events` rather than Kafka; see *Exporting transactions to Delta* above.

## Removing the schema

```
psql -d charglt -f ddl/remove_db.sql
```

## TODO

* Implement `ShowCurrentAllocations` (running totals), present in the VoltDB original.
* Scaled multi-worker tests have not yet been validated end-to-end.
