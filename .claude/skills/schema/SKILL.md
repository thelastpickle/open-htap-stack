---
name: schema
description: Change or verify the demo Cassandra schema, which is owned by the sink rather than by a migration. Use when altering a table's key or columns, when a query returns nothing after a change, when checking the partitioning of demo.events, when a transaction is refused on a session table, or when snapshots have accumulated.
user-invocable: true
allowed-tools:
  - Bash
  - Read
  - Edit
---

# Changing the schema

There is no migration tool.  `ingress/consumer/consumer.py:ensure_schema()` creates the keyspace and every table with `CREATE TABLE IF NOT EXISTS`, and runs on each sink start.  Two consequences decide everything below: the schema lives in the sink's image, and `IF NOT EXISTS` will not alter a table that already exists.  Rebuilding the sink after editing a key changes nothing, silently.

The keyspace is `demo`; the tables are `events`, `drone_latest_status`, `drone_text_embeddings`, `drone_events_by_entity`, `restricted_zones`, `alerts_by_bucket`, `ingestion_counts`, `sessions_open`, `session_seq_applied`, `session_timeline`, `session_timeline_plain`.

## Applying a change to an existing table

```bash
podman compose -f podman-compose.yml stop data-cassandra-sink
podman compose -f podman-compose.yml build data-cassandra-sink
podman exec cassandra cqlsh cassandra -e "DROP TABLE demo.events;"
podman compose -f podman-compose.yml up -d --no-deps data-cassandra-sink
podman exec cassandra cqlsh cassandra -e "DESCRIBE TABLE demo.events;"     # confirm the new key
podman exec cassandra nodetool clearsnapshot --all                          # the drop leaves an auto-snapshot
```

Stop the sink first, or it recreates the table from the old image while you are dropping it.  Confirm with `DESCRIBE`; a wrong key looks exactly like a working stack until a query returns nothing.  `DROP TABLE` leaves an auto-snapshot holding the old SSTables' disk, which `clearsnapshot --all` releases (`Requested clearing snapshot(s) for [all keyspaces] with [all snapshots]`).

For a wider change, `./stop-and-clean-data-and-schema.sh` is cleaner than dropping tables one at a time.

## `transactional_mode` is the one option a drop cannot be avoided for

Six tables are created `WITH transactional_mode='full'` — the three session tables and the three clearance ones — and the option can only be set at `CREATE TABLE`.  `ALTER TABLE ... WITH transactional_mode='full'` looks as though it works: it succeeds, and it leaves the table refusing every transaction with "Transaction Statement is unsupported when migrating away from Accord or before migration to Accord is complete for a range", because the `ALTER` sets `transactional_migration_from = 'off'` and starts a migration.

Neither way of finishing that migration exists on this stack.  `nodetool repair` declines, correctly: `Replication factor is 1. No repair is needed for keyspace 'demo'`.  `nodetool consensus_admin finish-migration` fails inside its own first round of repairs with `java.io.NotSerializableException: java.util.ArrayList$SubList`, which is a bug in the JMX call rather than anything about the schema.  So drop the table and let the sink recreate it, and check `nodetool consensus_admin list` shows `tableStates: []` afterwards.

Two further traps around that option:

- **The node refuses the `CREATE TABLE` when Accord is off**: `Cannot create table demo.x with transactional mode full with accord.enabled set to false`.  That would stop the sink at its schema step and with it the whole demo, which is why `consumer.py` writes the option only when `CASSANDRA_ACCORD_ENABLED` is true, and why the sink and the cassandra service read that same one declaration in `podman-compose.yml`.
- **`DESCRIBE TABLE` reports `transactional_mode`; `system_schema.tables` does not.**  An earlier version of this note said neither did, and that was wrong: `DESCRIBE TABLE demo.session_timeline` prints `AND transactional_mode = 'full'` beside `AND transactional_migration_from = 'none'`, and `session_timeline_plain` prints `'off'` in the same place.  What is missing is the column: `SELECT transactional_mode FROM system_schema.tables` is refused with "Undefined column name transactional_mode", so a script has to read the `create_statement` or ask behaviourally.

  **Reading it is one statement, and it works through the driver.**  `DESCRIBE KEYSPACE demo` is served node-side and returns 16 rows for the whole keyspace — the keyspace itself, its 14 tables and its one index — each with a `create_statement` carrying the option; **six read `full` and eight read `off`**.  That is one round trip for every table, where `DESCRIBE TABLE` is one per table.  `/api/schema/cql` does exactly this and takes the key structure from `system_schema.columns` beside it.

```bash
# every table's mode, from one statement
curl -s http://localhost:8000/api/schema/cql | python3 -c '
import json, sys
for t in json.load(sys.stdin)["tables"]:
    print("%-24s %s" % (t["name"], t["transactional_mode"]))'

# or behaviourally, which is the node refusing rather than the node describing
curl -s http://localhost:8000/api/transactions/session/schema     # a read-only transaction per table
podman exec cassandra cqlsh 172.20.0.10 -e \
  "BEGIN TRANSACTION SELECT seq FROM demo.session_timeline LIMIT 1; COMMIT TRANSACTION;"
```

A table that has not opted in answers `Accord transactions are disabled on table (See transactional_mode in table options)`.  Note the address: `cqlsh` with no host reaches `127.0.0.1`, which this node does not listen on.

`full` also routes **ordinary** reads and writes to the table through Accord, so every statement against those six must be at QUORUM; the driver's default profile is LOCAL_ONE and is refused with `ConsistencyLevel LOCAL_ONE is unsupported with Accord`.  That is the reason `events` is not opted in.

## The SQL schema needs a CREATE TABLE before it can be read

`/api/schema/sql` reads `pg_class` for the tables and `pg_attribute` for their columns, and cassandra-sql creates the second **on first use**.  So a restarted `accord-sql` reports all five tables with no columns and a note per table, while `pg_class` answers normally.

```bash
curl -s http://localhost:8000/api/schema/sql | python3 -c '
import json, sys
for t in json.load(sys.stdin)["tables"]:
    print("%-14s %2d columns  %s" % (t["name"], len(t["columns"]), t.get("note") or ""))'

curl -s -X POST http://localhost:8000/api/sql-console/reset   # the CREATE that registers pg_attribute
```

Judge that reset by its `CREATE` and `INSERT` statements rather than by `error_count`: two `DROP TYPE` statements report the type missing on a restarted service, which is expected.

## A keyspace holding an Accord table cannot be dropped

`DROP KEYSPACE cassandra_sql` is refused with `Cannot drop keyspace 'cassandra_sql' as it contains accord tables. (cassandra_sql.kv_store, ...)`, naming each.  Drop the thirteen tables first, then the keyspace.  This bites on cassandra-sql's three keyspaces, `cassandra_sql`, `cassandra_sql_internal` and `pg_catalog`, whose tables it creates itself with `transactional_mode = 'full'`; the same rule applies to `demo` once the three session tables exist.

`./stop-and-clean-data-and-schema.sh` sidesteps the refusal entirely, because it deletes the data directory rather than issuing CQL, and cassandra-sql recreates its keyspaces on its next start.  Prefer it.  Cassandra also leaves the directory of a dropped keyspace behind under `cassandra-data/data/`, so an empty directory there is not evidence that the keyspace still exists; ask `DESCRIBE KEYSPACE`.

## The two constants both sides must agree on

`demo.events` is `PRIMARY KEY ((event_bucket, shard), event_id)`, where `event_bucket` is a 15-minute UTC window as text and `shard` is `crc32(event_id) % 16`.  Bucket width and shard count are declared once in `podman-compose.yml` and passed to **both** the sink and the backend as `EVENT_BUCKET_MINUTES` and `EVENT_SHARDS`.  Recreate both services after changing either; a backend querying 16 shards against a sink writing 8 matches nothing, and returns no error either.

`event_bucket` is text rather than a timestamp so that one literal parses identically in CQL, Presto SQL and Spark SQL.

## Verifying the partitioning

A hash that spreads badly is invisible in every query and fatal to the point of the model.  `uuid1().int % N` puts everything in one shard, because the low bits of a version-1 UUID are the constant node field; hash the bytes instead.  Check with per-shard counts on a closed window, which should be within a percent of each other:

```bash
for k in $(seq 0 15); do
  podman exec cassandra cqlsh cassandra -e \
    "SELECT count(*) FROM demo.events WHERE event_bucket='2026-08-18T17:15' AND shard=$k;"
done
```

One shard at a time.  A `GROUP BY event_bucket, shard` over the whole window needs `shard IN (0,…,15)` to be fully partition-restricted, or CQL demands `ALLOW FILTERING`, and even then it times out on a window still filling at ~2,000 events/s.

Right after a wipe is the cheap moment to check spread, while the table is small.

## Snapshots

```bash
podman exec cassandra nodetool listsnapshots
podman exec cassandra nodetool clearsnapshot --all
```

The bulk reader takes one per read with a TTL derived from `spark_query_timeout_s`, so they expire on their own; `listsnapshots` printing nothing is the healthy state.  An accumulation means reads are failing before their cleanup, not that the TTL is wrong.
