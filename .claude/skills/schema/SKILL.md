---
name: schema
description: Change or verify the demo Cassandra schema, which is owned by the sink rather than by a migration. Use when altering a table's key or columns, when a query returns nothing after a change, when checking the partitioning of demo.events, or when snapshots have accumulated.
user-invocable: true
allowed-tools:
  - Bash
  - Read
  - Edit
---

# Changing the schema

There is no migration tool.  `ingress/consumer/consumer.py:ensure_schema()` creates the keyspace and every table with `CREATE TABLE IF NOT EXISTS`, and runs on each sink start.  Two consequences decide everything below: the schema lives in the sink's image, and `IF NOT EXISTS` will not alter a table that already exists.  Rebuilding the sink after editing a key changes nothing, silently.

The keyspace is `demo`; the tables are `events`, `drone_latest_status`, `drone_text_embeddings`, `drone_events_by_entity`, `restricted_zones`, `alerts_by_bucket`, `ingestion_counts`, `sessions_open`, `session_seq_applied`, `session_timeline`.

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
