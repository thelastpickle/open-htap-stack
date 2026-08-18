# Data model

The schema is defined in one place, `ensure_schema()` in [ingress/consumer/consumer.py](../ingress/consumer/consumer.py). &emsp;That process is the only one that has to exist for data to flow, so putting the definitions there means there is no separate migration step and no second copy to drift out of step.

| Table                     | Shape                                                   | Read by                                      |
| ------------------------- | ------------------------------------------------------- | -------------------------------------------- |
| `events`                  | One row per event, partitioned by a 15-minute bucket and a shard | Presto, the Spark bulk reader        |
| `drone_latest_status`     | One row per asset, the current state                     | The dashboard's map and indicators; all three engines |
| `drone_events_by_entity`  | Per-asset history, clustered by `event_time DESC`        | Flight paths, per-asset analysis             |
| `drone_text_embeddings`   | One row per asset: its text snippet and a 1536-float vector | Vector search, through a Storage-Attached Index (SAI) |
| `alerts_by_bucket`        | Alerts, partitioned by hour                              | The dashboard's alert feed                   |
| `ingestion_counts`        | Counter per 30-minute bucket                             | The ingestion volume chart                   |
| `restricted_zones`        | Zone polygons as Well-Known Text; reference data          | The map, and the sink's proximity checks     |
| `sessions_open`, `session_seq_applied`, `session_timeline` | Supporting tables for the Accord transaction demo | See the `mck/cassandra-6` branch |

## Why `events` is partitioned by time

```
PRIMARY KEY ((event_bucket, shard), event_id)
    event_bucket text   -- the 15-minute window, "2026-08-18T07:15" in UTC
    shard       int     -- 0..15, from a hash of the event's id
```

Keyed on the event alone, a question about a period of time had nothing to prune on: `event_time` is not part of the key, and a partition token is a hash, so "the last fifteen minutes" meant reading every row on every path. &emsp;With the window in the partition key it is a question about particular partitions, and each path exploits that in its own way:

| Path | What the bucket buys it |
| --- | --- |
| **Cassandra** | Reads only those partitions, and needs no `ALLOW FILTERING` |
| **Presto** | Pushes the partition-key predicate into its Cassandra connector |
| **Spark SQL, connector** | Same pushdown, so the scan is issued per partition |
| **Spark bulk reader** | Turns it into a `PartitionKeyFilter`, and skips SSTables whose token range cannot hold those keys |

A query for one window therefore names the window and every shard:

```sql
SELECT event_type, count(*) FROM events
WHERE event_bucket = '2026-08-18T07:15' AND shard IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)
GROUP BY event_type
```

Measured on a seven-core laptop with the ingest running: the same count against a window holding roughly half a million rows, and against a window that holds none.

| Path | Window with data | Empty window |
| --- | --- | --- |
| Cassandra | 1,955 ms | 17 ms |
| Presto | 2,065 ms | 194 ms |
| Spark SQL, connector | 5,078 ms | 176 ms |
| Spark bulk reader | 8,788 ms | 925 ms |

The empty-window column is the pruning, and for the bulk reader it is file skipping rather than a scan that finds nothing: Spark reported 529,103 records read for the populated window and **0** for the empty one, across the same seventeen tasks, 16.3 s of executor time against 354 ms.

What the bucket does not buy is the Cassandra Query Language (CQL) as an analytical engine. `count(*)` over a *full* window is about 1.8M rows at the default rate, and it times out at the coordinator: the window is addressable, not cheap. &emsp;That is the same division of labour the dashboard's comparison draws, only now the transactional path can express the question at all.

The shard exists to keep those partitions a sane size. &emsp;One 15-minute window at the demo's default 2,000 events/s is about 1.8M rows, far too much for a single partition; over sixteen shards it is nearer 110k rows, a few tens of megabytes. &emsp;It comes from a hash of the event's id rather than from the asset, so the spread does not collapse when the fleet is small. &emsp;It is a hash rather than the id taken modulo sixteen because these are version-1 universally unique identifiers, whose low bits are the node field and so are constant for a host; that arrangement measured as every event landing in one shard.

Both numbers are data model rather than tuning. &emsp;Change either and existing rows keep the bucket and shard they were written with, so queries naming the new ones stop matching the old rows.

The bucket is `text`, not `timestamp`, because it is written by hand into queries that four engines have to parse, and a quoted string means the same thing in CQL, Presto SQL and Spark SQL where a timestamp literal does not. &emsp;It sorts lexicographically, so a range predicate still reads naturally on the paths that cannot prune on it.

`drone_latest_status` holds one row per asset, so a full scan of it is bounded by fleet size rather than by how long the demo has been running. &emsp;That is what lets the dashboard scan it for indicators several times a minute without pretending Cassandra enjoys table scans.

Embeddings live in their own table deliberately: PrestoDB's bundled Cassandra driver cannot parse the CQL `vector` type and drops the metadata for any table carrying one, so a vector column on `drone_latest_status` would hide that table from Presto entirely. &emsp;Keeping 1536 floats out of the row the map reads every few seconds also keeps that read small.
