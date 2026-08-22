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

Measured on a seven-core laptop with the ingest running at 2,000 events/s, that same predicate without the grouping — `SELECT count(*)` — over one closed window holding 1,791,542 events, and then over a window that holds none:

| Path | Window with data | Empty window |
| --- | --- | --- |
| Cassandra | times out at the coordinator, 5.0 s | 19.1 ms |
| Presto | 5.4 s | 424 ms |
| Spark SQL, connector | no answer inside the 900 s guard | 3.1 s |
| Spark bulk reader | 14.3 s | 3.2 s |
| cqlite | 44.2 s | 2.6 s |

Presto, the bulk reader and cqlite returned the same 1,791,542, which is what a closed window is for: it cannot change while three paths count it. &emsp;cqlite joins that agreement only once the window's last rows have been flushed: an earlier run of the same statement, whose newest file was 112 s old, returned 1,740,038, and the run whose newest file was 30 s old returned the 1,791,542 the other two did.

That count costs the backend its own memory, which is the present limit on how large a question this path can be asked. &emsp;Measured over the window above, resident memory went from 340.3 MB to 7.224 GB in 26 s and settled at 4.088 GB; the CI comparison over another closed window took it from 4.274 GB to 11.08 GB, where it stayed after the run. &emsp;An earlier identical count was killed for exceeding memory, which is what a full-window merge risks on a 24 GB container machine shared with Cassandra and Spark. &emsp;Why the merge holds that much, and why it does not release it, is not diagnosed.

The empty-window column is the pruning, and each path prunes in its own currency. &emsp;Presto pushes the predicate into Cassandra and is answered in 424 ms; the connector pushes down the same predicate and spends 3.1 s of Spark's own overhead around it. &emsp;The bulk reader skips SSTables whose token range cannot hold those keys, but 2.1 s of its 3.2 s went on taking the snapshot before it could skip anything. &emsp;cqlite seeks the sixteen keys through each file's own index, having opened 7 files of 1,153.2 MB in 278.9 ms; that is its whole fixed cost, an eighth of the snapshot beside it. &emsp;Spark's own metrics show the bulk reader's skipping directly: seventeen tasks read 1,791,542 records for the populated window and **0** for the empty one, 47.8 s of executor time against 1.1 s.

Two paths could not answer the populated count at all, and both outcomes are findings. &emsp;Cassandra times out at the coordinator, because 1.8M rows through the request path is what the request path is not for: the window is addressable, not cheap. &emsp;The connector did not answer inside the dashboard's 900 s guard; the same path counts the same window without complaint when the count is grouped, in 23.9 s, so the failure is undiagnosed and no argument here rests on that path's ungrouped count.

The shard exists to keep those partitions a sane size. &emsp;One 15-minute window at the demo's default 2,000 events/s is about 1.8M rows, far too much for a single partition; over sixteen shards it is nearer 110k rows, a few tens of megabytes. &emsp;It comes from a hash of the event's id rather than from the asset, so the spread does not collapse when the fleet is small. &emsp;It is a hash rather than the id taken modulo sixteen because these are version-1 universally unique identifiers, whose low bits are the node field and so are constant for a host; that arrangement measured as every event landing in one shard.

Both numbers are data model rather than tuning. &emsp;Change either and existing rows keep the bucket and shard they were written with, so queries naming the new ones stop matching the old rows.

The bucket is `text`, not `timestamp`, because it is written by hand into queries that every access path has to parse, and a quoted string means the same thing in CQL, Presto SQL, Spark SQL and DataFusion SQL where a timestamp literal does not. &emsp;It sorts lexicographically, so a range predicate still reads naturally on the paths that cannot prune on it.

`drone_latest_status` holds one row per asset, so a full scan of it is bounded by fleet size rather than by how long the demo has been running. &emsp;That is what lets the dashboard scan it for indicators several times a minute without pretending Cassandra enjoys table scans.

Embeddings live in their own table deliberately: PrestoDB's bundled Cassandra driver cannot parse the CQL `vector` type and drops the metadata for any table carrying one, so a vector column on `drone_latest_status` would hide that table from Presto entirely. &emsp;Keeping 1536 floats out of the row the map reads every few seconds also keeps that read small.
