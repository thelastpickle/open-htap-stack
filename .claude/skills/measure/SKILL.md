---
name: measure
description: Measure a query across the five access paths and read the result honestly — the benchmark endpoint, the Spark REST API, and the traps that produce confident wrong conclusions. Use when quoting a number in docs, a PR body or UI copy, when explaining why one path is slower than another, or when a figure looks surprising.
user-invocable: true
allowed-tools:
  - Bash
  - Read
---

# Measuring, and reading the measurement

Every number in this repository's docs, PR body and UI copy is a measurement from a real run.  Getting one is easy; the work is in not fooling yourself about what it says.

## The measurement

```bash
curl -s -m 600 -X POST localhost:8000/api/query/benchmark \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT event_type, count(*) AS n FROM events GROUP BY event_type ORDER BY n DESC LIMIT 5",
       "limit": 10,
       "engines": ["cassandra", "presto", "spark", "spark_bulk", "cqlite"],
       "mode": "sequential",
       "reuse_snapshot": false}' > .ci-tmp/run.json
```

`mode` is `sequential` or `parallel`.  Sequential times each path alone, which is the only way a timing means what it appears to mean.  Parallel shows what contention costs.  They answer different questions; **never quote one beside the other** as though they were the same measurement.

Per engine the response carries `query_time_ms`, `row_count`, `rows`, `available`, `error`, the sampled `oltp`, and for the bulk reader `snapshot_bytes`, `snapshot_ms`, `snapshot_reused`, `snapshot_age_s`.  The cqlite path takes no snapshot and reports four fields of its own instead: `sstable_files` and `sstable_bytes` for the live files it merged, `reader_open_ms` for what listing and opening them cost, and `data_age_s` for how long ago the newest of them was written.  The top level adds `mode`, `oltp_baseline`, `oltp_combined` and `cancelled`.  Read the field names from `backend/app/models.py` rather than guessing them; `duration_ms` and `oltp_impact` are plausible and wrong.

`cassandra` returning an error on an analytical query is a result, not a failure of the run.  The endpoint reports it as a decline.

## Counting a window honestly

To check what a window actually holds, count it rather than deriving it from a grouped preset.  Ask `/api/query/window` for a bucket that has `closed`, then:

```bash
# per partition, from cqlsh; each is one partition, so it answers in a second or two
for k in $(seq 0 15); do
  podman exec cassandra cqlsh cassandra -e \
    "SELECT count(*) FROM demo.events WHERE event_bucket='$B' AND shard=$k;"
done
```

Presto and the bulk reader will both count the whole window in one statement (`WHERE event_bucket = '…' AND shard IN (0,…,15)`) and agree with that sum.  Two observations from doing it: the CQL path times out counting a whole window through the coordinator, which is the request path behaving correctly rather than a fault, and the connector path failed the same count with a bare `HiveSQLException` — undiagnosed, so do not build an argument on that path's count.  A window still filling cannot be counted this way at all; it grows while you read it.

## Where the detail is

The Spark application UI serves JSON on `:4040`:

```bash
curl -s localhost:4040/api/v1/applications | jq -r '.[].id'
curl -s "localhost:4040/api/v1/applications/$APP/stages?status=complete" \
  | jq -r '.[] | "\(.stageId) tasks=\(.numTasks) in=\(.inputRecords) run=\(.executorRunTime)ms"'
curl -s "localhost:4040/api/v1/applications/$APP/sql?planDescription=false" | jq '.[-1]'
curl -s "localhost:4040/api/v1/applications/$APP/jobs?status=running" | jq -r '.[].jobGroup'
```

`numTasks` is what explains most surprises.  A pruned read on one task loses to an unpruned read on seventeen; pruning that costs parallelism is slower, and the task count says so where the timing alone does not.

## Traps, each of which has already caught me here

**The result of a `GROUP BY … LIMIT k` is not a total.**  The window and whole-history presets group by `event_type`, order by it and keep five, and the producer emits **twenty** types round-robin, so summing those rows gives about a quarter of the window.  I read 1,794,446 `inputRecords` against a "row count" of 448,610, inferred a constant 4× reporting quirk in Spark, and wrote it down.  There is no quirk: a closed 15-minute window really holds ~1.79M events at the demo's ~2,000/s, and counting one directly gives 1,794,153 from cqlsh, Presto and the bulk reader alike.  `inputRecords` was telling the truth and the summed top-five was not a total.  When a figure looks like a clean multiple of another, suspect the arithmetic before inventing a mechanism.

**`snapshot_bytes` is availability, not consumption.**  It is the size of the snapshot the read was taken over.  When the statement names partitions, the read touched a fraction of it, so a MB/s rate computed from it is fiction.  Quote a rate only for a statement with no `WHERE`; the CI checker gates it exactly that way.

**A cqlite count answers as of the last flush.**  The path reads files, so a window still filling is undercounted and a table Cassandra has never flushed is declined outright; a stack that started minutes ago is in exactly that state.  `data_age_s` says which of the two you are looking at.  Flush before comparing counts — `podman exec cassandra nodetool flush demo` — as the CI step does; without it, a disagreement with the other four paths measures the memtable rather than the reader.

**cqlite prints a `float` in full.**  A `float` column is 32 bits, and this path renders the double it widens to, so 41.8 arrives as 41.79999923706055 where the CQL path shortens it.  Compare such a column with a tolerance; the CI identity check uses 0.1 °C.  A row that differs only in that expansion is the same row.

**A trend across runs is usually the table growing.**  "It is getting slower" turned out to be 34.5 s over 1.5 GB and then 28.7 s over 3.0 GB, a rate rising from 42 to 107 MB/s.  Repeat a measurement, and record the table size beside it, before believing a direction.

**The first run after a restart is not representative.**  JIT, page cache and the connector's session setup all land on it.

**Snapshot cost is a share, not the bulk of the work.**  Reuse won 2.4× on a small read and 1.34× on a windowed one; the snapshot is roughly a quarter of a bounded read.  Do not describe it as where the time goes.

## Attribution

Before writing down *why* a path was slower, get a second observation that distinguishes your explanation from its rival: the task count, a run with the filter removed, the stage's `executorRunTime` against wall clock.  A plausible mechanism that matches one number is how a wrong explanation gets published.
