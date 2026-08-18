# Mission Control — the dashboard

A web dashboard over the running stack, at <http://localhost:4000>. It exists to make one claim
visible: that the transactional store, the analytical engine and both batch paths are reading the
same data, at the same moment, with nothing copied between them.

Everything on every page is a query against the running stack. There are no fixtures, no seeded
screenshots and no invented numbers. Where a figure cannot be measured the page shows a dash.

```
                        browser :4000
                              │
                         nginx │ serves the bundle, proxies /api
                              ▼
                     FastAPI backend :8000
              CQL ──────┬──────┬────── HiveServer2
                        │  Presto HTTP        │
                        ▼      ▼              ▼
                 ┌───────────────────┐   ┌──────────────┐
                 │     Cassandra     │◄──┤    Presto    │
                 │                   │   └──────────────┘
                 │  request path ◄───────┤ Spark        │ connector
                 │  SSTable files ◄──────┤ (Thrift)     │ bulk reader
                 └───────────────────┘   └──────────────┘
                        ▲                       │
                        └── Sidecar: snapshot ──┘
```

## The pages

| Page         | What it shows                                                              | Where the data comes from                                                        |
| ------------ | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| **Overview** | Fleet KPIs, ingestion volume, service health, the latest alerts            | One bounded scan of `drone_latest_status`, plus the `ingestion_counts` counters    |
| **Map**      | Live positions, restricted airspace, and an asset's recorded flight path    | `drone_latest_status` for positions; `drone_events_by_entity` for the path         |
| **Alerts**   | Zone-proximity and breach alerts, newest first                             | `alerts_by_bucket`, read one hourly partition at a time                           |
| **Explore**  | SQL console, vector search, and the four-path comparison                    | Whichever path you pick; all four read the same Cassandra data                    |
| **Health**   | Reachability, latency by access path, and the work in flight                | A TCP probe per service, one timed query per path, and each engine's own query list |
| **Settings** | Fleet size, event rate, outlier share, pause, and the breach scenario      | Held in the backend; the data producer polls and adopts them                      |

## The comparison that matters

Explore → **Compare engines** runs one statement down the access paths you choose and reports what
each took. The statement is rewritten per dialect, and the rewrite is shown above each result, so the
comparison is inspectable rather than asserted.

Two controls decide what is being asked. **Which paths**: all four, or a subset — two against each
other, or one on its own as a reference. **How to run them**:

- **One at a time**, the default. Each path is timed with nothing else the dashboard controls
  running, so its figure is its own cost, and the single-partition read sampled four times a second
  beside it is the price that path alone charged the transactional path.
- **All at once.** The paths contend deliberately. Every figure inflates, which is the point: this is
  the mode that shows what the paths cost each other rather than what each costs alone. The probe
  becomes one measurement covering the whole window, because while the paths overlap that cost
  belongs to all of them and to none in particular. Timings from the two modes are not comparable,
  so the page states which mode produced the ones on screen. Expect it to be slower in wall clock
  than running the paths in turn, and expect a path starved long enough to give up rather than
  finish: that is the same contention, reported rather than hidden.

Each path holds its own connection, including the two Spark paths, which is what lets them genuinely
overlap rather than queueing behind a shared HiveServer2 session.

Either way the same read is sampled for three seconds immediately beforehand as a reference. It is
labelled "before this run" rather than "idle", because the stack never is: the ingest does not stop,
the dashboard polls, and a comparison that has just finished may still be releasing Spark executors
and snapshots. When that reference comes back uneven — a p95 far above its p50 — the page says so
and suggests running again, since every figure beside it is measured against it.

A second comparison started while one is running is refused with a 409, so that no set of numbers is
quietly produced while another run was in flight. A run whose browser gives up continues on the
server and keeps that refusal in force until it finishes.

The paths are not interchangeable, and that is the point:

| Path | How it reads | What it is for |
| --- | --- | --- |
| **Cassandra** | CQL request path | Point reads and bounded partition reads. No joins, no ordering on arbitrary columns, and grouping only by primary-key columns. |
| **Presto** | CQL request path | Full SQL, distributed scan. Shares the coordinator with live ingest. |
| **Spark SQL** | CQL request path, via spark-cassandra-connector | Full SQL in a Spark job. Per-partition work, and anything you want to hand to Spark afterwards. |
| **Spark bulk reader** | SSTable files, via the Sidecar | Reads a coordinated snapshot straight off disk. Never enters the request path, so a scan here cannot contend with OLTP latency. |

Four presets of deliberately different size, because one query cannot show what four paths are for,
and because the size of the question is most of the answer:

- **Latest state**, milliseconds — one bounded read of `drone_latest_status`. Cassandra answers in
  single-digit milliseconds; everything else pays for planning or for starting a job.
- **Group the fleet**, under a second — `GROUP BY` over the current fleet only. This is the smallest
  question CQL cannot express, so it is the default way to show the refusal without anybody waiting:
  *"Group by is currently only supported on the columns of the PRIMARY KEY"*.
- **One window**, seconds — the same grouping as the next preset, bounded to the partitions that hold
  one 15-minute window. This is the data model doing its job, and the difference between it and the
  whole history is the model rather than the engines.
- **Every event ever ingested**, minutes — the same grouping over the whole history. Opt-in, because
  on one node it is minutes rather than seconds.

Which window is asked about comes from the backend's `/api/query/window`, not from the page, because
the answer depends on the data: a bucket exists only because the sink wrote it. It prefers a window
that has **closed**, since a closed window cannot change while the paths read it, which is what makes
the next paragraph possible. But a demo minutes old has no closed window holding anything — for the
first quarter of an hour after a wipe every event is in the window still filling — so it walks back
from the last complete window to the newest one with events in it, up to two hours, and falls back to
the window now filling when there is none. The page says which it got, because that is the difference
between "these totals must agree" and "they differ by whatever arrived in between".

The same grouping bounded to one closed window against the whole history. Both presets return the five
commonest event types, so their totals are the sum of those five groups rather than the whole window:
the producer emits twenty types round-robin, and a 15-minute window holds about 1.8M events at the
demo's ~2,000/s — measured, one closed window held 1,794,153.

| Path | One closed window | Whole history |
| --- | --- | --- |
| Cassandra | declines the grouping | declines the grouping |
| Presto | 3.1 s | 6.7 s |
| Spark SQL, connector | 10.0 s | 7.2 s |
| Spark bulk reader | 9.2 s | 23.6 s |

**All three analytical paths returned the same five counts, to the row**, 448,610 events across them.
That is the property a closed window buys and
the unbounded presets cannot offer: they see the table grow underneath them and disagree by a few
thousand rows, while a finished window is immutable and they agree to the row. CI asserts that
equality whenever the window it was given had closed, and says so when it had not: a fresh stack is
minutes old, so the assertion would otherwise be claiming that no event arrived mid-comparison, which
is false by design.

**The connector is slower with the bound, and the reason is worth knowing.** It prunes correctly —
Spark reports it reading exactly the window's rows — but it plans a partition-key query as a *single*
task, where the unbounded scan splits into seventeen. So a third of the data is read by one core while
four sit idle. Pruning and parallelism are not the same thing, and this is the path where they pull
against each other; bounding the question on the other three makes them two to three times faster.

One measured run of the last preset, one path at a time, over 36.4M events on a seven-core laptop
with the ingest running, gives the shape of it:

| Path | Answered in | Point read p50 | p95 | max |
| --- | --- | --- | --- | --- |
| *before the run* | — | 2.2 ms | 2.7 ms | 2.9 ms |
| Cassandra | declines | — | — | — |
| Presto | 241 s | 2.6 ms | 13.5 ms | 72 ms |
| Spark SQL, connector | 113 s | 2.9 ms | 7.0 ms | 96 ms |
| Spark bulk reader | 147 s | 2.3 ms | 5.4 ms | 357 ms |

The bulk reader's figure here predates the Sidecar concurrency fix described under *What limits the
bulk reader*; the same path is now about 1.9× faster than this table implies.

Read the right column, not the middle one. The bulk reader is not the fastest here and the page does
not pretend otherwise: on one node, at this scale, it is not. What it is, is the only path that
leaves point-read latency where it found it — p50 unchanged, p95 lowest of the three — because its
scan reads SSTable files rather than entering the request path. Its one outlier is the snapshot: hard-
linking the live SSTables is a brief pass on the node, so expect a single spike at the start and
nothing after it. The two paths that read through Cassandra move p95 by 2.6× and 5× instead, which
on a single shared node is exactly what should happen.

Two things are worth watching beyond the clock. The paths that read through Cassandra see the table
grow underneath them while they scan, so their totals differ from each other — 1,833,893 against
1,851,178 against 1,857,129 for the same group above; the bulk reader answers from one snapshot, so
its groups are consistent with each other. And none of these figures is a benchmark: this is one node
sharing its cores with Presto, Spark and a live ingest. Given more nodes the three analytical paths
scale out and the transactional one does not change at all, which is the reason for separating them.

### What bounds a run

Three limits decide when the dashboard stops waiting. Running the paths together makes the worst case
ordinary, so all three are set for it rather than for the typical one:

- `SPARK_QUERY_TIMEOUT_S`, 900 s, is the Thrift Server socket timeout: how a starved or wedged query
  is told from a slow one. It bounds the wait, not the query — a scan that answers in 113 s alone was
  still working after 180 s with three other paths beside it, which is why the old 180 s was too
  tight for any contended run.
- The bulk reader's snapshot TTL is derived from that timeout rather than chosen separately.
  Cassandra clears a snapshot when its TTL says so, whatever is still reading it, and a read that
  loses its snapshot mid-scan fails with `Required 1 replicas but only 0 responded` — which is what a
  fixed fifteen minutes did to a sixteen-minute contended run. CI asserts the derived TTL against the
  timeout the backend is actually running with, so the two cannot drift apart again.
- nginx allows an hour per request, which is generous on purpose. Giving up early is the worse
  failure: the comparison carries on in the backend and keeps holding the lock that makes runs one at
  a time, so the browser reports a timeout and then has its retry refused. The refusal says how long
  the run it is waiting on has been going, so a long run is distinguishable from a stuck dashboard.

A path that fails is reported with how long it ran first, because a path declining a query in a
millisecond and a path starved out after a quarter of an hour are different findings.

**All four paths at once, over the whole history, does not finish here, and that is the finding.** One
measured run: 27 minutes of wall clock, Presto answering in 17 m 44 s against 241 s alone, and both
Spark paths giving up — with seven cores between four scans of 36M rows, the Spark jobs outlast the
900 s guard. The Thrift Server log shows the connector's job running the whole 27 minutes and
finishing after the dashboard had stopped waiting for it. Raising the guard until they finish would trade a demonstrable
answer for an hour of waiting and a snapshot pinning SSTables for most of it, so the limit stays and
the page says what to expect. What survives is the measurement the mode exists for: over 6,335 point
reads spanning the window, p50 4.6 ms and p95 23.7 ms against 2.3 ms and 2.9 ms taken just before —
the transactional path made twice as slow at the median and eight times at the tail, with no read
failing. Select fewer paths and they contend and still finish.

## Seeing and stopping what is running

The Health page carries the operator's half of the dashboard: what the engines are working on, and
the controls to stop it. Each engine is asked directly rather than the dashboard listing what it
submitted, so work it knows nothing about appears too — a query from another browser tab, or a
`presto-cli` session in a container, which is usually what you want to know when the dashboard has
gone slow for no reason of its own. Presto's coordinator and Spark's application UI are both read
over HTTP rather than through the dashboard's own connections, because the one query worth asking
about is the one holding the connection that would answer.

- **A comparison in flight** is shown with its age, its mode, and which paths have answered so far,
  since that is what a 409 on Explore is about. **Stop it** ends it: Presto's query is cancelled by
  its coordinator, each Spark path has its connection cut, and the Spark jobs are killed as well.
  Both halves are needed. Cutting the connection stops the dashboard waiting, but Spark carries on
  working for a session that has gone, and an orphaned job keeps the cores the next comparison would
  be timed against. The run's own request returns at once, marked cancelled, with each path saying
  which of the two happened to it.
- **Any query** can be cancelled on its own, by the handle its engine gave it: a Presto `query_id` or
  a Spark job id. Spark's jobs also show task progress, which is the only honest progress bar in the
  stack.
- **Reconnect** rebuilds this backend's connection to a service — two of them for Spark, since the
  connector and the bulk reader hold their own sessions. It costs no downtime and is what clears a
  session that has gone stale while the service itself is fine. A client busy with a query says so
  instead: rebuilding would queue behind the query rather than replace it, and a control that hangs
  for a quarter of an hour explains nothing.

Cassandra is listed with the others and says it keeps no register of running queries, because it does
not: a point read is milliseconds, so anything worth seeing on this page arrived through one of the
other two paths.

**Restarting a service is not offered, on purpose.** The dashboard is a container beside the others,
reachable from a browser, so control over its neighbours is exactly what it should not have; giving it
the container runtime's socket would be a real escalation for a demo. Each service card therefore
carries the `podman restart` command rather than a button, next to the wipe and snapshot-clearing
commands. Reconnecting is the half worth having anyway.

## Vector search

Each asset carries a snippet of prose on some unrelated subject, sampled by the producer from
`ingress/producer/wikipedia.txt`. Explore → **Vector search** embeds your phrase, asks Cassandra's
SAI index for the nearest neighbours from `drone_text_embeddings`, scores each with
`similarity_cosine`, then point-reads each matching asset for its live position. One search
therefore exercises the analytical index and the transactional path together.

Press **Build embeddings** once to populate the table; nothing is indexed until then.

With `OPENAI_API_KEY` set the backend embeds through that endpoint. Without one it uses a local
hashing embedder — no key, no network, and matching that is lexical rather than semantic, but real,
ranked and reproducible.

### Why embeddings live in their own table

`drone_text_embeddings` is separate from `drone_latest_status` for two reasons:

1. PrestoDB's bundled Cassandra driver cannot parse the CQL `vector` type, and drops the metadata
   for the whole table when it meets one. A vector column on the live-status table would make that
   table invisible to Presto, taking the analytical half of the demo with it.
2. An embedding is 1536 floats. Keeping it out of the row the map reads every few seconds keeps that
   read small.

## What limits the bulk reader

A bulk read pulls whole SSTables from the Sidecar over HTTP, so its duration is the table's bytes
divided by the rate the Sidecar can serve them. That made it the one number worth measuring, and the
first measurement was unflattering. With `server_verticle_instances: 1` — one Vert.x event loop
serving every request — one stream ran at 62 MB/s and four concurrent streams were served *one after
another* for the same 64 MB/s in total, while raw disk in the same container did 433 MB/s and
`python -m http.server` over the same container hop did 332 MB/s. The scan's duration was the table's
size divided by that 62 MB/s, to within 4%, with Spark idle at a tenth of a core waiting.

Two configuration changes, and the same query over the same ~40 GB:

| | Sidecar aggregate, 4 streams | Whole-history scan |
| --- | --- | --- |
| One verticle instance, 3 Spark cores | 64 MB/s | 622 s, 648 s |
| Four verticle instances, 3 Spark cores | 293 MB/s | 443 s |
| Four verticle instances, 5 Spark cores | 293 MB/s | 348 s |

The Sidecar's instances decide whether streams overlap at all; Spark's core cap decides how many there
are, because each task alternates between streaming and processing and so uses only a fraction of a
core. Neither is the ceiling now: at 116 MB/s the scan is below what the Sidecar can serve, and the
remaining limit is one laptop shared between an ingest, a Cassandra and a scan.

Two consequences worth keeping in mind. The cost tracks the table, which grows at the ingest rate: at
2,000 events/s that is 7.2M rows an hour, adding about 13 s to every subsequent scan, so a preset that
took four minutes this morning takes longer this afternoon for no other reason. And a faster bulk path
is felt slightly more by the transactional one — not through the CQL request path, which it still
never touches, but because the disk and the cores are shared. Measured during the 348 s scan: point
read p50 3.5 ms and p95 9.8 ms.

### Why every bulk result states its volume

Growth is easy to mistake for decay, and after wiping the data it is dramatic: the table refills from
nothing, so it doubles, then triples, and each read of it costs proportionally more. So every bulk
result carries the size of the snapshot it was taken over. It is the only path that can say — the other
three read through Cassandra and see rows, not files — and it is the figure that separates "this read
was bigger" from "this read was slower".

The rate it implies is quoted only when the statement scanned all of it. A statement naming partitions,
as the windowed preset does, reads only those, so dividing the snapshot's size by the duration would
describe a scan that never happened.

Measured on a table refilling after a wipe, one preset, four data points: 34.5 s over 1.5 GB, 45.3 s
over 2.3 GB, 33.7 s over 2.8 GB, then four consecutive reads at 3.0 GB of 31.9, 33.0, 28.7 and 28.7 s.
The clock looks erratic and rising; the rate rises steadily from 42 to 107 MB/s as the fixed cost of
taking a snapshot and starting a job is amortised over more data. Run-to-run variance on one laptop
sharing itself with an ingest is easily ±30%, so read the rate rather than the clock, and read neither
as a benchmark.

Snapshots from earlier reads are still around while their TTL runs, so `nodetool listsnapshots` can
show several at once. Measured harmless: four reads in a row left five snapshots and did not slow down,
because a snapshot only hard-links files that were already live.

## Why the spark service republishes two resources

The Thrift Server starts with two families of jars resolved by `--packages`: the CQL connector and
the Analytics bulk reader. Spark puts those on its *application* classloader. Both libraries then load
a resource by name from a long-lived server thread, whose context classloader is the system one, which
cannot see a jar added to the application loader:

- the Cassandra driver's `reference.conf`, which it re-reads every five minutes. A reload that cannot
  find it produces a profile with no defaults, and the next schema refresh parks for ever on a missing
  `advanced.control-connection.schema-agreement.timeout`.
- the Analytics `bridges/five-zero.jar`, the per-Cassandra-version implementation the bulk reader picks
  by `cassandra.releaseVersion`. Without it, a bulk read reports
  `Missing Cassandra implementation for version FIVEZERO`.

Both failures are time-dependent — the first queries after a restart succeed — which makes them
unpleasant to diagnose from the dashboard alone. The spark service therefore republishes each resource
as a jar of its own under `/opt/spark/jars`, which is on the JVM's system classpath. Only the resources
are republished, so no class and no library version is shadowed.

## Demo controls

The Settings page writes to the backend's memory; the data producer polls
`GET /api/settings/demo` every ten seconds and adopts what it finds. Every control there changes
what the stack generates:

- **Fleet size** — assets emitting telemetry, up to the producer's `MAX_ENTITIES`.
- **Events per second** — total ingest rate across the fleet.
- **Overheating assets** — the share of the fleet running an anomalous internal temperature, so the
  outlier queries on Explore have something to find.
- **Pause** — stops generation; stored data stays put.
- **Trigger breach scenario** — flags a real airborne asset as breaching and writes a matching
  alert, which the map, the KPIs and the alert feed then pick up through their ordinary queries.
- **Truncate `drone_latest_status`** — after reducing the fleet size, retired assets keep their last
  row and the KPIs keep counting them. This clears them; history and the zones are untouched.

Nothing here is persisted. Restarting the backend returns the demo to the values the compose file
declares, and the producer follows within a poll cycle.

## Running the dashboard from source

The compose file builds and serves both halves, so this is only for working on them.

```shell
# Backend.  Reaching Cassandra from the host means the driver discovers the node's
# in-network broadcast address, so tell it to use the published port instead.
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
CASSANDRA_HOST=localhost CASSANDRA_TRANSLATE_ADDRESSES_TO=127.0.0.1 \
  PRESTO_HOST=localhost PRESTO_PORT=8088 \
  SPARK_THRIFT_HOST=localhost KAFKA_HOST=localhost KAFKA_PORT=9092 SPARK_UI_HOST=localhost \
  .venv/bin/uvicorn app.main:app --reload --port 8000
```

```shell
# Frontend.  Vite proxies /api to localhost:8000, so no CORS and no compiled-in host.
cd frontend && npm install && npm run dev
```

The API documents itself at <http://localhost:8000/docs>.

## Resetting the data

```shell
scripts/cleanup-data.sh              # truncate the generated tables, keep the stack up
./stop-and-clean-data-and-schema.sh  # stop everything and clear the data directories
```
