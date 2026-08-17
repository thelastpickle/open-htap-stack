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
| **Health**   | Per-service reachability and latency by access path                        | A TCP probe per service, and one timed query per path                             |
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

Three presets of deliberately different size, because one query cannot show what four paths are for,
and because the size of the question is most of the answer:

- **Latest state**, milliseconds — one bounded read of `drone_latest_status`. Cassandra answers in
  single-digit milliseconds; everything else pays for planning or for starting a job.
- **Group the fleet**, under a second — `GROUP BY` over the current fleet only. This is the smallest
  question CQL cannot express, so it is the default way to show the refusal without anybody waiting:
  *"Group by is currently only supported on the columns of the PRIMARY KEY"*.
- **Every event ever ingested**, minutes — the same grouping over the whole history. Opt-in, because
  on one node it is minutes rather than seconds.

One measured run of the last preset, one path at a time, over 36.4M events on a seven-core laptop
with the ingest running, gives the shape of it:

| Path | Answered in | Point read p50 | p95 | max |
| --- | --- | --- | --- | --- |
| *before the run* | — | 2.2 ms | 2.7 ms | 2.9 ms |
| Cassandra | declines | — | — | — |
| Presto | 241 s | 2.6 ms | 13.5 ms | 72 ms |
| Spark SQL, connector | 113 s | 2.9 ms | 7.0 ms | 96 ms |
| Spark bulk reader | 147 s | 2.3 ms | 5.4 ms | 357 ms |

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
900 s guard. The Thrift Server log shows the bulk reader's job completing after 27 minutes with
nothing left to hand the answer to. Raising the guard until they finish would trade a demonstrable
answer for an hour of waiting and a snapshot pinning SSTables for most of it, so the limit stays and
the page says what to expect. What survives is the measurement the mode exists for: over 6,335 point
reads spanning the window, p50 4.6 ms and p95 23.7 ms against 2.3 ms and 2.9 ms taken just before —
the transactional path made twice as slow at the median and eight times at the tail, with no read
failing. Select fewer paths and they contend and still finish.

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
