# Mission Control: the dashboard

A web dashboard over the running stack, at <http://localhost:4000>. It exists to make one claim visible: that the transactional store, the analytical engine, both batch paths and the reader inside the dashboard itself are reading the same data, at the same moment, with nothing copied between them.

Everything on every page is a query against the running stack. &emsp;There are no fixtures, no seeded screenshots and no invented numbers. &emsp;Where a figure cannot be measured the page shows a dash.

```
                        browser :4000
                              │
                         nginx │ serves the bundle, proxies /api
                              ▼
                     Quarkus backend :8000
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

The fifth path is inside the box the dashboard already occupies, which is why it does not appear above:

```
                     Quarkus backend :8000
                        │  cqlite + DataFusion, in this process
                        ▼
                     cassandra-data/, mounted read-only
                        └── the live SSTable files, parsed where they lie
```

## The pages

| Page         | What it shows                                                              | Where the data comes from                                                        |
| ------------ | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| **Overview** | Fleet indicators, ingestion volume, service health, the latest alerts       | One bounded scan of `drone_latest_status`, plus the `ingestion_counts` counters    |
| **Map**      | Live positions, restricted airspace, and an asset's recorded flight path    | `drone_latest_status` for positions; `drone_events_by_entity` for the path         |
| **Alerts**   | Zone-proximity and breach alerts, newest first                             | `alerts_by_bucket`, read one hourly partition at a time                           |
| **Explore**  | One statement against one chosen path, vector search, and the five-path comparison | Whichever path you pick; all five read the same Cassandra data              |
| **Transactions** | Three subtabs, selected by `?tab=`: Accord, SQL and Schema               | Accord over six `demo` tables, cassandra-sql over its own three keyspaces, and each engine's own catalog |
| **Streaming** | The latest mutations as they arrive on Kafka, with the registered Avro schema | The `cdc-mutations` topic and Apicurio; nothing here reads Cassandra          |
| **Health**   | Reachability, latency by access path, and the work in flight                | A connection probe per service, one timed query per path, and each engine's own query list |
| **Settings** | Fleet size, event rate, outlier share, pause, and the breach scenario      | Held in the backend; the data producer polls and adopts them                      |

The left navigation collapses to its icons, and stays collapsed across a reload; the comparison table is wide, and the nav is worth its width only while you are moving between pages.

Every page carries the trademark attribution for the projects the stack runs, and marks which are Apache Software Foundation projects and which are not. &emsp;Presto is a Linux Foundation mark, and cqlite carries the Apache licence without being an Apache project; neither endorses this demonstration.

## The comparison that matters

Explore → **Compare engines** runs one statement down the access paths you choose and reports what each took. &emsp;The statement is rewritten per dialect, and the rewrite is shown above each result, so the comparison is inspectable rather than asserted.

Two controls decide what is being asked. &emsp;**Which paths**: all five, or a subset (two against each other, or one on its own as a reference). &emsp;**How to run them**:

- **One at a time**, the default. &emsp;Each path is timed with nothing else the dashboard controls running, so its figure is its own cost, and the single-partition read sampled four times a second beside it is the price that path alone charged the transactional path.
- **All at once.** The paths contend deliberately. &emsp;Every figure inflates, which is the point: this is the mode that shows what the paths cost each other rather than what each costs alone. &emsp;The probe becomes one measurement covering the whole window, because while the paths overlap that cost belongs to all of them and to none in particular. &emsp;Timings from the two modes are not comparable, so the page states which mode produced the ones on screen. &emsp;Expect it to be slower in wall clock than running the paths in turn, and expect a path starved long enough to give up rather than finish: that is the same contention, reported rather than hidden.

A third control appears whenever the bulk reader is one of the paths. &emsp;**Snapshot**: take a fresh one for the read, or re-read the one the last bulk query took.

Taking a snapshot hardlinks every SSTable of the table, so it costs the same whether the query then reads all of it or one window; a fixed cost that a bounded read pays in full. &emsp;Measured on this laptop:

| | Total | Of which the snapshot |
| --- | --- | --- |
| Bounded read of the fleet, fresh | 0.84 s | 0.24 s |
| Bounded read of the fleet, reusing | 0.35 s | 0.06 s |
| One closed window, fresh | 4.07 s | 0.92 s |
| One closed window, reusing | 3.04 s | 0.05 s |

So it is a quarter of a bounded read, and a larger share the more files the table has; not most of the cost, but the part that does not shrink when the question does. &emsp;What reuse spends is currency: the rows are as of when that snapshot was taken rather than now, so the bulk reader stops answering the same instant as the paths that read through Cassandra. &emsp;That is why it is off by default, and why every bulk result says whether its snapshot was reused and how old it was.

Reuse is refused, and a fresh snapshot taken, in three cases: nothing has been taken yet, what was taken has since gone, or too little of its time-to-live (TTL) is left to survive the read. &emsp;That last one matters because Cassandra expires a snapshot on time regardless of who is reading it, and a read that loses its snapshot half way through fails outright.

It pairs particularly well with the window preset once that window has closed, and for a reason worth understanding. &emsp;A finished window cannot change, so any snapshot taken *after* it closed holds all of it: measured before cqlite was added, the three analytical paths of the time still agreed to the row, on the same five group counts totalling 448,555 events, with the bulk reader reading a snapshot 63 seconds old. &emsp;The staleness costs nothing there. &emsp;A snapshot taken *during* the window being queried is a different matter: it holds only part of it, and then the bulk reader's total will be lower than the others'. &emsp;The reported age is what tells the two cases apart.

Each path holds its own connection, including the two Spark paths, which is what lets them genuinely overlap rather than queueing behind a shared HiveServer2 session.

Either way the same read is sampled for three seconds immediately beforehand as a reference. &emsp;It is labelled "before this run" rather than "idle", because the stack never is: the ingest does not stop, the dashboard polls, and a comparison that has just finished may still be releasing Spark executors and snapshots. &emsp;When that reference comes back uneven (a p95 far above its p50) the page says so and suggests running again, since every figure beside it is measured against it.

A second comparison started while one is running is refused with a 409, so that no set of numbers is quietly produced while another run was in flight. &emsp;A run whose browser gives up continues on the server and keeps that refusal in force until it finishes.

The paths are not interchangeable, and that is the point:

| Path | How it reads | What it is for |
| --- | --- | --- |
| **Cassandra** | CQL request path | Point reads and bounded partition reads. No joins, no ordering on arbitrary columns, and grouping only by primary-key columns. |
| **Presto** | CQL request path | Full SQL, distributed scan. Shares the coordinator with live ingest. |
| **Spark SQL** | CQL request path, via spark-cassandra-connector | Full SQL in a Spark job. Per-partition work, and anything you want to hand to Spark afterwards. |
| **Spark bulk reader** | SSTable files, via the Sidecar | Reads a coordinated snapshot straight off disk. Never enters the request path, so a scan here cannot contend with transactional latency. |
| **cqlite** | The live SSTable files, in the dashboard's own process | Full SQL, planned and executed by DataFusion over files cqlite parses in place. No snapshot, no Sidecar and no second service: the parse and the SQL run inside the dashboard, so the whole path is one library it loads. Answers as of the last flush. |

Four presets of deliberately different size, because one query cannot show what five paths are for, and because the size of the question is most of the answer:

- **Latest state**, milliseconds: one bounded read of `drone_latest_status`. Cassandra answers in single-digit milliseconds; everything else pays for planning or for starting a job.
- **Group the fleet**, under a second: `GROUP BY` over the current fleet only. &emsp;This is the smallest question CQL cannot express, so it is the default way to show the refusal without anybody waiting: *"Group by is currently only supported on the columns of the PRIMARY KEY"*.
- **One window**, seconds: the same grouping as the next preset, bounded to the partitions that hold one 15-minute window. &emsp;This is the data model doing its job, and the difference between it and the whole history is the model rather than the engines.
- **Every event ever ingested**, minutes: the same grouping over the whole history. &emsp;Opt-in, because on one node it is minutes rather than seconds.

Which window is asked about comes from the backend's `/api/query/window`, not from the page, because the answer depends on the data: a bucket exists only because the sink wrote it. &emsp;It prefers a window that has **closed**, and it reports separately whether that window is **settled**, meaning the sink has finished writing it. &emsp;But a demo minutes old has no closed window holding anything, because for the first quarter of an hour after a wipe every event is in the window still filling, so it walks back from the last complete window to the newest one with events in it, up to two hours, and falls back to the window now filling when there is none. &emsp;The page says which it got, because that is the difference between "these totals must agree" and "they differ by whatever arrived in between".

The two flags are not the same flag, and the reason is the sink. &emsp;It files each event under the event's own timestamp, so a sink behind the topic keeps inserting into windows the clock has already passed: three paths read the closed window `2026-08-27T18:15` and returned 80,810, 81,697 and 82,869 rows, growing in the order they ran, while the sink sat some 645,900 records behind a producer running 1,899/s against its own 712/s.

`settled` therefore comes from Kafka, where the sink's own progress is recorded. &emsp;For each of the topic's twelve partitions the backend takes the offset of the first record stamped at or after the window's end and compares it with the offset the sink has committed; every partition must be at or past it. &emsp;The sink commits a batch only once every write in it is acknowledged, so a committed offset past that one means nothing left to consume can be filed under this window. &emsp;A partition holding no record that late is settled when the sink has read all of it. &emsp;The flag moves one way only, which is why a comparison may read it before the run and trust it after; `settled_detail` carries the evidence either way, and a false says how many partitions are short and by how many records.

Asking Cassandra instead, whether the window now filling holds a row, is what that replaced, and the flaw was one partition's progress standing for twelve. &emsp;The sink polls with `max_poll_records` across all of them, so under a backlog their positions diverge: measured on a stack four minutes old, the lag ran from 560 records on one partition to 29,718 on another, 130,173 in all, and that test called the window settled. &emsp;Continuous integration then read a closed window that grew by 3,673 rows over the next 70 s, which is the failure the offsets remove.

The table below sets the same grouping bounded to one closed window against the same grouping over the whole history. &emsp;Both presets order by `event_type` and keep five of the twenty types the producer emits round-robin, so their totals are the sum of those five groups rather than the whole window: a 15-minute window holds about 1.8M events at the demo's ~2,000/s, and one closed window measured 1,794,153.

| Path | One closed window | Whole history |
| --- | --- | --- |
| Cassandra | declines the grouping | declines the grouping |
| Presto | 8.8 s | 30.5 s |
| Spark SQL, connector | 22.2 s | 42.8 s |
| Spark bulk reader | 19.8 s | 94.2 s |
| cqlite | 74.7 s | 412.5 s |

Each column is its own sequential run on the same stack, one path at a time, with the ingest running throughout.

Every timing in this section was measured on Cassandra 5.0.9, whose BTI files are version `da`. &emsp;The stack now runs 6.0-alpha2 and writes `ea`, and the paragraphs below name what was measured again there. &emsp;The whole table has not been re-run at this scale, so read these five rows as the run they were: one afternoon, one file set.

**All four analytical paths returned the same five counts, to the row**, 446,778 events across them for the window `2026-08-22T12:30`. &emsp;That is the property a closed window buys and the unbounded presets cannot offer: they see the table grow underneath them and disagree by a few thousand rows, while a finished window is immutable and they agree to the row. &emsp;Continuous integration asserts that equality whenever the window it was given was settled, and says so when it was not: a fresh stack is minutes old and its sink is behind, so the assertion would otherwise be claiming that no event arrived mid-comparison, which is false by design. &emsp;cqlite joins that agreement only because the step flushes the table first; without a flush its answer stops at the newest file on disk, and the reported `data_age_s` is what says so.

The two file readers agreed with each other over the whole history as well, on all five counts, where the two request-path scans came in lower in the order they ran: 4,397,530 for Presto, 4,410,734 for the connector, then 4,420,748 for the bulk reader and for cqlite. &emsp;The rising figures are the table growing under each scan in turn. &emsp;The last two matching to the row is what should happen when no flush falls between them: the Sidecar snapshot hardlinks the files cqlite went on to read, and cqlite reported its newest file as 95 s old, older than the bulk read before it.

**cqlite is the slowest path here, and its own figures say why.** Over the whole history it merged 19 live files of 3,241.1 MB at 8 MB/s where the bulk reader read a 3,256.3 MB snapshot at 35 MB/s: near enough the same bytes, a quarter of the throughput. &emsp;Neither statement has a `WHERE`, which is the only case where a rate from those figures means anything. &emsp;Re-measured on the 6.0 `ea` files, counting the whole table in one sequential run each, the gap narrowed but did not close: cqlite merged 450,318,008 bytes of live files in 32.9 s, 13.7 MB/s, against the bulk reader's 452,446,775-byte snapshot in 13.4 s, 33.9 MB/s. &emsp;Both counts returned 2,509,300 rows. &emsp;Bounding the question is where this path changes character: it recognises equality and `IN` on the partition key and seeks each key through the file's index, so one shard of a window took 2.9 s against 237 s for the same count without the bound. &emsp;Re-measured after the dependency sweep, a shard took 3.7 s over 15 files of 5,074.0 MB, where an unbounded count of the same table took 538.9 s over 13 files of 4,340.2 MB an hour earlier. &emsp;The bound's cost hardly moved as the table grew; the walk's grew with it, which is what a read that touches one partition should do. &emsp;On the `ea` files, with ingest stopped so both statements read one fixed set of 4 generations and 488,777,346 bytes, a shard took 818.5 ms against the walk's 24.9 s: the same 30× separation at a tenth of the volume.

**The bound costs memory where the walk does not**, which is the opposite of what the timings suggest, and it is worth stating because the backend runs this reader in its own process. &emsp;A walk streams: 25,234,785 rows over 4.64 GB of files held 0.15 GB of anonymous memory, where the idle process holds 0.10 GB. &emsp;A seek does not, because cqlite decodes every row of every partition given to one merger before the merge starts, so a window of 16 partitions and 1,779,134 rows held 6.83 GB.

Both halves hold on the 6.0 `ea` files, at close to the same cost per row. &emsp;A walk of 1,714,500 rows over 304,168,186 bytes held 0.11 GB; the same 16-partition window, 1,246,271 rows, held 4.84 GB in one merger and 1.09 GB one at a time. &emsp;So the seek costs about 3.9 GB per million rows on either file set, and the walk costs what it costs whatever it reads.

The reader therefore reads the named partitions one at a time, which took the `da` window to 1.41 GB and was quicker with it, 23.7 s against 39.0 s. &emsp;Neither figure is given back in full, so a repeat costs more than the first: one partition at a time reached 2.67 GB, and all 16 in one merger crossed the container's 8 GB limit, where the kernel killed the backend and compose restarted it. &emsp;The single `ea` read above did not cross it, holding 4.84 GB for a smaller window, which says the limit is reached by row count rather than by file version. &emsp;That limit is the point of having one: a host that runs out of memory lets the kernel pick its victim, and it may pick Cassandra, which every access path reads through.

**A cgroup's peak is not the reader's memory.** &emsp;The `da` walk above charged 4.19 GB of page cache to the same container, being the SSTable files it read, and the `ea` one 0.31 GB against 0.11 GB of its own; a memory limit counts those pages although the kernel reclaims them rather than killing for them. &emsp;`memory.peak` sums anonymous memory and page cache, and reading it alone made a walk that holds 0.15 GB look as though it held 4.37 GB. &emsp;Read `anon` from `memory.stat` while the statement runs, as `scripts/cqlite-memsample.sh` does; a sample costs two `podman exec` calls, so the timings above come from unsampled runs.

**An earlier run over a smaller history showed the connector *slower* with the bound**, 10.0 s against 7.2 s, and the reason is worth keeping. &emsp;It prunes correctly, and Spark reports it reading exactly the window's rows, but it plans a partition-key query as a *single* task where the unbounded scan splits into seventeen. &emsp;So a third of the data is read by one core while four sit idle. &emsp;Pruning and parallelism are not the same thing, and this is the path where they pull against each other.

The whole-history run above, on a seven-core laptop over about 17.7M events, also carries the measurement the mode exists for: a point read sampled throughout each path's scan, and for three seconds beforehand as a reference.

| Path | Answered in | Point read p50 | p95 | max | Reads sampled |
| --- | --- | --- | --- | --- | --- |
| *before the run* | — | 3.1 ms | 4.1 ms | 114.7 ms | 12 |
| Cassandra | declines | — | — | — | — |
| Presto | 30.5 s | 10.1 ms | 29.5 ms | 36.6 ms | 116 |
| Spark SQL, connector | 42.8 s | 5.5 ms | 11.7 ms | 20.6 ms | 167 |
| Spark bulk reader | 94.2 s | 6.9 ms | 23.4 ms | 1,080.9 ms | 355 |
| cqlite | 412.5 s | 3.6 ms | 12.3 ms | 395.3 ms | 1,610 |

Read the right-hand columns, not the "answered in" one. &emsp;Neither file reader is fastest here, and the page does not pretend otherwise: on one node, at this scale, neither is. &emsp;What they are is the two paths that leave the point read nearest where they found it, a median of 3.6 ms for cqlite and 6.9 ms for the bulk reader against a 3.1 ms reference, where Presto reading through the coordinator moved it to 10.1 ms. &emsp;The bulk reader's 1.08 s outlier is the snapshot: hardlinking the live SSTables is a brief pass on the node, so expect one spike at the start and nothing after it. &emsp;cqlite takes no snapshot and has no equivalent pass; its 395 ms maximum is a shared machine's cores, not the request path, which is also the honest limit of the claim on a laptop.

Compare the medians rather than the tails, for two reasons. &emsp;Each path is sampled only while it runs, so the row counts differ by more than an order of magnitude and a longer scan has more chance to catch an unlucky read. &emsp;And the reference itself is twelve reads with a 114.7 ms maximum in them, which is the unevenness the page warns about rather than a floor worth quoting.

None of these figures is a benchmark: this is one node sharing seven cores with Presto, Spark, the dashboard's own reader and a live ingest. &emsp;Given more nodes the analytical paths scale out and the transactional one does not change at all, which is the reason for separating them.

### What bounds a run

Three limits decide when the dashboard stops waiting. &emsp;Running the paths together makes the worst case ordinary, so all three are set for it rather than for the typical one:

- `SPARK_QUERY_TIMEOUT_S`, 900 s, is the Thrift Server socket timeout: how a starved or wedged query is told from a slow one. &emsp;It bounds the wait rather than the query: a scan that answers in 113 s alone was still working after 180 s with three other paths beside it, which is why the old 180 s was too tight for any contended run.
- The bulk reader's snapshot TTL is derived from that timeout rather than chosen separately. &emsp;Cassandra clears a snapshot when its TTL says so, whatever is still reading it, and a read that loses its snapshot mid-scan fails with `Required 1 replicas but only 0 responded`, which is what a fixed fifteen minutes did to a sixteen-minute contended run. &emsp;CI asserts the derived TTL against the timeout the backend is actually running with, so the two cannot drift apart again.
- nginx allows an hour per request, which is generous on purpose. &emsp;Giving up early is the worse failure: the comparison carries on in the backend and keeps holding the lock that makes runs one at a time, so the browser reports a timeout and then has its retry refused. &emsp;The refusal says how long the run it is waiting on has been going, so a long run is distinguishable from a stuck dashboard.

A path that fails is reported with how long it ran first, because a path declining a query in a millisecond and a path starved out after a quarter of an hour are different findings.

**All five paths at once, over the whole history, is a run whose every figure is inflated by the other four.** One measured run finished in 6 m 42 s, with Presto answering in 58.0 s, the connector in 3 m 5.8 s, the bulk reader in 3 m 16.3 s and cqlite in 6 m 42.3 s, none of them starved out. &emsp;Presto paid 1.9× its sequential figure for the company, the connector 4.3× and the bulk reader 2.1×; cqlite came in level with its own, 402 s here against 412 s alone on a slightly larger table, which is what the timings predict: the other four were all finished by 3 m 16 s, so half of its scan had the machine to itself. &emsp;An earlier run over 36.4M events did not finish at all: Presto took 17 m 44 s against 241 s alone, and both Spark paths outlasted the 900 s guard, with the Thrift Server log showing the connector's job still running after the dashboard had stopped waiting for it. &emsp;That outcome is still reachable on a larger table, and raising the guard until such jobs finish would trade a demonstrable answer for a snapshot pinning SSTables for most of an hour, so the limit stays and a path that gives up is reported with the time it ran. &emsp;What survives either way is the measurement the mode exists for: over 1,534 point reads spanning the run, p50 5.1 ms against 5.5 ms taken just before, one read at 1.9 s, and none failing. &emsp;Read that median rather than the tails: the reference was eleven reads with a 209 ms maximum among them, so the pair of medians is the only like-for-like comparison the run offers.

## Seeing and stopping what is running

The Health page carries the operator's half of the dashboard: what the engines are working on, and the controls to stop it. &emsp;Each engine is asked directly rather than the dashboard listing what it submitted, so work it knows nothing about appears too: a query from another browser tab, or a `presto-cli` session in a container, which is usually what you want to know when the dashboard has gone slow for no reason of its own. &emsp;Presto's coordinator and Spark's application UI are both read over HTTP rather than through the dashboard's own connections, because the one query worth asking about is the one holding the connection that would answer.

- **A comparison in flight** is shown with its age, its mode, and which paths have answered so far, since that is what a 409 on Explore is about. &emsp;**Stop it** ends it: Presto's query is cancelled by its coordinator, each Spark path has its connection cut, the Spark jobs are killed as well, and the cqlite scan is told to stop. &emsp;Both halves are needed. &emsp;Cutting the connection stops the dashboard waiting, but Spark carries on working for a session that has gone, and an orphaned job keeps the cores the next comparison would be timed against. &emsp;The run's own request returns at once, marked cancelled, with each path saying which of the two happened to it.
- **Any query** can be cancelled on its own, by the handle its engine gave it: a Presto `query_id` or a Spark job id. &emsp;Spark's jobs also show task progress, which is the only honest progress bar in the stack.
- **Reconnect** rebuilds this backend's connection to a service, two of them for Spark, since the connector and the bulk reader hold their own sessions. &emsp;It costs no downtime and is what clears a session that has gone stale while the service itself is fine. &emsp;A client busy with a query says so instead: rebuilding would queue behind the query rather than replace it, and a control that hangs for a quarter of an hour explains nothing.

Cassandra is listed with the others and says it keeps no register of running queries, because it does not: a point read is milliseconds, so anything worth seeing on this page arrived through one of the other four paths.

cqlite is listed too, and for the opposite reason: it runs in this backend rather than in a service, so there is no host and port to probe and no query id to cancel. &emsp;Its card gives the directory it reads and how many files are in it, and while a scan is running it says so and points at the comparison's own stop control, which is the only handle that exists. &emsp;Stopping it costs nothing afterwards: the merge gives up at its next partition and the next query starts a fresh scan, so this is the one path that needs no reconnect after a cancel.

**Restarting a service is not offered, on purpose.** The dashboard is a container beside the others, reachable from a browser, so control over its neighbours is exactly what it should not have; giving it the container runtime's socket would be a real escalation for a demo. &emsp;Each service card therefore carries the `podman restart` command rather than a button, next to the wipe and snapshot-clearing commands. &emsp;Reconnecting is the half worth having anyway.

## Vector search

Each asset carries a snippet of prose on some unrelated subject, sampled by the producer from `htap-producer/wikipedia.txt`. Explore → **Vector search** embeds your phrase, asks Cassandra's Storage-Attached Index for the nearest neighbours from `drone_text_embeddings`, scores each with `similarity_cosine`, then point-reads each matching asset for its live position. &emsp;One search therefore exercises the analytical index and the transactional path together.

Press **Build embeddings** once to populate the table; nothing is indexed until then.

With `OPENAI_API_KEY` set the backend embeds through that endpoint. &emsp;Without one it uses a local hashing embedder: no key, no network, and matching that is lexical rather than semantic, but real, ranked and reproducible.

### Live embedding, and why it is not on the write path

A one-off build goes stale, because the producer rotates each asset's snippet every 5 to 30 seconds. &emsp;**Live embedding**, beside the build button, keeps the index following those writes: the backend re-reads the snippets every five seconds and embeds the ones whose text changed.

The loop never sits in a write. &emsp;The sink writes a snippet and waits for nothing; the backend reads it afterwards and writes the vector separately, so the index follows the data rather than standing in front of it. &emsp;Measured on a hundred assets with the local embedder, a pass took 32 to 97 ms and embedded 25 to 64 snippets, and the single-partition read beside it stayed where it was: p50 1.9 ms, p95 3.7 ms over 238 reads with the loop running, against p50 2.6 ms and p95 9.0 ms over 236 reads with it off. &emsp;The two samples differ by less than this stack's own variation between runs, which is the claim: the embedder did not move the request path.

The panel states how far behind the index is, because "on" is not the same as "keeping up". &emsp;A pass embeds at most 64 assets and reports the rest as waiting; at a hundred assets, 21 of the hundred snippets were waiting at the moment of one check, all of them written within the previous five seconds. &emsp;A backend that restarts learns from the table which snippets it has already embedded, so turning the loop back on re-embeds what changed rather than the whole fleet.

The alternative was to embed in the ingest sink, on the write itself. &emsp;It was rejected twice over: it would put an embedding call, and with a key a network round trip, in front of every write; and it would make the data path depend on the dashboard, where today either dashboard service can be stopped without touching ingest.

### Why embeddings live in their own table

`drone_text_embeddings` is separate from `drone_latest_status` for two reasons:

1. PrestoDB's bundled Cassandra driver cannot parse the CQL `vector` type, and drops the metadata for the whole table when it meets one. &emsp;A vector column on the live-status table would make that table invisible to Presto, taking the analytical half of the demo with it.
2. An embedding is 1536 floats. &emsp;Keeping it out of the row the map reads every few seconds keeps that read small.

## What limits the bulk reader

A bulk read pulls whole SSTables from the Sidecar over HTTP, so its duration is the table's bytes divided by the rate the Sidecar can serve them. &emsp;That made it the one number worth measuring, and the first measurement was unflattering. &emsp;With `server_verticle_instances: 1`, one Vert.x event loop serving every request, one stream ran at 62 MB/s and four concurrent streams were served *one after another* for the same 64 MB/s in total, while raw disk in the same container did 433 MB/s and `python -m http.server` over the same container hop did 332 MB/s. &emsp;The scan's duration was the table's size divided by that 62 MB/s, to within 4%, with Spark idle at a tenth of a core waiting.

Two configuration changes, and the same query over the same ~40 GB:

| | Sidecar aggregate, 4 streams | Whole-history scan |
| --- | --- | --- |
| One verticle instance, 3 Spark cores | 64 MB/s | 622 s, 648 s |
| Four verticle instances, 3 Spark cores | 293 MB/s | 443 s |
| Four verticle instances, 5 Spark cores | 293 MB/s | 348 s |

The Sidecar's instances decide whether streams overlap at all; Spark's core cap decides how many there are, because each task alternates between streaming and processing and so uses only a fraction of a core. &emsp;Neither is the ceiling now: at 116 MB/s the scan is below what the Sidecar can serve, and the remaining limit is one laptop shared between an ingest, a Cassandra and a scan.

Two consequences worth keeping in mind. &emsp;The cost tracks the table, which grows at the ingest rate: at 2,000 events/s that is 7.2M rows an hour, adding about 13 s to every subsequent scan, so a preset that took four minutes this morning takes longer this afternoon for no other reason. &emsp;And a faster bulk path is felt slightly more by the transactional one, though not through the CQL request path, which it still never touches, but because the disk and the cores are shared. &emsp;Measured during the 348 s scan: point read p50 3.5 ms and p95 9.8 ms.

### Why every bulk result states its volume

Growth is easy to mistake for decay, and after wiping the data it is dramatic: the table refills from nothing, so it doubles, then triples, and each read of it costs proportionally more. &emsp;So every bulk result carries the size of the snapshot it was taken over. &emsp;It is one of the two paths that can say, since the three that read through Cassandra see rows rather than files, and it is the figure that separates "this read was bigger" from "this read was slower". &emsp;cqlite reports the size of the live files it merged, for the same reason and with the same caveat.

The rate it implies is quoted only when the statement scanned all of it. &emsp;A statement naming partitions, as the windowed preset does, reads only those, so dividing the snapshot's size by the duration would describe a scan that never happened.

Measured on a table refilling after a wipe, one preset, four data points: 34.5 s over 1.5 GB, 45.3 s over 2.3 GB, 33.7 s over 2.8 GB, then four consecutive reads at 3.0 GB of 31.9, 33.0, 28.7 and 28.7 s. &emsp;The clock looks erratic and rising; the rate rises steadily from 42 to 107 MB/s as the fixed cost of taking a snapshot and starting a job is amortised over more data. &emsp;Run-to-run variance on one laptop sharing itself with an ingest is easily ±30%, so read the rate rather than the clock, and read neither as a benchmark.

Snapshots from earlier reads are still around while their TTL runs, so `nodetool listsnapshots` can show several at once. &emsp;Measured harmless: four reads in a row left five snapshots and did not slow down, because a snapshot only hard-links files that were already live.

## Why the spark service republishes two resources

The Thrift Server starts with two families of jars resolved by `--packages`: the CQL connector and the Analytics bulk reader. &emsp;Spark puts those on its *application* classloader. &emsp;Both libraries then load a resource by name from a long-lived server thread, whose context classloader is the system one, which cannot see a jar added to the application loader:

- the Cassandra driver's `reference.conf`, which it re-reads every five minutes. &emsp;A reload that cannot find it produces a profile with no defaults, and the next schema refresh parks for ever on a missing `advanced.control-connection.schema-agreement.timeout`.
- the Analytics `bridges/six-zero.jar`, the per-Cassandra-version implementation the bulk reader picks by `cassandra.releaseVersion`. Without it, a bulk read reports `Missing Cassandra implementation for version SIXZERO`.

Both failures are time-dependent, since the first queries after a restart succeed, which makes them unpleasant to diagnose from the dashboard alone. &emsp;The spark service therefore republishes each resource as a jar of its own under `/opt/spark/jars`, which is on the JVM's system classpath. &emsp;Only the resources are republished, so no class and no library version is shadowed.

## Demo controls

The Settings page writes to the backend's memory; the data producer polls `GET /api/settings/demo` every ten seconds and adopts what it finds. &emsp;Every control there changes what the stack generates:

- **Fleet size**: assets emitting telemetry, up to the producer's `MAX_ENTITIES`.
- **Events per second**: total ingest rate across the fleet, 5 to 5,000. &emsp;The stack starts at 5 and every figure in these documents was measured at 2,000, so this is the first control to touch: nothing downstream of the producer bounds the data, and a stack left running at 2,000 fills a laptop's disk in an afternoon.
- **Overheating assets**: the share of the fleet running an anomalous internal temperature, so the outlier queries on Explore have something to find.
- **Pause**: stops generation; stored data stays put.
- **Trigger breach scenario**: flags a real airborne asset as breaching and writes a matching alert, which the map, the indicators and the alert feed then pick up through their ordinary queries.
- **Truncate `drone_latest_status`**: after reducing the fleet size, retired assets keep their last row and the indicators keep counting them. &emsp;This clears them; history and the zones are untouched.

Nothing here is persisted. &emsp;Restarting the backend returns the demo to the values the compose file declares, and the producer follows within a poll cycle.

## Running the dashboard from source

The compose file builds and serves both halves, so this is only for working on them.

```shell
# Backend.  Built by the reactor and run from the packaged output; measured at 0.980 s to
# listening.  Reaching Cassandra from the host means the driver discovers the node's
# in-network broadcast address, so tell it to use the published port instead.
mvn -B -ntp -pl htap-backend -am package -DskipTests
CASSANDRA_HOST=localhost CASSANDRA_TRANSLATE_ADDRESSES_TO=127.0.0.1 \
  PRESTO_HOST=localhost PRESTO_PORT=8088 \
  SPARK_THRIFT_HOST=localhost KAFKA_HOST=localhost KAFKA_PORT=9092 SPARK_UI_HOST=localhost \
  ACCORD_SQL_HOST=localhost \
  java -jar htap-backend/target/quarkus-app/quarkus-run.jar
```

The cqlite path declines under that command, and two settings are why: `CQLITE_LIBRARY` defaults to the path inside the image and `htap-cqlite/dist/` holds Linux libraries only, so a host outside Linux has none to load; and `CQLITE_DATA_DIR` defaults to the container's mount rather than `./cassandra-data/data`. &emsp;`scripts/build-cqlite-so.sh` builds a library. &emsp;The Python backend declined that path from the host for the same reason, so nothing is lost here that was available before.

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
