# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

A runnable demonstration that one Cassandra dataset can serve transactional and analytical work without ETL copies, and that analytical scans need not touch the OLTP request path. Everything here exists to make that claim checkable, so **a change is not finished until it has been run against the stack and measured**. Numbers in the docs, the PR body and the UI copy are measurements from a real run, not estimates; if you cannot measure a claim, do not make it.

## The five access paths

The same rows are reachable five ways, and the difference between them is the whole point of the demo. Every part of `backend/app/db/` and the compare page exists to keep them comparable:

| Path | Route | Property it demonstrates |
| --- | --- | --- |
| `cassandra` | CQL request path | point/bounded reads; **declines** what CQL cannot express (no `GROUP BY` on a non-key column) |
| `presto` | Presto → Cassandra connector → CQL | full SQL, distributed scan, still over the request path |
| `spark` | Spark Thrift Server → spark-cassandra-connector → CQL | batch SQL over the request path |
| `spark_bulk` | Spark Thrift Server → Cassandra Analytics → Sidecar → SSTable files | reads a coordinated snapshot directly; **cannot** contend with OLTP; rows are as of the snapshot, not now |
| `cqlite` | DataFusion → cqlite → the live SSTable files, in the backend process | reads the files where they lie: no snapshot, no Sidecar and no JVM; **cannot** contend with OLTP; rows are as of the last flush, so what is still in a memtable is not read |

Cassandra failing a query is often the correct, interesting result. Do not "fix" it by widening the SQL — the refusal is the finding, and the UI reports it as a decline rather than an error.

## Skills

`.claude/skills/` holds the four procedures that are easy to get subtly wrong; read the relevant one before starting rather than after.

| Skill | Read it when |
| --- | --- |
| `stack` | bringing the stack up, getting a code change into a running service, or reading a container failure |
| `ci-step` | before pushing a workflow change, or to run the failing CI assertion locally |
| `measure` | before quoting any number, or when explaining why one path beat another |
| `schema` | before changing a table, or when a query silently returns nothing |

## Commands

Everything runs under podman-compose. `compose.yml` is a symlink to `podman-compose.yml`; edit the latter.

```bash
# whole stack (needs a podman machine with >12 GB)
podman compose -f podman-compose.yml up -d

# rebuild and restart one service after a code change
podman compose -f podman-compose.yml build backend frontend
podman compose -f podman-compose.yml up -d --no-deps backend frontend

# wipe data and schema (stops everything, deletes cassandra-data/)
./stop-and-clean-data-and-schema.sh
# truncate the demo tables but keep the stack up
./scripts/cleanup-data.sh
```

Frontend: `cd frontend && npm run build` — this runs `tsc -b` first, so it is the typecheck. `npm run dev` proxies `/api` to `localhost:8000` (`VITE_API_PROXY` overrides).

Backend: no test suite. `python3 -m py_compile backend/app/**/*.py` is the syntax check; correctness is verified by running queries against the stack. To run the backend on the host instead of in the network, set `CASSANDRA_TRANSLATE_ADDRESSES_TO=127.0.0.1` — otherwise the driver discovers `172.20.0.10` and cannot reach it.

Rust: no Rust is built here.  The cqlite reader's source lives in the cqlite fork; `cd ~/src/mck/cqlite/cqlite-datafusion && cargo test --all && cargo fmt --all --check && cargo clippy --all-targets` — 38 unit tests and a doctest, in about a second.  There are **no** fixture SSTables: every test is over a synthetic schema or a temporary directory, so anything about reading real files is verified by running a query against the stack.  A reader change reaches this stack through `scripts/build-cqlite-wheel.sh`; see `backend/dist/VENDOR.md`.

Ports: dashboard `4000`, backend API and `/docs` `8000`, Cassandra `9042`, Sidecar `9043`, Presto `8088`, Spark master UI `8080`, Spark application UI `4040`, Thrift Server `10000`, Kafka `9092`.

## Testing

There is one test suite and it is `.github/workflows/test-podman-compose.yaml`: a single job that builds the stack, waits on each service, then asserts behaviour through the CLIs and the dashboard API. The "Test Mission Control dashboard" step is the largest and covers the five paths.

CI is slow (~15 min to a first failure) and podman on the runner has no systemd, so healthchecks never leave "starting" — the workflow does its own ordering and waiting rather than trusting `depends_on: service_healthy`. **Run a step locally before pushing**; see the `ci-step` skill, which extracts a step from the YAML and runs it verbatim against the running stack.

Assertions must be structural, not timing-based, and must hold on a stack that is minutes old with data still arriving. A fresh CI stack has no closed 15-minute window, small tables, and a growing Kafka backlog; assumptions that hold on a laptop that has been ingesting for an hour do not hold there.

## Architecture notes that span files

**The demo schema is owned by the sink**, `ingress/consumer/consumer.py:ensure_schema()`, not by any migration. See the `schema` skill before changing it: a rebuild alone will not apply a new key.

**`demo.events` is partitioned by time**: `PRIMARY KEY ((event_bucket, shard), event_id)`, where `event_bucket` is a 15-minute UTC window as text and `shard` is `crc32(event_id) % 16`. Bucket width and shard count are declared once in `podman-compose.yml` and read by both the sink and the backend (`EVENT_BUCKET_MINUTES`, `EVENT_SHARDS`); the two disagreeing produces queries that match nothing. Text rather than a timestamp so one literal parses in CQL, Presto SQL and Spark SQL alike.

**The Spark container holds both Spark package sets in one JVM.** `spark/install-cassandra-libraries.sh` puts the connector and Analytics jars on the *system* classpath before the Thrift Server starts; `--packages` would put them on the application classloader, where the driver's `reference.conf` is invisible to the config-reload thread and schema resolution wedges after 5 minutes. The Thrift Server also gets its own Derby metastore and a `spark.cores.max` cap so it cannot starve `spark-sql` jobs run beside it in the same container.

**The bulk reader takes a snapshot per read.** `backend/app/db/spark_client.py` derives the snapshot TTL from `spark_query_timeout_s` — Cassandra expires a snapshot on time regardless of who is reading, and a read that loses its snapshot mid-scan fails with "Required 1 replicas but only 0 responded". A caller may reuse the last snapshot (`reuse_snapshot`), which is refused when too little TTL remains. Registering the view and running the statement are one locked operation, because the view name is per-table and a second query would otherwise replace the first's snapshot mid-flight.

**The cqlite reader arrives as a prebuilt wheel, and its source lives in the cqlite fork.** `backend/dist/` holds one abi3 wheel per architecture in Git LFS, and `backend/Dockerfile` verifies the checksum beside it before installing; nothing in the backend image compiles.  The source is `cqlite-datafusion/`, a nested workspace on `mck/open-htap-stack` in [thelastpickle/cqlite](https://github.com/thelastpickle/cqlite), beside the patched `cqlite-core` it reads below, and `backend/dist/VENDOR.md` names the commit each wheel came from.  cqlite's own `arrow` feature is left off on purpose: `RecordBatch` appears in cqlite-core only under `src/export/`, above the query layer, and this crate reads below it, so the feature would add a `HashMap` per row and mean leaving `KWayMerger`.

The trade that arrangement makes is worth knowing before changing the reader.  A reader change now costs a fork commit, a wheel build for both architectures with the amd64 one under emulation, and a commit here; in exchange the backend image builds in seconds rather than the 9m25s it took when it compiled 313,000 lines of Rust, and CI no longer pays that on the scheduled run where no layer cache exists.

The reader reads `cassandra-data/` mounted read-only, in the backend's own process, and its answer is as of the last flush: an unflushed table has no file, so the path declines rather than returning nothing, and `data_age_s` says how far behind the answer is. Registration needs Cassandra once, for each table's `CREATE TABLE`, so the reader cannot parse files with a schema the cluster has since changed.

**A predicate that names partitions is pushed into the reader.** The fork's `cqlite-datafusion/crates/cqlite-datafusion/src/predicate.rs` recognises equality and `IN` on every partition-key column, and reports those filters `Exact` so DataFusion drops them; the scan then seeks to each key through the BTI trie instead of walking the table and discarding rows. Measured on `ea`: one shard of a 15-minute window took 818.5 ms where a walk of the same 4 generations of 488,777,346 bytes took 24.9 s; on a much larger `da` table it was 3.7 s against 539 s. The named keys go to their merger a few at a time, one by default, because that is what bounds the memory: cqlite's seek merger decodes every row of every partition it is given before the merge starts. A window of 16 partitions read together held 4.84 GB of anonymous memory for 1,246,271 `ea` rows and 6.83 GB for 1,779,134 `da` rows, near enough 3.9 GB per million rows on either file set, against 1.09 GB and 1.41 GB read one at a time. One at a time was also faster, 11.3 s against 14.3 s under sampling on `ea` and 23.7 s against 39.0 s on `da`. The walk has no such cost, because `KWayMerger` streams: 1,714,500 `ea` rows over 304,168,186 bytes held 0.11 GB, and 25,234,785 `da` rows over 4.64 GB of files held 0.15 GB, where the idle process holds 0.10 GB. The container's 8 GB limit is what keeps that seek growth off the host: a second window read with all 16 partitions in one merger crossed it on the `da` files and the kernel killed the backend, and before the limit existed it killed uvicorn three times in one afternoon, at 11 to 13 GB, with the host choosing the victim.

**The reader applies the token bound itself, twice, because cqlite drops it.** `ScanTokenBound` is documented as a pushdown hint the consumer must enforce, and a generation this stack writes is BTI, at `da` and at `ea` alike, which fails the gate on cqlite's Summary-guided walk and drains through the stitch-and-parse window instead; that route takes no bound.  `scan.rs::in_slice` tests the partition's token at the merge boundary, and that test is the guarantee: before it, four slices over a 100-row table returned 400 rows, and after it 100 rows at 1, 2, 3, 7 and 16 slices, agreeing with CQL.  `TokenGate`, in the fork's `cqlite-core/src/storage/sstable/reader/data_access/summary_scan/mod.rs`, applies the same bound far earlier, at the one emit both fallback routes pass through, so an out-of-slice row is never converted into a `MergeEntry`.  An equivalent gate in the merge producer saved 43% of the work a slice repeats on one `da` generation, 16.14 s of CPU to 9.14 s, and held peak resident at 35 to 39 MB where seven slices had reached 716 MB; `TokenGate` filters one layer earlier still, so read those as the floor.  The second test stays because `ScanTokenBound` is a hint the consumer must enforce and the provider is published against a registry `cqlite-core` that carries no gate.

**`cqlite_splits` stays at 1 because most of a slice is work every other slice repeats.** The BTI route drains the data section sequentially with no partition-index seek, so each slice re-reads and re-parses the whole file and only the row decode divides.  Swept once per SSTable version, and the direction is the same on both: N× the CPU buys no wall clock.  Two `ea` generations of 180,672,491 bytes took 9.53 s, 14.05 s, 23.83 s and 38.68 s of CPU at 1, 2, 4 and 7 slices, with the wall clock rising from 14.05 s to 17.34 s; one 203.7 MB `da` generation of 1,102,576 rows took 11.79 s, 22.05 s, 40.33 s and 73.01 s, its wall clock 6.0 to 7.4 s at one slice against 11.2 to 12.4 s at seven.  Solving N·P + R from the 2- and 4-slice points puts the repeated share at 53% on `ea` and 71% on `da`, so read it as a majority and not a constant: each is one sweep, and what makes the two differ has not been measured.  Memory is not the reason, which earlier notes here had wrong: the seek path is what holds rows, and splits divide only the walk, whose merger streams.  Raising this waits on a walk driven from `Partitions.db`, which is a new range-scan route in cqlite rather than an added parameter.

**Read `anon` from the cgroup, not `memory.peak`,** when attributing a cqlite query's memory.  A walk charges the page cache of every file it reads to the backend's cgroup, 4.19 GB of it for the whole table, and a memory limit counts those pages although the kernel reclaims them rather than killing for them.  `memory.peak` sums the two, and reading it alone made a walk that holds 0.15 GB look as though it held 4.37 GB.  `scripts/cqlite-memsample.sh` samples `memory.stat` while a statement runs; a sample costs two `podman exec` calls, so it stretches a wall clock and the timings come from an unsampled run.

**A clustering-predicate row skip was measured and not built.** Skipping `assemble_read_cells` and the Arrow append for an excluded row can save only what those cost, and over one shard of a window they are 8.5% of the merge: three paired runs of 111,841 rows averaged 1359.0 ms with no column assembled and 1474.5 ms with ten.  The merge is the other 91.5%, and during a walk it runs at 0.78 of a core out of seven with no block-device reads at all, so the constraint is the single-threaded merge rather than either I/O or row assembly.

**One comparison runs at a time** (`backend/app/routes/query.py`): two overlapping runs would each be timed while the other ran. The lock records what is running, so the Health page can show and cancel it. Cancelling a Spark query means closing its socket (`shutdown(SHUT_RDWR)`, not `close()`) *and* killing the job group via the Spark UI REST API — HiveServer2 leaves jobs running otherwise.

**Accord is on, and `transactional_mode='full'` reaches further than `BEGIN TRANSACTION`.** Six tables opt in — `sessions_open`, `session_seq_applied`, `session_timeline` for the sequence demo, and `zone_occupancy`, `zone_clearance`, `drone_clearance` for the clearance one — and `events` deliberately does not. `full` routes *every* read and write to an opted-in table through Accord, so a plain `INSERT` and an ordinary `SELECT` are both refused at the driver's default LOCAL_ONE: "ConsistencyLevel LOCAL_ONE is unsupported with Accord for write/commit". Opting `events` in would therefore have broken every dashboard read of it as well as putting consensus in front of 2,000 writes a second; `execute_write` and `_timeline_rows` use QUORUM `SimpleStatement`s rather than changing `execute_query`'s global default. A transaction must also be **deterministic**, so every timeuuid and timestamp is bound by the caller with `uuid_from_time`; `now()` would be evaluated per replica. And an Accord transaction returns **no `[applied]` column**, only the row its own `SELECT` projects, so `backend/app/routes/transactions.py` derives `applied` and the refusal reason from the guard values and CI asserts the row count rather than the field. The option cannot be added to an existing table, which costs a wipe to learn.

**The clearance semaphore counts down because Accord will not compare two `LET` references.** `IF occ.granted < occ.capacity` raises `SyntaxException … IllegalArgumentException null`, so `_grant_cql` holds a decrementing `remaining` and tests `occ.remaining > 0`; the counter's agreement with the holder rows is then checked on every response as `capacity == remaining + holders` and reported as `consistent`. The demo's seven outcomes are what the transaction is for, and the contention route is the claim: 8, 16 and 32 concurrent askers against a capacity of 2 each granted exactly 2, with no errors and with different winners between runs. Measured over 100 repeats, a grant's p50 is 1.31 ms and a release's 1.24 ms. The sink owns the capacities, so an assertion reads `capacity` from the response rather than naming 2.

Measured over four runs of 2,000 applied transactions, the median is stable and the maximum is not: 1.66 – 1.87 ms for the transaction, 0.83 – 1.00 ms for an `IF NOT EXISTS` lightweight transaction and 0.43 – 0.56 ms for a plain insert, against maxima of 6.2 – 26.7, 4.7 – 32.0 and 3.2 – 28.4 ms respectively. No path's tail separates from the others, so quote the medians and say the maxima are noise; one run alone had read as though the transaction's tail were the finding. All of it is one node at RF=1, which pays none of the round trip Accord exists to reduce, so it is a floor.

**Accord makes an unclean shutdown fatal, and the entrypoint has to say otherwise.** Accord writes a `started` marker into `cassandra-data/accord_journal/` and a `stopped` marker on a clean stop; finding the first without the second, `AccordService.localStartup()` throws "Stop marker is older than start marker (-1<…), so cannot assume we have a complete log of our votes in any consensus groups. Exiting." Every table's data is intact and the node will not open. A `podman machine stop`, a sleeping laptop or an out-of-memory kill each cause it. `cassandra/entrypoint.sh` therefore sets `accord.journal.stop_marker_failure_policy: ALLOW_UNSAFE_STARTUP`, and what that gives up is the guarantee that this node knows every vote it cast; at RF=1 there is no peer to hold a conflicting one, so **a multi-node cluster must not carry it**. The key is snake_case although the Java field is `stopMarkerFailurePolicy`, the enum is `EXIT` (the default) / `UNSAFE_STARTUP` / `ALLOW_UNSAFE_STARTUP` / `REBOOTSTRAP`, and both middle values reach the same branch; established by `javap`, because 6.0-alpha2 is installed from a binary tarball and there is no source here to read.

The failure was hard to attribute, and each way it hid is worth knowing. The container reported `Up (starting)` with nothing listening, because the entrypoint polled `cqlsh` against a daemon it had backgrounded and lost; that loop now tests `kill -0` and exits, so the container exits and its log ends at the cause. `spark` looked like a second, unrelated failure but had simply never been started, because its `depends_on` is `condition: service_healthy` and cassandra never became healthy: `State=initialized`, `StartedAt=0001-01-01`, an empty log. And neither the sink nor the backend recovers a dead session on its own — ten hours later the sink was still reporting "Unable to complete the operation against any hosts" with an empty error map, meaning no host left to try. The `stack` skill has the recovery.

**The live embedder runs behind the writes, never in them.** `backend/app/routes/vector.py` can keep `drone_text_embeddings` following the snippets the sink writes: it re-reads them every five seconds and embeds the ones whose text changed, holding one digest per asset in memory so an unchanged snippet costs nothing. Embedding in the sink instead was rejected twice over: it would put an embedding call, and with a key a network round trip, in front of every write; and it would make the data path depend on the dashboard, where today either dashboard service can be stopped without touching ingest. The toggle is off at startup so that turning it on is what shows the point read staying put. Measured at a hundred assets: a pass of 32 to 97 ms, and p50 1.9 ms against a 2.6 ms reference over ~237 reads each.

**Reported figures are labelled with what they measure.** `snapshot_bytes` is the size of the snapshot a bulk read was taken over, and `sstable_bytes` the size of the live files a cqlite read opened; neither is what the read consumed when the statement names partitions, so a MB/s rate is quoted only for a statement with no `WHERE`. Likewise the compare presets group by `event_type` and keep five of the twenty types, ordered by name, so the sum of their rows is about a quarter of the window rather than its total. Both mistakes have been made here and corrected; see the `measure` skill for how each was caught.

## Conventions

Comments explain **why**, and record measurements and rejected alternatives where they would otherwise be re-litigated; several in `podman-compose.yml` and `spark_client.py` exist because the obvious setting was tried and measured worse. Match that density rather than stripping it.

Prose in docs and UI copy follows Strunk and White, two spaces after a period, semicolons and colons over em dashes, and no hard wrapping in markdown. State limitations plainly — the demo's credibility rests on the awkward numbers being present.

Editing `podman-compose.yml`: the Spark `command` is one single-quoted shell string, so an apostrophe anywhere in it, comments included, truncates the script; and compose interpolates `${...}` everywhere, comments included, so a shell variable needs its dollar doubled.

## Language rules

- **Terminology taboos.**  Do not use "canonical", "cache", "bearing", "substrate"; name the precise thing instead (the source, the pinned projection, the memoised value, and so on).
- **Expand an acronym on first use** in a document, then use the bare form.
- **Avoid Negative parallelisms:** "It's not X, it's Y." As in: "It's not a product launch. &emsp;It's a paradigm shift." This is rhetorical construction, and is rarely of value.
- **Avoid Rule of threes:** "Innovative, transformative, and groundbreaking." &emsp;Avoid defaulting to triplets when listing anything: adjectives, benefits, takeaways.
- **Avoid False ranges:** "From intimate gatherings to global movements." &emsp;"From technical expertise to creative vision." &emsp;The structure implies a spectrum, but there's no actual spectrum. &emsp;These are just loosely related things dressed up to sound comprehensive.
- **Avoid Compulsive summaries:** "Overall," "In conclusion". I.e, the tendency to restate what was just said, even when the passage is too short to require it. &emsp;Human writers sometimes do this in long documents, its use should be rare.
- **Use terminology accurately.**  Follow the ASD-STE100 Standard for Technical Documentation.  All terminology terms that has appeared with a specific technical meaning and context should thereafter not be borrowed. Avoid AI typical vocabulary: delve, intricate, tapestry, pivotal, underscore, landscape, foster, testament, enhance, crucial.

## House writing style

- English per Strunk and White's *The Elements of Style*.
- Favour semicolons, colons, and commas over em-dashes.
- Do not hard-wrap lines in markdown or other prose documents.
- Keep paragraphs to 1-4 sentences (essay style, introduce one concept per paragraph, prefer sentences short and active in voice and use more paragraphs). 
- Two spaces after a sentence-ending period; write the second as `&emsp;` in markdown.