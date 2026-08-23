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

Rust: `cd cqlite-datafusion && cargo test --all && cargo fmt --all --check && cargo clippy --all-targets` — 38 unit tests and a doctest, in about a second, so run them before rebuilding the backend image.  There are **no** fixture SSTables: every test is over a synthetic schema or a temporary directory, so anything about reading real files is verified by running a query against the stack.

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

**The cqlite reader is a Rust crate built into the backend image, and it builds cqlite from a copy of upstream's main.** There is no cqlite-core 0.17: crates.io stops at 0.16.1, there is no such tag, and main still declares 0.16.1.  So `cqlite-datafusion/vendor/cqlite-core/` holds main's tip, redirected there by `[patch.crates-io]`; `vendor/cqlite-core/VENDOR.md` records the commit, what was dropped, the local patches and how to refresh.  A `[patch]` binds only the workspace that declares it, so `crates/cqlite-datafusion/` still names plain `cqlite-core = "0.16.1"` and stays publishable.  cqlite's own `arrow` feature is left off on purpose: `RecordBatch` appears in cqlite-core only under `src/export/`, above the query layer, and this crate reads below it, so the feature would add a `HashMap` per row and mean leaving `KWayMerger`.

`cqlite-datafusion/` is a self-contained Cargo workspace that refers to nothing else in this repository, so it can be offered upstream to cqlite as it stands; `backend/Dockerfile` builds it with maturin in a first stage and installs the wheel in the second, which is why the `backend` build context is the repository root. It reads `cassandra-data/` mounted read-only, in the backend's own process, and its answer is as of the last flush: an unflushed table has no file, so the path declines rather than returning nothing, and `data_age_s` says how far behind the answer is. Registration needs Cassandra once, for each table's `CREATE TABLE`, so the reader cannot parse files with a schema the cluster has since changed.

**A predicate that names partitions is pushed into the reader.** `cqlite-datafusion/crates/cqlite-datafusion/src/predicate.rs` recognises equality and `IN` on every partition-key column, and reports those filters `Exact` so DataFusion drops them; the scan then seeks to each key through the BTI trie instead of walking the table and discarding rows. Measured: one shard of a 15-minute window took 3.7 s where a walk of the whole table took 539 s. The named keys go to their merger a few at a time, one by default, because that is what bounds the memory: cqlite's seek merger decodes every row of every partition it is given before the merge starts, so a window of 16 partitions and 1.78M rows held 6.83 GB of anonymous memory read together and 1.41 GB read one at a time, and one at a time was faster, 23.7 s against 39.0 s. The walk has no such cost, because `KWayMerger` streams: 25,234,785 rows over 4.64 GB of files held 0.15 GB, where the idle process holds 0.10 GB. A second window read with all 16 partitions in one merger crosses the container's 8 GB limit, and the kernel kills the backend; before that limit existed it killed uvicorn three times in one afternoon, at 11 to 13 GB, and the host chose the victim.

**The reader applies the token bound itself, twice, because cqlite drops it.** `ScanTokenBound` is documented as a pushdown hint the consumer must enforce, and a Cassandra 5 generation is BTI, which fails the gate on cqlite's Summary-guided walk and drains through the stitch-and-parse window instead; that route takes no bound.  `scan.rs::in_slice` tests the partition's token at the merge boundary, and that test is the guarantee: before it, four slices over a 100-row table returned 400 rows, and after it 100 rows at 1, 2, 3, 7 and 16 slices, agreeing with CQL.  `SliceGate`, a listed local patch in `cqlite-datafusion/vendor/cqlite-core/`, applies the same bound far earlier, at the one emit funnel every producer thread passes through, so an out-of-slice row is never converted into a `MergeEntry`.  That saved 43% of the work a slice repeats, 16.14 s of CPU to 9.14 s, and held peak resident at 35 to 39 MB where seven slices previously reached 716 MB.  Its tests cannot live beside it, because the vendored manifest has no `[dev-dependencies]`; `VENDOR.md` says what covers it instead.

**`cqlite_splits` stays at 1 because 71% of a slice is work every other slice repeats.** The BTI route drains the data section sequentially with no partition-index seek, so each slice re-reads and re-parses the whole file and only the row decode divides.  One 203.7 MB generation of 1,102,576 rows took 11.79 s, 22.05 s, 40.33 s and 73.01 s of CPU time at 1, 2, 4 and 7 slices; solving N·P + R from the 2- and 4-slice points gives 71% repeated and 29% divided, in a pre-gate and a post-gate sweep alike.  So the ceiling is about 1.4× wall clock for N× the CPU, and on seven cores shared with the stack the wall clock rose instead, 6.0 to 7.4 s at one slice against 11.2 to 12.4 s at seven.  Memory is not the reason, which earlier notes here had wrong: the 6.83 GB figure is the **seek** path, and splits divide only the walk, whose merger streams.  Raising this waits on a walk driven from `Partitions.db`, which is a new range-scan route in cqlite rather than an added parameter.

**Read `anon` from the cgroup, not `memory.peak`,** when attributing a cqlite query's memory.  A walk charges the page cache of every file it reads to the backend's cgroup, 4.19 GB of it for the whole table, and a memory limit counts those pages although the kernel reclaims them rather than killing for them.  `memory.peak` sums the two, and reading it alone made a walk that holds 0.15 GB look as though it held 4.37 GB.  `scripts/cqlite-memsample.sh` samples `memory.stat` while a statement runs; a sample costs two `podman exec` calls, so it stretches a wall clock and the timings come from an unsampled run.

**A clustering-predicate row skip was measured and not built.** Skipping `assemble_read_cells` and the Arrow append for an excluded row can save only what those cost, and over one shard of a window they are 8.5% of the merge: three paired runs of 111,841 rows averaged 1359.0 ms with no column assembled and 1474.5 ms with ten.  The merge is the other 91.5%, and during a walk it runs at 0.78 of a core out of seven with no block-device reads at all, so the constraint is the single-threaded merge rather than either I/O or row assembly.

**One comparison runs at a time** (`backend/app/routes/query.py`): two overlapping runs would each be timed while the other ran. The lock records what is running, so the Health page can show and cancel it. Cancelling a Spark query means closing its socket (`shutdown(SHUT_RDWR)`, not `close()`) *and* killing the job group via the Spark UI REST API — HiveServer2 leaves jobs running otherwise.

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