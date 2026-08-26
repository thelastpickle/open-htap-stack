# Architecture: Scope, Consistency, and Enterprise Considerations

This document covers the technical scope of the hybrid transactional/analytical processing (HTAP) stack, the consistency model it provides, the trade-offs it makes, and the enterprise concerns it needs to answer. &emsp;The [README](../README.md) covers the quickstart; [WHY.md](WHY.md) covers the argument for the approach.

## Contents

- [A. &emsp;What this demo is (and is not)](#a-what-this-demo-is-and-is-not)
- [B. &emsp;How strict serializability is achieved (and what availability means)](#b-how-strict-serializability-is-achieved-and-what-availability-means)
- [C. &emsp;HTAP analytics without duplicating data](#c-htap-analytics-without-duplicating-data-persisted-structure-reads--snapshots)
- [D. &emsp;Resource isolation: protecting OLTP tail latency](#d-resource-isolation-protecting-oltp-tail-latency)
- [E. &emsp;SQL interface: what "Postgres-compatible" means here](#e-sql-interface-what-postgres-compatible-means-in-this-repo)
- [F. &emsp;Parquet / Iceberg exports: optional optimisation, not foundation](#f-parquet--iceberg-exports-optional-optimisation-not-the-foundation)
- [G. &emsp;Consistency model: tunable where appropriate](#g-consistency-model-tunable-where-appropriate)
- [H. &emsp;Enterprise realities](#h-enterprise-realities-and-how-to-think-about-them)
- [Hard Questions FAQ](#hard-questions-faq)

---

## A. What this demo is (and is not)

### What it is

- A runnable proof-of-concept showing end-to-end ingestion and query paths across online transaction processing (OLTP) and analytics.
- A demonstration of **global strict serializability** for multi-key and multi-table transactions under the constraints described below.
- A demonstration of analytics reads that operate against persisted structures (including coordinated snapshots) to avoid OLTP interference.

### What it is not (yet)

- A claim of full feature parity with mature, general-purpose SQL databases. &emsp;No two SQL dialects agree anyway.
- A promise that every enterprise workload can be consolidated immediately without trade-offs.
- A turnkey drop-in replacement for warehouses and lakes in every scenario; some organisations may still benefit from exporting to Parquet/Iceberg on cold storage for cost, performance or lifecycle reasons.

---

## B. How strict serializability is achieved (and what availability means)

### Where strict serializability comes from

Strict serializability is provided by **Accord**, Cassandra Enhancement Proposal 15 (CEP-15), included in this stack. &emsp;Accord's design goals are:

- strict-serializable isolation across multi-key transactions
- low latency in the common case (single wide-area round trip under normal conditions)
- leaderless operation without introducing a global bottleneck

Accord is informed by research from the University of Michigan and Apple. &emsp;The protocol uses commodity clocks rather than specialised time infrastructure (like Google Spanner's TrueTime), which removes the commit-wait latency Spanner pays on writes.

### Availability model (practical interpretation)

This system is **quorum-based** for transactional decisions:

- If a quorum can be reached for the keys involved in a transaction, the transaction proceeds.
- If a quorum cannot be reached due to failures or partitions, the system preserves correctness by blocking progress or falling back to slower coordination paths, rather than returning inconsistent results.

A practical rule of thumb:

- With replication factor (RF) = 3, you can typically lose one replica per key-range and still make progress.
- With higher RF (and appropriate rack, availability-zone or region placement), you can tolerate more failures. &emsp;How many can be down depends on the quorum configuration and the failure-domain topology.

Accord's leaderless design means availability is **per-request**. &emsp;A down server affects only the transactions that touch keys whose coordination path includes that server, not the cluster as a whole. &emsp;This is materially different from leader-based consensus (Raft, multi-Paxos) where a leader outage pauses all writes in the leader's scope until a new leader is elected.

### What the same guarantee costs elsewhere

Strict serializability is rare enough that the systems offering it can be named, and naming them is more useful than claiming exclusivity. &emsp;What separates them is where each puts the component that orders transactions, because that choice sets the failure model.

CEP-15 surveys the field it was designed against, and the survey is the clearest statement of the design axis. &emsp;It puts existing approaches in two families: a global leader, as in FaunaDB and FoundationDB, which it calls "simple and correct but introduces a scalability bottleneck that would be irreconcilable with the size of many Cassandra clusters"; and a combination of a transaction log with per-key leaders, as in DynamoDB, CockroachDB and YugabyteDB, which it judges "unlikely to be better than two round-trips in the general case" and which "appear to require either specialised hardware clocks or provide only serializable isolation".

| System | What it documents | What the design charges for it |
| --- | --- | --- |
| **FoundationDB** | Strict serializability | One Sequencer process assigns every read and commit version in the cluster; losing it triggers a transaction-system recovery. &emsp;Across 289 production traces the median was 3.08 s and the 90th percentile 5.28 s, with read-write transactions blocked for the duration.  Scalability is very limited. |
| **Google Spanner** | External consistency, which is strict serializability | TrueTime, meaning specialised time infrastructure in every datacentre, and a commit-wait on every write. |
| **CockroachDB** | Serializable | States plainly that it stops short: "CockroachDB doesn't quite offer strict serializability, but we're fairly close to it." &emsp;It permits an anomaly it names causal reverse. |
| **TiDB** | Snapshot isolation, presented as `REPEATABLE-READ` for MySQL compatibility | No serializable level at all. &emsp;Transaction timestamps are allocated by the Placement Driver rather than by the replicas holding the data. |
| **YugabyteDB** | Serializable, in its PostgreSQL-compatible API | Documents serializability rather than external consistency, so it makes no claim about ordering relative to real time. |

The argument for the leaderless design follows from the third column of that table rather than from any benchmark. &emsp;A design that reaches consistency through a leader puts a low-traffic but critical component on the write path. &emsp;Replacing that component with a distributed one tends to leave a smaller critical component behind it, and every failure of whatever remains is a recovery or an election, either of which is a pause.

The probability that some such component fails somewhere rises with node count, so the tail degrades as the cluster grows. &emsp;Building consistency over a store that is available by default has no such component to lose, so the failure model keeps its shape as the cluster grows. &emsp;That is a claim about design rather than a measurement, and it is falsifiable: it predicts that a leader-based system's write tail latency worsens with cluster size faster than a leaderless one's.

**What this costs, and what is not yet shown.** &emsp;Having no leader means no leader-side batching or pipelining of the consensus stream, which is a real throughput advantage that leader-based designs keep. &emsp;Contention on the same keys moves Accord off its one-round-trip fast path onto a slower path. &emsp;Scale-invariance is a property of the design and not a result: Accord is not generally available, and CEP-15 describes its own prototype as "incomplete and not ready for production use".

**None of the figures in this section were measured here.** &emsp;The stack does run Accord, and the README carries what was measured of it, but that is a single node at `replication_factor: 1` and so says nothing about any claim in this section: every one of them is about how a design behaves as a cluster grows. &emsp;Each figure below is the vendor's or the authors' own published number, cited so that it can be checked; see the references at the end of this document. &emsp;In particular, this repository has no head-to-head benchmark against Spanner or any distributed-SQL system, and no published one surfaced, so no throughput comparison is offered. &emsp;The comparison on offer is the mechanism: one wide-area round trip under normal conditions, against a commit-wait or a hop to a leader.  &emsp;This [blog post](https://medium.com/@jingyuzhou/a-critique-on-foundationdb-transaction-system-8b640c06f6cd) demonstrates production experience where these systems quickly fail as minimal scale and cannot past even a smaller number of 30 nodes.

### "No rollback" (what is meant here)

Accord is designed so that transactions do not need to "roll back" in the traditional sense of speculatively-applied state that must be undone.

Instead, transactions are journaled and ordered before they apply, and may be **blocked** (or forced onto a slower coordination path) when conflicts or failures require additional coordination. &emsp;This trades tail latency for correctness and availability under failure.

**The developer-experience claim**: that blocked-transactions are simpler to reason about than explicit rollback, depends on your application. &emsp;If your application relies heavily on explicit rollback semantics (compensating actions, saga patterns built around failed transactions), you'll need to adapt. &emsp;If your application is primarily read-heavy with occasional writes that need strong consistency, the blocked-transaction model often produces cleaner code.

---

## C. HTAP analytics without duplicating data: persisted-structure reads + snapshots

### What "without duplicating data" means here

The goal is to avoid a permanent architectural requirement to:

- copy OLTP data into a second system (warehouse / lake / search index / vector DB / cache tier)
- maintain continuous ETL just to make analytics possible

Instead, the stack supports analytics that read:

- directly from persisted on-disk structures (SSTable-oriented bulk read)
- from coordinated cluster snapshots of those structures

This is why the demo emphasises:

- **Spark Bulk Reader/Writer** paths that interact with persisted structures via Sidecar endpoints
- snapshot-coordinated reads for consistent analytical views

### Snapshot-coordinated analytics (demo behaviour)

In the demo, "snapshotting" refers to:

1. coordinating snapshots across the cluster (a well-understood Cassandra operation)
2. running analytic queries over the snapshotted persisted structures

This yields a stable analytical view while allowing OLTP to continue without disruption: resource isolation by construction.

---

## D. Resource isolation: protecting OLTP tail latency

### Isolation strategy

The demo's design intent:

- OLTP uses the normal request path (Cassandra Query Language, CQL, or the Postgres wire protocol; coordinator → replica)
- analytics uses persisted-structure reads and/or snapshot reads via the Sidecar
- a Sidecar can stream/offload snapshot files to separate storage to reduce repeated I/O contention

The intended outcome:

- minimal impact to OLTP p99 latency under analytic load, provided the system is configured correctly

The architectural reason this works: analytical reads don't go through Cassandra's coordinator queues, the read-repair path, or the normal replica-read path. &emsp;They read SSTable files directly, bypassing all the mechanisms that OLTP requests compete for.

### Query shapes: when to use which path

| Query shape | Best path |
|---|---|
| Point reads (single partition key) | OLTP path |
| Bounded partition reads (single partition key, range of clustering keys) | OLTP path |
| Per-partition analytics (aggregations within a single partition) | OLTP path or persisted-structure reads, depending on concurrency and service-level agreements |
| Wide scans / large token-range reads | Persisted-structure reads (bulk reader) |
| Cross-partition aggregations | Persisted-structure reads |
| Full-table scans | Persisted-structure reads |

**Note**: OLTP wide scans (token-range queries via CQL) will generally have higher p99 latency than partition reads. &emsp;They can still be fast, but the analytics path is designed to be the better tool for that job.

---

## E. SQL interface: what "Postgres-compatible" means in this repo

### What is provided

The adapter is GEICO's [cassandra-sql](https://github.com/geico/cassandra-sql), and it runs in this stack as the `accord-sql` service, built from a pinned revision plus one patch whose whole diff is in this repository. &emsp;It is a **Postgres wire-protocol and Postgres-dialect adapter** on the transaction layer, planning with Apache Calcite. &emsp;`psql`, or any Postgres driver, connects to port 5432 and gets joins, subqueries, aggregates over non-key columns and multi-statement `BEGIN`/`COMMIT` transactions; the **SQL** subtab of the dashboard's Transactions page drives it. &emsp;It sits on Accord: its own tables carry `transactional_mode = 'full'`.

**It is a sixth interface and not a sixth access path.** &emsp;In its `kv` storage mode, the mode its own documentation recommends, it stores SQL rows in three keyspaces of its own under an ordered key-value encoding of its own, so it cannot read `demo.events` and it appears in no comparison with the five paths. &emsp;Its `schema` mode does read native Cassandra tables, and was rejected: that mode's own documentation says "No transactions", "No JOINs or subqueries" and "Eventual consistency only", which leaves it weaker than the Presto path already here.

### What you should assume

- This is **not** a claim of full Postgres feature parity. &emsp;The project states "~40% (core features only)" and calls itself not production-ready.
- The declared constraints are mostly not enforced. &emsp;A duplicate primary key overwrites, and `FOREIGN KEY`, `NOT NULL` and `ENUM` are each accepted and ignored; `UNIQUE` is the one that is held. &emsp;An application that relies on the database to refuse bad data must check that assumption statement by statement.
- Numbers are not exact. &emsp;`DECIMAL` is held as a double, so money arithmetic carries binary floating-point error, and an `UPDATE` that does arithmetic on an integer column stores a double in it.
- A bound parameter of an integer type silently matches nothing. &emsp;This is the defect most likely to reach production quietly, because every Postgres driver binds by default.
- **A join answers wrongly in four distinct ways, and none of them raises.** &emsp;A column name held by two joined tables resolves to one table for the whole statement whatever the qualifier says; `ORDER BY` is ignored on a grouped result; arithmetic across two joined tables returns one operand and discards the operator; and arithmetic against a literal inside a join projection drops the column, so a client reading by position gets a different one. &emsp;Single-table arithmetic and single-table `ORDER BY` are both exact, which is what places all four in the join. &emsp;A join is the reason to reach for this adapter at all, so read this as the section's main caveat.

The README's *Example Application (OLTP) SQL* section carries each of those with the statement that produced it, and timings from a real run. &emsp;Treat the adapter as a pragmatic interoperability layer, useful for onboarding and for tooling compatibility, and not as a substitute for validating the SQL semantics your application depends on.

**If your application relies on Postgres-specific features** (advanced window functions, recursive CTEs, specific transaction isolation semantics, extensions), verify against the project's documented coverage before committing to a migration.

### The partitioner question

cassandra-sql's own prerequisites require `ByteOrderedPartitioner`. &emsp;**This stack runs Murmur3 with 16 tokens and the partitioner needed no patch**, which was checked rather than assumed: no code under `src/main/` reads the partitioner, the one range scan is legal under Murmur3, and row order is discarded anyway. &emsp;`CassandraConfigTest` does assert that `system_views.settings` reports the byte-ordered partitioner, and so fails here; the build therefore runs `-x test`, and the integration tests need a Cassandra on `localhost:9042` that a build stage has not got in any case.

The service does carry one patch, in `accord-sql/patches/`, and it is unrelated to the partitioner: `CassandraExecutor` hard-codes `localhost:9042` in a second Cassandra session, and its unconditional `@PostConstruct` fails that bean, and with it the application, whenever Cassandra is in another container. &emsp;The patch gives that session the four properties `CassandraConfig` already reads.

Byte-ordered partitioning was measured here and rejected for the cluster, because it is cluster-wide and cannot be changed on an existing data directory. &emsp;Three of the demo's features refuse it, two of them inside libraries this repository does not own: storage-attached indexes ("Storage-attached index does not support the following IPartitioner implementations"), which is the only vector index Cassandra has and therefore the whole vector search page; the spark-cassandra-connector ("Unsupported partitioner"); and the CEP-28 bulk reader, whose `Partitioner` is an enum of Murmur3 and Random. &emsp;So the project's own warning that "Byte Order Partitioner + Accord is poorly tested, journals are not compacting, gets slower over time" does not apply to this deployment.

---

## F. Parquet / Iceberg exports: optional optimisation, not the foundation

This stack does not require Parquet/Iceberg to function as an HTAP foundation.

However, exporting to columnar formats is still useful when you explicitly want:

- backups and long-term retention
- cold storage / tiering
- very scan-heavy workloads where columnar storage provides materially better cost/performance
- interop with external tools that expect Iceberg/Parquet inputs

In those cases:

- Parquet/Iceberg are performance and lifecycle optimisations
- the authoritative, freshest, strongly-consistent view remains in the OLTP store

This is a meaningful distinction from architectures where Parquet/Iceberg is itself the source of truth and the OLTP store holds a copy of it.

---

## G. Consistency model: tunable where appropriate

Both the underlying store and the transaction layer support **tunable consistency** (including per-operation tuning).

Use this intentionally:

- **strict-serializable transactions** (via Accord) for correctness-critical invariants: financial transactions, inventory, auth, audit
- **weaker consistency** where latency/availability trade-offs are acceptable and correctness requirements permit: high-volume telemetry, analytics ingest, session data

The availability of tunable consistency does not mean "use it casually." &emsp;Every weakening of consistency is a correctness assertion about what your application can tolerate. &emsp;Document those assertions.

---

## H. Enterprise realities (and how to think about them)

Unified OLTP and analytics stacks must answer the same enterprise concerns any production data platform faces. &emsp;These are the common ones and how we recommend thinking about them.

### 1. Operational maturity and support

Enterprises will ask:

- Who operates this at 2 a.m.?
- What are the failure modes and runbooks?
- How do upgrades, repairs, and incident response work?

**Recommendation**:

- Treat this repo as an evaluation harness, not a production blueprint
- Define an operational model: site-reliability ownership, on-call rotation, service-level indicators and objectives, upgrade cadence, repair and compaction monitoring
- Budget for specialist skills (Cassandra, Accord, Spark bulk I/O) or commercial support contracts

### 2. Governance, security, and data access boundaries

Expect requirements for:

- centralised access control, whether role-based or attribute-based
- auditing and lineage
- data masking and tokenisation policies
- separation of duties and tenant isolation

**Recommendation**:

- Define the governance plane early, **including analytic access patterns**; the Spark Bulk Reader path reads SSTables directly, which has implications for row-level security that the OLTP path enforces
- Ensure consistent policy enforcement across OLTP and analytics interfaces

### 3. Schema evolution and migration

Enterprises need:

- safe schema change workflows
- backfills and reprocessing strategies
- compatibility guarantees across versions

**Recommendation**:

- Document the migration path for legacy SQL workloads: what works today, what is planned
- Accept that wide-column data modelling differs from relational modelling; some Postgres schemas will translate cleanly, others won't

### 4. Change data capture and integration (Kafka, streaming, dedup)

Enterprises typically require:

- reliable change data capture (CDC) to Kafka
- dedup semantics aligned with replication and failure handling
- clean operational experience for CDC pipelines

**This runs in the demo.**&emsp;`demo.drone_latest_status` declares `cdc = true`, and the Sidecar reads the commit log segments Cassandra hard-links into `cdc_raw` and publishes each mutation to `cdc-mutations` as Avro, with the schema in Apicurio.&emsp;Measured: 2,718 records/s, a p50 median of 8.0 s end to end, no decode failure in 5.4 million records.&emsp;The change stream never queries the node, which is the same property the analytical paths have.

Two things an enterprise reader should weigh.&emsp;The replication-factor-aware dedup is configured, `watermark_seconds: 1800`, and one node at RF=1 does not exercise it.&emsp;And `cdc_raw` is bounded, so a publisher far enough behind either loses changes or stalls the writers, depending on `cdc_block_writes`; both states were measured here.&emsp;See [CDC-TO-KAFKA.md](CDC-TO-KAFKA.md).

### 5. Analytics expectations and BI tooling

Teams will ask:

- What is the concurrency model for analytics?
- What are the limits for joins, aggregations, and scans?
- Which BI tools work out of the box?

**Recommendation**:

- Set expectations clearly: "supported query shapes" + "best path per shape"
- Provide example workloads and reproducible benchmarks
- Validate specific BI tool compatibility (Tableau, PowerBI, Looker) against the Presto interface before committing

### 6. Disaster recovery and multi-region

Enterprises will require:

- a clearly documented recovery point objective and recovery time objective
- failover/failback procedures
- proof that invariants hold during regional impairment

**Recommendation**:

- Include a disaster-recovery drill guide and a failure-injection test plan
- Define how "availability" is preserved while respecting correctness
- Accord's leaderless design helps here: quorum loss in one region doesn't stop operations on keys whose quorum is elsewhere

---

## Hard Questions FAQ

### 1. Is this production ready, or generally available?

No. &emsp;This repository is a proof-of-concept you can run today to evaluate the approach. &emsp;Production readiness requires additional hardening, operational tooling, performance tuning, and, for most enterprises, a commercial support contract.

### 2. How can you claim strict serializability and still be "always-on available"?

Strict serializability requires coordination, and coordination requires quorum.

"Available" here means: **as long as a quorum can be reached for the keys involved in a transaction**, the transaction proceeds. &emsp;Accord's leaderless design means availability is per-request; a down server affects only the transactions whose coordination path includes that server, not the cluster as a whole.

Under partitions or failures that prevent quorum for some keys, the system preserves correctness by blocking or degrading to slower coordination paths rather than returning inconsistent results. &emsp;You trade tail latency for correctness during failure, and per-request availability for global availability. &emsp;Both trade-offs are explicit and documented.

### 3. Is "no data duplication" realistic, or do we still need Parquet/Iceberg/lakes?

"No duplication" means you can run many analytics workloads **directly over persisted structures and snapshots** without requiring an always-on ETL copy.

You may still export to Parquet/Iceberg for explicit goals: cold storage, backups, scan-heavy workloads where columnar storage wins on cost/performance, or interop with external tools. &emsp;Those exports are **optional optimisations**, not required plumbing. &emsp;The distinction matters because it changes the ETL layer from mandatory infrastructure to a choice you make per-workload.

### 4. What are the trade-offs enterprises will actually feel?

In practice:

- **SQL feature coverage**: the Postgres-compatible interface is a protocol + dialect adapter, not full Postgres parity. &emsp;Validate your required SQL semantics against what's actually implemented.
- **Query-path selection**: teams need to learn which access path fits which workload. &emsp;The OLTP path is slower for wide scans than the bulk-read path; the bulk-read path has higher setup cost for small queries. &emsp;Wrong-path queries will work but will surprise you on cost or latency.
- **Operational maturity**: Cassandra operations, Accord transaction debugging, Spark bulk I/O tuning are real specialist skills. &emsp;Either build them in-house or contract commercial support.
- **Governance and CDC integration**: the technical capabilities are present; integrating them with existing enterprise governance and CDC platforms is implementation work.
- **Data modelling**: wide-column modelling is different from relational. &emsp;Some schemas translate cleanly (time-series, event logs, key-value lookups); some don't (heavily normalised relational schemas with complex joins).

### 5. How does this compare to CockroachDB, TiDB, YugabyteDB, SingleStore, Snowflake Hybrid Tables, or Postgres + Citus?

Each takes a different bet on the HTAP problem:

- **CockroachDB, TiDB, YugabyteDB**: distributed SQL with strong relational semantics and varying consistency models. &emsp;Excellent fit for application workloads that need relational integrity; analytical throughput on the same cluster is typically more constrained than this stack's bulk-reader approach.
- **SingleStore**: row + columnar hybrid in a single engine, strong analytical performance on fresh data, strong concurrency story for BI. &emsp;Commercial licensing, closed-source core.
- **Snowflake Hybrid Tables / Unistore**: OLTP tables bolted onto an online analytical processing (OLAP) architecture. &emsp;Good if you're already on Snowflake and want to reduce round-trips; the OLTP latency profile, concurrency model, and per-node scaling differ from a distributed-database-first approach.
- **Postgres + Citus**: horizontal sharding of Postgres with good distributed-query support. &emsp;Excellent if you have Postgres expertise already; strict-serializable distributed transactions across shards are bounded differently.

**This stack's bet**: start with a distributed OLTP store that already scales horizontally for write-heavy workloads, add strict-serializable transactions via Accord, and read analytics directly from persisted storage via the bulk reader.

Wins on: write scaling, OLAP throughput per node, per-request availability under failure.
Loses on: relational feature richness (compared to distributed-SQL databases), managed-service maturity (compared to Snowflake), operational familiarity in shops that already know Postgres.

### 6. Is this platform linearly scalable?

Yes, for the documented workload shapes: point reads, bounded partition reads, and write ingest all scale linearly across documented cluster sizes. &emsp;Cassandra's scaling story at this layer is well-established.

Token-range scans and cross-partition transactions have different scaling characteristics and are bounded by coordination overhead rather than node count. &emsp;Wide-scan analytical throughput scales with disk I/O bandwidth per node times node count (which is why the per-node throughput number in the TCO doc matters more than the cluster total).

See the [TCO worksheet](TCO-Comparisons.md) for specific workload/cluster-size examples.

---

## References

- Accord / CEP-15: transactions, strict serializability, failure tolerance goals. &emsp;The goals list, the survey of prior work and the prototype's status are quoted from <https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-15%3A+General+Purpose+Transactions>
- CEP-28: Spark bulk reader/writer via Sidecar to persisted storage
- Cassandra Analytics: bulk reader/writer examples
- cassandra-sql: Postgres wire protocol and a Calcite-planned dialect over Cassandra, <https://github.com/geico/cassandra-sql>. &emsp;The compliance figure, the storage-mode limitations and the partitioner warning quoted in section E are from that repository's own README

Sources for section B's comparison, each quoted from the system's own documentation or paper:

- FoundationDB: Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key Value Store", SIGMOD 2021, <https://www.foundationdb.org/files/fdb-paper.pdf>. &emsp;Strict serializability is section 2.4.2; the singleton Sequencer is 2.3.1; the 3.08 s median and 5.28 s 90th percentile over 289 traces, and the note that client reads were unaffected, are section 5.3; the single August 2020 recovery and the five-9s figure are section 5.1; the 10 KB key, 100 KB value and 10 MB transaction limits are section 2.2, and the 5 s multi-version window is section 6.4
- Google Spanner: "TrueTime and external consistency", <https://cloud.google.com/spanner/docs/true-time-external-consistency>
- CockroachDB: "Living Without Atomic Clocks", <https://www.cockroachlabs.com/blog/consistency-model/>, whose section headed "CockroachDB's consistency model: more than serializable, less than strict serializability" is the source of both the quotation and the causal-reverse anomaly
- TiDB: "TiDB Transaction Isolation Levels", <https://docs.pingcap.com/tidb/stable/transaction-isolation-levels/>, and the Placement Driver's role in "TiDB Architecture", <https://docs.pingcap.com/tidb/stable/tidb-architecture/>
- YugabyteDB: "Transaction isolation levels", <https://docs.yugabyte.com/preview/architecture/transactions/isolation-levels/>
