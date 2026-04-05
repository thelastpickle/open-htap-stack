# Architecture: Scope, Consistency, and Enterprise Considerations

This document covers the technical scope of the HTAP stack, the consistency model it provides, the trade-offs it makes, and the enterprise concerns it needs to answer. The [README](../README.md) covers the quickstart; [WHY.md](WHY.md) covers the argument for the approach.

## Contents

- [A. What this demo is (and is not)](#a-what-this-demo-is-and-is-not)
- [B. How strict serializability is achieved (and what availability means)](#b-how-strict-serializability-is-achieved-and-what-availability-means)
- [C. HTAP analytics without duplicating data](#c-htap-analytics-without-duplicating-data-persisted-structure-reads--snapshots)
- [D. Resource isolation: protecting OLTP tail latency](#d-resource-isolation-protecting-oltp-tail-latency)
- [E. SQL interface: what "Postgres-compatible" means here](#e-sql-interface-what-postgres-compatible-means-in-this-repo)
- [F. Parquet / Iceberg exports: optional optimization, not foundation](#f-parquet--iceberg-exports-optional-optimization-not-the-foundation)
- [G. Consistency model: tunable where appropriate](#g-consistency-model-tunable-where-appropriate)
- [H. Enterprise realities](#h-enterprise-realities-and-how-to-think-about-them)
- [Hard Questions FAQ](#hard-questions-faq)

---

## A. What this demo is (and is not)

### What it is

- A runnable proof-of-concept showing end-to-end ingestion and query paths across OLTP and analytics.
- A demonstration of **global strict serializability** for multi-key and multi-table transactions under the constraints described below.
- A demonstration of analytics reads that operate against persisted structures (including coordinated snapshots) to avoid OLTP interference.

### What it is not (yet)

- A claim of full feature parity with mature, general-purpose SQL databases. Every SQL is different anyway 🤷
- A promise that every enterprise workload can be consolidated immediately without trade-offs.
- A turnkey drop-in replacement for warehouses and lakes in every scenario, some organizations may still benefit  exporting to Parquet/Iceberg on cold storage for cost/performance/lifecycle reasons.

---

## B. How strict serializability is achieved (and what availability means)

### Where strict serializability comes from

Strict serializability is provided by **Accord (CEP-15)**, included in this stack. Accord's design goals are:

- strict-serializable isolation across multi-key transactions
- low latency in the common case (single wide-area round trip under normal conditions)
- leaderless operation without introducing a global bottleneck

Accord is informed by research from the University of Michigan and Apple. The protocol uses commodity clocks rather than specialized time infrastructure (like Google Spanner's TrueTime), which removes the commit-wait latency Spanner pays on writes.

### Availability model (practical interpretation)

This system is **quorum-based** for transactional decisions:

- If a quorum can be reached for the keys involved in a transaction, the transaction proceeds.
- If a quorum cannot be reached due to failures or partitions, the system preserves correctness by blocking progress or falling back to slower coordination paths, rather than returning inconsistent results.

A practical rule of thumb:

- With replication factor (RF) = 3, you can typically lose one replica per key-range and still make progress.
- With higher RF (and appropriate rack/AZ/region placement), you can tolerate more failures. How many can be down depends on the quorum configuration and the failure-domain topology.

Accord's leaderless design means availability is **per-request**. A down server affects only the transactions that touch keys whose coordination path includes that server, not the cluster as a whole. This is materially different from leader-based consensus (Raft, multi-Paxos) where a leader outage pauses all writes in the leader's scope until a new leader is elected.

### "No rollback" (what is meant here)

Accord is designed so that transactions do not need to "roll back" in the traditional sense of speculatively-applied state that must be undone.

Instead, transactions are journaled and ordered before they apply, and may be **blocked** (or forced onto a slower coordination path) when conflicts or failures require additional coordination. This trades tail latency for correctness and availability under failure.

**The developer-experience claim**: that blocked-transactions are simpler to reason about than explicit rollback, depends on your application. If your application relies heavily on explicit rollback semantics (compensating actions, saga patterns built around failed transactions), you'll need to adapt. If your application is primarily read-heavy with occasional writes that need strong consistency, the blocked-transaction model often produces cleaner code.

---

## C. HTAP analytics without duplicating data: persisted-structure reads + snapshots

### What "without duplicating data" means here

The goal is to avoid a permanent architectural requirement to:

- copy OLTP data into a second system (warehouse / lake / search index / vector DB / cache tier)
- maintain continuous ETL just to make analytics possible

Instead, the stack supports analytics that read:

- directly from persisted on-disk structures (SSTable-oriented bulk read)
- from coordinated cluster snapshots of those structures

This is why the demo emphasizes:

- **Spark Bulk Reader/Writer** paths that interact with persisted structures via Sidecar endpoints
- snapshot-coordinated reads for consistent analytical views

### Snapshot-coordinated analytics (demo behavior)

In the demo, "snapshotting" refers to:

1. coordinating snapshots across the cluster (a well-understood Cassandra operation)
2. running analytic queries over the snapshotted persisted structures

This yields a stable analytical view while allowing OLTP to continue without disruption — resource isolation by construction.

---

## D. Resource isolation: protecting OLTP tail latency

### Isolation strategy

The demo's design intent:

- OLTP uses the normal request path (CQL / Postgres wire protocol, coordinator → replica)
- analytics uses persisted-structure reads and/or snapshot reads via the Sidecar
- a Sidecar can stream/offload snapshot files to separate storage to reduce repeated I/O contention

The intended outcome:

- minimal impact to OLTP p99 latency under analytic load, provided the system is configured correctly

The architectural reason this works: analytical reads don't go through Cassandra's coordinator queues, the read-repair path, or the normal replica-read path. They read SSTable files directly, bypassing all the mechanisms that OLTP requests compete for.

### Query shapes: when to use which path

| Query shape | Best path |
|---|---|
| Point reads (single partition key) | OLTP path |
| Bounded partition reads (single partition key, range of clustering keys) | OLTP path |
| Per-partition analytics (aggregations within a single partition) | OLTP path or persisted-structure reads, depending on concurrency and SLAs |
| Wide scans / large token-range reads | Persisted-structure reads (bulk reader) |
| Cross-partition aggregations | Persisted-structure reads |
| Full-table scans | Persisted-structure reads |

**Note**: OLTP wide scans (token-range queries via CQL) will generally have higher p99 latency than partition reads. They can still be fast, but the analytics path is designed to be the better tool for that job.

---

## E. SQL interface: what "Postgres-compatible" means in this repo

### What is provided

The SQL interface in this stack is a **Postgres wire-protocol + Postgres-dialect adapter** implemented as a prototype on top of the transaction layer, using Apache Calcite for query parsing and planning.

### What you should assume

- This is **not** a claim of full Postgres feature parity.
- The prototype's GitHub page documents exactly what is implemented and what is not.
- Treat it as a pragmatic interoperability layer:
  - useful for onboarding, tooling compatibility, and incremental migration from Postgres
  - **not** a substitute for validating the SQL semantics your application actually depends on

**If your application relies on Postgres-specific features** (advanced window functions, recursive CTEs, specific transaction isolation semantics, extensions), verify against the prototype's documented coverage before committing to a migration.

---

## F. Parquet / Iceberg exports: optional optimization, not the foundation

This stack does not require Parquet/Iceberg to function as an HTAP foundation.

However, exporting to columnar formats is still useful when you explicitly want:

- backups and long-term retention
- cold storage / tiering
- very scan-heavy workloads where columnar storage provides materially better cost/performance
- interop with external tools that expect Iceberg/Parquet inputs

In those cases:

- Parquet/Iceberg are performance and lifecycle optimizations
- the authoritative, freshest, strongly-consistent view remains in the OLTP store

This is a meaningful distinction from architectures where Parquet/Iceberg IS the source of truth and the OLTP store is a cache.

---

## G. Consistency model: tunable where appropriate

Both the underlying store and the transaction layer support **tunable consistency** (including per-operation tuning).

Use this intentionally:

- **strict-serializable transactions** (via Accord) for correctness-critical invariants: financial transactions, inventory, auth, audit
- **weaker consistency** where latency/availability trade-offs are acceptable and correctness requirements permit: high-volume telemetry, analytics ingest, session data

The availability of tunable consistency does not mean "use it casually." Every weakening of consistency is a correctness assertion about what your application can tolerate. Document those assertions.

---

## H. Enterprise realities (and how to think about them)

Unified OLTP+analytics stacks must answer the same enterprise concerns any production data platform faces. These are the common ones and how we recommend thinking about them.

### 1. Operational maturity and support

Enterprises will ask:

- Who operates this at 2am?
- What are the failure modes and runbooks?
- How do upgrades, repairs, and incident response work?

**Recommendation**:

- Treat this repo as an evaluation harness, not a production blueprint
- Define an operational model: SRE ownership, on-call rotation, SLIs/SLOs, upgrade cadence, repair/compaction monitoring
- Budget for specialist skills (Cassandra, Accord, Spark bulk I/O) or commercial support contracts

### 2. Governance, security, and data access boundaries

Expect requirements for:

- centralized RBAC/ABAC
- auditing and lineage
- data masking / tokenization policies
- separation of duties and tenant isolation

**Recommendation**:

- Define the governance plane early, **including analytic access patterns**, the Spark Bulk Reader path reads SSTables directly, which has implications for row-level security that the OLTP path enforces
- Ensure consistent policy enforcement across OLTP and analytics interfaces

### 3. Schema evolution and migration

Enterprises need:

- safe schema change workflows
- backfills and reprocessing strategies
- compatibility guarantees across versions

**Recommendation**:

- Document the migration path for legacy SQL workloads: what works today, what is planned
- Accept that wide-column data modelling is different from relational modelling; some Postgres schemas will translate cleanly, others won't

### 4. CDC and integration (Kafka, streaming, dedup)

Enterprises typically require:

- reliable CDC to Kafka
- dedup semantics aligned with replication and failure handling
- clean operational experience for CDC pipelines

**Note**: the Sidecar is intended to provide CDC-to-Kafka with replication-factor-aware dedup semantics. This is expected to be added to the demo.

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

- clearly documented RPO/RTO
- failover/failback procedures
- proof that invariants hold during regional impairment

**Recommendation**:

- Include a DR drill guide and a failure-injection test plan
- Define how "availability" is preserved while respecting correctness
- Accord's leaderless design helps here, quorum loss in one region doesn't stop operations on keys whose quorum is elsewhere

---

## Hard Questions FAQ

### 1. Is this production ready / GA?

No. This repository is a proof-of-concept you can run today to evaluate the approach. Production readiness requires additional hardening, operational tooling, performance tuning, and: for most enterprises; a commercial support contract.

### 2. How can you claim strict serializability and still be "always-on available"?

Strict serializability requires coordination, and coordination requires quorum.

"Available" here means: **as long as a quorum can be reached for the keys involved in a transaction**, the transaction proceeds. Accord's leaderless design means availability is per-request, a down server affects only the transactions whose coordination path includes that server, not the cluster as a whole.

Under partitions or failures that prevent quorum for some keys, the system preserves correctness by blocking or degrading to slower coordination paths rather than returning inconsistent results. You trade tail latency for correctness during failure, and per-request availability for global availability. Both trade-offs are explicit and documented.

### 3. Is "no data duplication" realistic, or do we still need Parquet/Iceberg/lakes?

"No duplication" means you can run many analytics workloads **directly over persisted structures and snapshots** without requiring an always-on ETL copy.

You may still export to Parquet/Iceberg for explicit goals: cold storage, backups, scan-heavy workloads where columnar storage wins on cost/performance, or interop with external tools. Those exports are **optional optimizations**, not required plumbing. The distinction matters because it changes the ETL layer from mandatory infrastructure to a choice you make per-workload.

### 4. What are the trade-offs enterprises will actually feel?

In practice:

- **SQL feature coverage** — the Postgres-compatible interface is a protocol + dialect adapter, not full Postgres parity. Validate your required SQL semantics against what's actually implemented.
- **Query-path selection** — teams need to learn which access path fits which workload. The OLTP path is slower for wide scans than the bulk-read path; the bulk-read path has higher setup cost for small queries. Wrong-path queries will work but will surprise you on cost or latency.
- **Operational maturity** — Cassandra operations, Accord transaction debugging, Spark bulk I/O tuning are real specialist skills. Either build them in-house or contract commercial support.
- **Governance and CDC integration** — the technical capabilities are present; integrating them with existing enterprise governance and CDC platforms is implementation work.
- **Data modelling** — wide-column modelling is different from relational. Some schemas translate cleanly (time-series, event logs, key-value lookups); some don't (heavily normalized relational schemas with complex joins).

### 5. How does this compare to CockroachDB, TiDB, YugabyteDB, SingleStore, Snowflake Hybrid Tables, or Postgres + Citus?

Each takes a different bet on the HTAP problem:

- **CockroachDB, TiDB, YugabyteDB** — distributed SQL with strong relational semantics and varying consistency models. Excellent fit for application workloads that need relational integrity; analytical throughput on the same cluster is typically more constrained than this stack's bulk-reader approach.
- **SingleStore** — row + columnar hybrid in a single engine, strong analytical performance on fresh data, strong concurrency story for BI. Commercial licensing, closed-source core.
- **Snowflake Hybrid Tables / Unistore** — OLTP tables bolted onto an OLAP architecture. Good if you're already on Snowflake and want to reduce round-trips; the OLTP latency profile, concurrency model, and per-node scaling differ from a distributed-database-first approach.
- **Postgres + Citus** — horizontal sharding of Postgres with good distributed-query support. Excellent if you have Postgres expertise already; strict-serializable distributed transactions across shards are bounded differently.

**This stack's bet**: start with a distributed OLTP store that already scales horizontally for write-heavy workloads, add strict-serializable transactions via Accord, and read analytics directly from persisted storage via the bulk reader.

Wins on: write scaling, OLAP throughput per node, per-request availability under failure.
Loses on: relational feature richness (compared to distributed-SQL databases), managed-service maturity (compared to Snowflake), operational familiarity in shops that already know Postgres.

### 6. Is this platform linearly scalable?

For the documented workload shapes: point reads, bounded partition reads, and write ingest; yes, scaling is linear across documented cluster sizes. Cassandra's scaling story at this layer is well-established.

Token-range scans and cross-partition transactions have different scaling characteristics and are bounded by coordination overhead rather than node count. Wide-scan analytical throughput scales with disk I/O bandwidth per node times node count (which is why the per-node throughput number in the TCO doc matters more than the cluster total).

See the [TCO worksheet](TCO-Comparisons.md) for specific workload/cluster-size examples.

---

## References

- Accord / CEP-15 :: transactions, strict serializability, failure tolerance goals
- CEP-28 :: Spark bulk reader/writer via Sidecar to persisted storage
- Cassandra Analytics :: bulk reader/writer examples
- SQL prototype repo :: Postgres wire protocol + Calcite-based dialect coverage
