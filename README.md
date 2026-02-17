🏗️🚧👷👷🏽‍👷‍️🚧  see todos inline (also see mck/drone-data-model branch for new data model coming)

# Vendor-Neutral Open Source HTAP Data Platform

_Proof of Concept that takes 4 minutes to demonstrate_  
  An enterprise grade Hybrid Transactional Analytical Processing (HTAP) Data Platform

  _with the following key characteristics_
- Record of Truth: simplifies data governance, security, catalogue and schema management
  - OLTP store with high concurrency + horizontal scaling 
  - Strict-serializable ACID transactions
  - Different SQL interfaces: OLAP SparkSQL & PrestoDB, OLTP Postgres wire-protocol + dialect (subset PoC),
  - OLAP resource isolation without data duplication from OLTP
- Ready for today's Agentic AI demands
- Ecosystem integration: Apache Kafka, Apache Spark, PrestoDB, Apache Parquet, Apache Iceberg
- Freedom to Operate: built with commoditized software not controlled by vendors, that can be deployed anywhere
- 80%+ Lower Total Cost of Ownership [compared](TCO-Comparisons.md) to common OLTP + ETL + OLAP solutions

---
1. [Demo Quick Start](#demo-quick-start)  
2. [Vision Statement (the "why")](#why-just-one-data-platform)  
3. [Appendix: Scope, Architecture Notes, and Enterprise Considerations](#appendix-scope-architecture-notes-and-enterprise-considerations)  
4. [FAQ (hard question and direct answers)](#hard-questions-faq-direct-answers)   
---

## Demo Quick Start
  
##### Bring up the whole stack in under 4 minutes, and start ingesting event data.

```shell
podman compose -f podman-compose.yml up
```

  <br>

##### Example CQL Queries (plumbing)

```sql
podman exec cassandra \
 cqlsh cassandra -e "SELECT * FROM demo.events LIMIT 3;"
```
TODO– quick explaination of data model used

  <br>

##### Example **Presto Queries**

```shell
podman exec presto \
 presto-cli --execute "SHOW SCHEMAS FROM cassandra;"

podman exec presto \
 presto-cli --execute "SELECT * FROM cassandra.demo.events LIMIT 100;"

podman exec presto \
 presto-cli --execute "SELECT event_type, COUNT(*) FROM cassandra.demo.events GROUP BY event_type;"
```
To watch query progress in browser, open http://localhost:8088/ui/  

  <br>

##### Example per-partition **SparkSQL Query**

A simple query using the Cassandra-Spark-Connector (requires creating a temp view first)
```shell
podman exec spark \
 spark-sql --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  --conf spark.cassandra.connection.host=cassandra
```
```sql
  CREATE TEMPORARY VIEW events_for_partition_queries USING org.apache.spark.sql.cassandra
  OPTIONS (keyspace 'demo', table 'events');

  SELECT * FROM events_for_partition_queries LIMIT 3;"
```
The `spark-cassandra-connector` is best for per-partition (or per-index) queries.  Reads and writes go through Cassandra's CQL interface and its jvm.

  
  <br>

##### Example bulk (direct to sstable file) **SparkSQL Query**

Queries using the Cassandra Spark Bulk Reader (via the Cassandra Sidecar)
```shell
podman exec -it spark \
 spark-sql \
  --packages org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.3.0,org.apache.cassandra:analytics-sidecar-vertx-client-all:0.3.0,org.apache.cassandra:cassandra-bridge_spark3_2.12:0.3.0 \
```
```sql
CREATE OR REPLACE TEMP VIEW events_for_bulk_queries
USING org.apache.cassandra.spark.sparksql.CassandraDataSource
OPTIONS (
  sidecar_contact_points "cassandra",
  keyspace "demo",
  table "events",
  DC "datacenter1",
  createSnapshot "true",
  snapshotName "htap_demo_sparksql",
  numCores "4"
);

SELECT count(*) FROM events_for_bulk_queries;

SELECT event_type, COUNT(*) AS cnt, MIN(event_time) AS first_seen, MAX(event_time) AS last_seen
  FROM events_for_wide_queries GROUP BY event_type ORDER BY cnt DESC LIMIT 10;
```
To watch query progress in browser, open http://localhost:4040/

The Cassandra Bulk Reader/Writer interfaces directly to the data directory disks of the Cassandra nodes.  Reads are direct to a snapshot of the sstables files directly (off disk), providing point-in-time consistency.  Direct file access provides high throughput of bulk or analytics-style read and writes that does not impact latencies of other requests to the Cassandra cluster.


  <br>

##### Dump the whole database to Parquet

To write to a single Parquet file, continue on from the `spark-sql` example above:
```sql
INSERT OVERWRITE DIRECTORY '/var/lib/cassandra/parquet-exports/demo_events' USING parquet
SELECT /*+ COALESCE(1) */ * FROM events_for_bulk_queries;
```
This writes the entire `demo.events` table to a single Parquet file in the `cassandra-data` directory. The `coalesce(1)` reduces all partitions to one before writing, resulting in a single output file.

  <br>

##### Move Parquet files quickly into the database

FIXME: currently broken w/ 'DecoratedKey… not serializable result: java.nio.HeapByteBuffer'
```shell
podman exec -it spark \
 spark-shell  \
  --packages org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.3.0,org.apache.cassandra:analytics-sidecar-vertx-client-all:0.3.0,org.apache.cassandra:cassandra-bridge_spark3_2.12:0.3.0   \
```
```scala
val df = spark.read.parquet("/var/lib/cassandra/parquet-exports/demo_events")

df.write
  .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
  .option("sidecar_contact_points", "cassandra")
  .option("keyspace", "demo")
  .option("table", "events")
  .option("DC", "datacenter1")
  .option("numCores", "4")
  .mode("append")
  .save()
```
This writes SSTables directly to disk and uses the Sidecar to load them into Cassandra, bypassing the CQL layer for maximum throughput. 

  <br>

##### CDC (Change Data Capture) to Kafka
Simple configuration to CDC all writes into the database into a Kafka topic
```
todo
```

  <br>

##### Example **Accord Transactions**  
TODO–  see `mck/cassandra-6` branch

  <br>
  
##### Example Application (OLTP) SQL
TODO–  see `mck/cassandra-6` branch

---

# Why: The Benefits of One interoperable Data Platform

One Data Platform for all your needs, like the good ol' days of RDMS but no longer the monolith.

In details, the reasons to choose this stack are broken down into the following:  
• [Architecture – Keep It Simple, Keep It Fast, Keep It Consistent](#keeping-it-simple-keeping-it-fast-keeping-it-consistent)  
• [AI data platform demands](#ai-data-platform-demands)  
• [Vector Similarity Search](#vector-similarity-search)  
• [ACID Guarantees](#acid-guarantees)  
• [SQL Compatibility](#sql-compatibility)  
• [Change Data Capture Kafka Streaming](#change-data-capture-kafka-streaming)  
• [Why PrestoDB and Apache Spark](#why-prestodb-and-apache-spark)  
• [Why Podman](#why-podman)  
• [Common (Bad) Assumptions: YAGNI, Scale, and the OLAP Platform](#common-bad-assumptions-yagni-scale-and-the-olap-platform)  
• [Appendix: Scope, Architecture Notes, and Enterprise Considerations](#appendix-scope-architecture-notes-and-enterprise-considerations)  
• [Hard Questions FAQ (direct answers)](#hard-questions-faq-direct-answers)  
• [References](#references)  

And, for an illustration of the material reductions in recurring spend on licenses and Saas every month see [TCO-Comparisons.md](TCO-Comparisons.md).

## Architecture – Keep It Simple, Keep It Fast, Keep It Consistent

Build a unified data platform that can consolidate application/transactional (OLTP), Analytics (OLAP), machine learning and generative AI, workloads in one platform.

Choose a data platform stack that serves different consumption patterns and modalities while keeping the data in place: one record of truth.

This avoids 3 common problems that evolve:
1. Data duplication and inconsistency
2. ETLs and pipelines that are hard to maintain and slow to run
3. Multiple databases that are hard to manage, catalogue, schema version, secure, govern
4. Explosion of technology portfolio, operational and software costs
3. Siloing of data efforts through the enterprise

The stack demostrates a set of standard technologies put together that is as simple to setup as any single database, and gives you all the same functionality, while future-proofing for AI, ML and Analytics.  

Following the principles of _record of truth_ and _compute storage separation_, the AI, ML and Analytics are compute workloads without separate storage.

By providing direct access to the data, but different access paths for different compute workloads (OLTP, OLAP, ML, AI) to ensure resource management and isolation the data platform is fast and efficient, best serving the SLO needs for each workload (latency or throughput).

Application workloads (OLTP) can expect p99 latencies for writes under 5ms and reads under 50ms, regardless of throughput or concurrency.

## AI Data Platform demands

AI agents need real-time access to all data sources in an enterprise, with high throughput, strong consistency and low latency, without bypassing security and governance controls.

Today's AI progress is outpacing data readiness. There is no AI without Data– any enterprise's potential with AI agents fundamentally depends on modernisation efforts of the data platform.

The Mobile-First era (~2007) exposed the systems-design mess—and resulting limitations—created by duplicating data stores as data volumes exploded. Many organizations no longer revisit why certain splits were originally necessary, that for example the underlying reason for the denormalisation and duplication of data: from transactional databases to search indexes, and from application OLTP databases to analytical OLAP data lakes; is because of industry technical constraints.  But 20 years later we still often choose technologies based solely on comparisons within silos and bounded contexts, forgetting, or unable, to re-evalute if past systems designs limitations still apply.  These architectures are slow to be revisited once institutionalized and has led to an explosion in enterprise-wide complexity, limitations, and rapidly increasing costs.

Agentic AI gives us an opportunity to avoid repeating the mistakes of the mobile-first era and to build a data platform that is scalable, consistent, and available—while maintaining low latency, high throughput, and real-time access to the enterprise data catalogue. This also enables centralised, real-time governance and security across the full catalogue.

## Vector Similarity Search

TODO

## ACID Guarantees

Many OLTP databases provide ACID semantics, but Serializability is rare and **Strict Serializability is rarer** .

As load increases and storage becomes inherently distributed, transactional guarantees matter more. Serializable isolation is rare in production, while Strict Serializability raises the requirements higher. Durability must now also account for single points of failure and multiple simultaneous catastrophic hardware failures, something single (writer) databases fail to do.

## SQL Compatibility

SQL—and particularly the Postgres dialect—is a standard skill relied upon by both developers and data scientists.

SQL plays a valuable role early in the application lifecycle, when the domain model and schema change frequently, as well as for data exploration and analysis. While persisting to disk a rigid logical “SQL schema” is an unnecessary performance overhead for applications with prepared and static access patterns, the logical SQL view often simplifies work for both developers and data scientists.

SQL is an interface layer; in this stack it is implemented as a protocol/dialect adapter over the transactional layer. SQL can be implemented on top of many storage engines, using just transactions and an underlying key-value store. This HTAP can easily add Document, Graph and other modalities.

This stack demonstrates how different SQL implementations deliver value for varying throughput, latency, and domain requirements. The stack offers different SQL interfaces for: direct application SQL (a PoC Postgres-shim), and production-ready analytical SQL in both: partition-based  (Spark/Presto) and file-direct analytical SQL (Spark/Iceberg/Presto).  These different SQL interfaces make it possible to work with and migrate legacy platforms where federation of exisiting data sources is required.

## Change Data Capture Kafka Streaming
TODO

## Why PrestoDB and Apache Spark
TODO

## Why Podman

The demo uses Podman as the open source alternative to Docker.  It is still compatible with Docker, so you are free to still use Docker.

## Common (Bad) Assumptions: YAGNI, Scale, and the OLAP Platform

**Myth:** "YAGNI: You don't need to design your database for scale. Redesign for scale later when needed" 

**Reality:** The cost of scaling later is significantly higher than the cost of scaling now, and left too late becomes a business killer.  The cost of redesigning critical foundational data systems across an enterprise late takes years, and leaves technical debt in perpituity.  Beyond the cost opportunity as business growth is stalled, the cost of scaling late incurs business growth limitations, ir not stagnation, and can lead to the business failure.  The cost of scaling now is a sunk cost, with a sunk trade-off primarily in learning new technologies and skills.

**Myth:** "Analytics is a separate responsibility and platform" 

**Reality:** This assumption often emerges from application-team constraints. Compounded by a variant of the previous myth of "we tackle the analytics needs later when needed".  It results in businesses with database sprawl debt and limited engineering competence and application in an isolated silo where data scientists struggle with stale, fragmented and poor quality data on inefficient insufficient solutions leading to excessive manual labour and limited results.  Data Mesh makes an attempt to address this in principle but the available tactics today (data fabrics and data products) fail to address the most consequential limitations that can easily and should be addressed, when done early.  

The era of Agentic AI forces us to the reality that the data consumption patterns typical of analytical computations is now a critical part of the business, indistinguishable and inseparatable from the transactional application stack, and enterprise data platforms must now be designed accordingly, ideally from the beginning.  Developers no longer get to pass it off as not their responsibility.

---

## Appendix: Scope, Architecture Notes, and Enterprise Considerations

This repository is a **proof-of-concept HTAP stack** intended to be easy to run and evaluate. It demonstrates a unified pattern for:
- high-concurrency OLTP ingestion and point/bounded reads
- strict-serializable ACID transactions (transactional tables / operations)
- analytics that can operate over persisted structures (including snapshots) without requiring an always-on ETL pipeline

> Status: **Not GA**. Available to test in the demo today. Production hardening, UX/debuggability, and performance tuning are in progress.

---

### A. What this demo is (and is not)

#### What it is
- A runnable POC showing end-to-end ingestion + query paths across OLTP and analytics.
- A demonstration of **global strict serializability** for multi-key and multi-table transactions under the constraints described below.
- A demonstration of analytics reads that can be executed against persisted structures (including snapshots) to reduce OLTP interference.

#### What it is not (yet)
- A claim of full feature parity with other mature, general-purpose SQL databases.
- A promise that every enterprise workload can be consolidated immediately without trade-offs.
- A turnkey “drop-in replacement” for warehouses/lakes in every scenario (some organizations will still choose to export to Parquet/Iceberg for cost/performance/lifecycle reasons).

---

### B. How strict serializability is achieved (and what availability means)

#### Where strict serializability comes from
Strict serializability is provided by **Accord (CEP-15)**, included in this stack. The intention is:
- strict-serializable isolation
- low latency in the common case (single wide-area round trip under normal conditions)
- leaderless operation without introducing a global bottleneck

#### Availability model (practical interpretation)
This system is **quorum-based** for transactional decisions:
- If a quorum can be reached, the system remains available for those operations.
- If a quorum cannot be reached due to failures or partitions, the system preserves correctness by blocking progress rather than returning inconsistent results.

A practical rule of thumb:
- With replication factor (RF) = 3, you can typically lose 1 replica per key-range and still make progress.
- With higher RF (and appropriate placement), you can tolerate more failures, but “how many can be down” depends on the quorum configuration and failure domain (racks / AZs / regions).

#### “No rollback” (what is meant here)
Accord is designed so that transactions do not need to “roll back” in the traditional sense of “speculatively applied state that must be undone.”

Instead, transactions are journaled/ordered and may be **blocked** (or forced onto a slower path) when conflicts or failures require coordination. This can trade tail latency for correctness and availability under failure, and creates an simpler application development experience.

---

### C. HTAP analytics without duplicating data: persisted-structure reads + snapshots

#### What “without duplicating data” means here
The goal is to avoid a permanent architectural requirement to:
- copy OLTP data into a second system (warehouse/lake)
- maintain continuous ETL just to make analytics possible

Instead, the stack supports analytics that can read:
- directly from persisted on-disk structures (SSTable-oriented bulk read)
- from coordinated cluster snapshots of those structures

This is why the demo emphasizes:
- **Spark bulk reader/writer** paths that can interact with persisted structures via Sidecar endpoints
- snapshot-coordinated reads for consistent analytical views

#### Snapshot-coordinated analytics (demo behavior)
In the demo, “snapshotting” refers to:
1) coordinating snapshots across the cluster
2) running analytic queries over the snapshotted persisted structures

This yields a stable analytic view while allowing OLTP to continue without disruption (i.e. resource isolation).

---

### D. Resource isolation: protecting OLTP tail latency

#### Isolation strategy
The demo’s design intent is:
- OLTP uses the normal request path
- analytics uses persisted-structure reads and/or snapshot reads
- a Sidecar can stream/offload snapshot files to separate storage to reduce repeated I/O contention

The intended outcome is:
- minimal impact to OLTP p99 latency under analytic load, provided the system is configured correctly

### Query shapes: when to use which path
- **Point reads / bounded partition reads**: OLTP path
- **Per-partition analytics**: either OLTP path or persisted-structure reads depending on concurrency and SLAs
- **Wide scans / large token-range reads**: persisted-structure reads are typically preferred

Note: OLTP wide scans (token-range queries) will generally have higher p99 latency than partition reads. They can still be “fast,” but the analytics path is designed to be the better tool for that job.

---

### E. SQL interface: what “Postgres-compatible” means in this repo

#### What is provided
The SQL interface in this stack is a **Postgres wire-protocol + Postgres dialect adapter** implemented as a prototype on top of the transaction layer using Apache Calcite.

#### What you should assume (important)
- This is **not** a claim of full Postgres feature parity.
- The prototype’s GitHub page documents exactly what is implemented and what is not.
- Treat it as a pragmatic interoperability layer:
  - helpful for onboarding, tooling compatibility, and incremental migration
  - not a substitute for validating required SQL semantics for your application

---

### F. Parquet / Iceberg exports: optional optimization, not the foundation

This stack does not require Parquet/Iceberg to function as an HTAP foundation.

However, exporting to columnar formats can still be useful when you explicitly want:
- backups and long-term retention
- cold storage / tiering
- very scan-heavy workloads where columnar storage provides better cost/performance

In those cases:
- Parquet/Iceberg are performance and lifecycle optimizations
- the authoritative, freshest, strongly consistent view remains in the OLTP store

---

### G. Consistency model: tunable where appropriate

Both the underlying store and the transaction layer support **tunable consistency** (including per-operation tuning).

Use this intentionally:
- strict-serializable transactions for correctness-critical invariants
- weaker consistency where latency/availability tradeoffs are acceptable and correctness requirements permit

---

### H. Enterprise realities (and how to think about them)

Below are the common enterprise concerns that unified OLTP+analytics stacks must address.

#### 1) Operational maturity and support
Enterprises will ask:
- Who operates this at 2am?
- What are the failure modes and runbooks?
- How do upgrades, repairs, and incident response work?

Recommendation:
- treat this repo as an evaluation harness
- define an operational model (SRE ownership, oncall, SLIs/SLOs, upgrade cadence)

#### 2) Governance, security, and data access boundaries
Expect requirements for:
- centralized RBAC/ABAC
- auditing and lineage
- data masking / tokenization policies
- separation of duties and tenant isolation

Recommendation:
- define the governance plane early, including analytic access patterns
- ensure consistent policy enforcement across OLTP + analytics interfaces

#### 3) Schema evolution and migration
Enterprises need:
- safe schema change workflows
- backfills and reprocessing strategies
- compatibility guarantees across versions

Recommendation:
- document the migration path for legacy SQL workloads (what works today, what is planned)

#### 4) CDC and integration (Kafka, streaming, and dedup)
Enterprises typically require:
- reliable CDC to Kafka
- dedup semantics aligned with replication and failure handling
- a clean operational experience for CDC pipelines

Note:
- The Sidecar is intended to provide CDC-to-Kafka with replication-factor-aware dedup semantics.
- This is expected to be added to the demo.

#### 5) Analytics expectations and BI tooling
Teams will ask:
- What is the concurrency model for analytics?
- What are the limits for joins, aggregations, and scans?
- Which tools work out of the box?

Recommendation:
- set expectations clearly: “supported query shapes” + “best path per shape”
- provide example workloads and reproducible benchmarks

### 6) Disaster recovery and multi-region
Enterprises will require:
- clearly documented RPO/RTO
- failover/failback procedures
- proof that invariants hold during regional impairment

Recommendation:
- include a DR drill guide and a failure-injection test plan
- define how “availability” is preserved while respecting correctness

---

## Hard Questions FAQ (direct answers)

### 1) Is this production ready / GA?
No. This repository is a proof-of-concept you can run today to evaluate the approach. Production readiness requires additional hardening, operational tooling, and performance tuning.

### 2) How can you claim strict serializability and still be “always-on available”?
Strict serializability requires coordination. Availability here means: **as long as a quorum can be reached**, transactions and reads that require strict guarantees can proceed. The leaderless design of the Accord protocol means availability is per request, meaning down server only impact the limited set of requests. Under partitions or failures that prevent quorum, the system preserves correctness by blocking or degrading to slower coordination paths rather than returning inconsistent results.

### 3) Is “no data duplication” realistic, or do we still need Parquet/Iceberg/lakes?
“No duplication” means you can run many analytics workloads **directly over persisted structures and snapshots** without requiring an always-on ETL copy. You may still export to Parquet/Iceberg for explicit goals like cold storage, backups, or cost/performance optimization for scan-heavy workloads. Those exports are optional optimizations, not required plumbing.

### 4) What are the tradeoffs enterprises will actually feel?
The main tradeoffs are:
- validating SQL feature coverage vs relying on “Postgres-compatible” branding
- choosing the correct query path for each workload shape (point reads vs scans)
- operational maturity (observability, runbooks, upgrades, DR drills)
- governance and CDC integration details (often the real enterprise blockers)

### 6) Is this platform always linearly scalable?
Yes.
TODO

---

## References
- Accord / CEP-15 (transactions, strict serializability, failure tolerance goals)
- CEP-28 (Spark bulk reader/writer via Sidecar to persisted storage)
- Cassandra Analytics (bulk reader/writer examples)
- SQL prototype repo (Postgres wire protocol + Calcite-based dialect coverage)

---
