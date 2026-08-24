# Why: The argument for one interoperable data platform

One data platform for all your needs, like the good old days of the relational database management system, but no longer a monolith.

This document is the argument for the approach. &emsp;The [README](../README.md) covers the quickstart; [ARCHITECTURE.md](ARCHITECTURE.md) covers the technical scope; [TCO-Comparisons.md](TCO-Comparisons.md) covers the money.

## Contents

- [The inherited architecture](#the-inherited-architecture)
- [What actually changed](#what-actually-changed)
- [Why AI workloads expose the dual-system cost](#why-ai-workloads-expose-the-dual-system-cost)
- [ACID guarantees at scale](#acid-guarantees-at-scale)
- [SQL compatibility, honestly](#sql-compatibility-honestly)
- [Vector similarity search: a compounding case against separation](#vector-similarity-search-a-compounding-case-against-separation)
- [Why Presto and Apache Spark: different analytical access patterns, one shared dataset](#why-presto-and-apache-spark-different-analytical-access-patterns-one-shared-dataset)
- [Broken opinions](#broken-opinions)
- [What to take from this](#what-to-take-from-this)

---

## The inherited architecture

Your application database handles writes. &emsp;Your data warehouse handles analytics. &emsp;Your extract-transform-load (ETL) pipelines connect them. &emsp;Your data scientists complain about stale data. &emsp;Your platform team owns three systems and four schemas, and carries a Friday incident rotation.

None of this was inevitable. &emsp;It was a workaround for technical limitations that no longer apply. &emsp;We just forgot.

The split between online transaction processing (OLTP) and online analytical processing (OLAP) made sense when RAM was expensive, consensus protocols were slow, and columnar scans blocked the write path. &emsp;Those constraints have shifted quietly over the last five to ten years. &emsp;The architecture most of us inherited now costs more than it earns, and the cost is compounding, because every new data modality (search indexes, vector stores, feature stores, cache tiers) gets bolted on as yet another duplicated copy behind yet another pipeline.

This is not a new problem. &emsp;The industry has been aware of the duplication tax for at least fifteen years. &emsp;What is new is that the technical foundations for solving it properly all landed within roughly the same five-year window: distributed consensus protocols with single-round-trip latency, columnar scans that do not contend with write paths, and commodity hardware fast enough to serve both workloads from one dataset. &emsp;The tools caught up with the problem, and most enterprise architectures have not yet noticed.

---

## What actually changed

Three specific shifts made the unified approach viable:

**1. Consensus protocols got faster.** Paxos and Raft require multiple round trips and have a leader bottleneck. &emsp;EPaxos improved on this. &emsp;Accord, Cassandra Enhancement Proposal 15 (CEP-15), provides strict-serializable distributed transactions in a single wide-area round trip, leaderless, using commodity clocks: the same isolation class Google Spanner offers, without Spanner's TrueTime commit-wait. &emsp;The cost of a distributed transaction dropped from "multiple round trips plus clock wait" to "one round trip plus quorum."

**2. Storage-tiered compute got normal.** Compute/storage separation, treating analytics as a set of compute workloads that share the same persisted data, moved from specialist territory to well-understood architecture. &emsp;The idea that OLAP should have its own storage tier is itself an artifact of the old constraints.

**3. Direct-to-storage analytical paths got built.** The Cassandra Spark Bulk Reader (CEP-28) reads SSTables directly from disk via snapshots, bypassing the OLTP request path entirely. &emsp;This eliminates the contention that traditionally forced OLAP into a separate system. &emsp;Analytics read the same storage the OLTP path writes to, without fighting it.

Individually these are incremental improvements. &emsp;Composed, they remove the technical justification for the dual-system architecture. &emsp;What remains is inertia.

---

## Why AI workloads expose the dual-system cost

AI agents and retrieval-augmented systems share one access pattern with traditional applications and one with analytics:

- **Like applications**, they need low-latency point lookups and strong consistency. &emsp;Stale data produces wrong answers. &emsp;There is no user tolerance for ETL lag between "data written" and "data retrievable by an agent."
- **Like analytics**, they need full-dataset scans for embedding generation, feature extraction, aggregation, and retraining.

In a traditional OLTP + warehouse architecture, AI workloads are forced to straddle both systems. &emsp;Teams end up either:

- running AI retrieval against stale warehouse copies (wrong answers, confidently delivered), or
- building yet another specialised store (vector DB, feature store, cache tier) with its own ETL pipeline, which compounds the duplication problem the warehouse was supposed to solve.

The second path is dominant today, which is why enterprise data-platform spend is rising faster than enterprise data-platform value. &emsp;Every new AI initiative adds infrastructure, and every piece of infrastructure needs a pipeline to keep it in sync with the system of record.

A unified hybrid transactional/analytical processing (HTAP) store collapses this. &emsp;The same record of truth serves application queries, vector similarity search, analytical scans for feature generation, and change data capture (CDC) to downstream machine-learning pipelines. &emsp;Governance is centralised by construction rather than federated across multiple systems.

**This problem is not novel to AI.** The same argument applied to real-time personalisation, fraud detection, and operational analytics for the last decade. &emsp;AI workloads just make the cost of the duplicated architecture more visible, because they exercise both access patterns simultaneously and they run hot enough that staleness becomes customer-visible.

Developers no longer get to pass analytics off as somebody else's problem. &emsp;The data consumption patterns typical of analytical computation are now indistinguishable from the transactional application stack. &emsp;If your platform architecture doesn't reflect that, it will be the thing that holds your AI initiatives back, not the models.

---

## ACID guarantees at scale

Many OLTP databases provide atomicity, consistency, isolation and durability (ACID). &emsp;**Serializability is rare**. &emsp;**Strict serializability is rarer still**.

As load increases and storage becomes inherently distributed, transactional guarantees matter more, not less. &emsp;Single-writer databases scale by careful partitioning; cross-partition transactions remain expensive or simply unavailable. &emsp;Serializable isolation in production is already the exception. &emsp;Strict serializability (the property that transactions appear to execute atomically at a single point in real time, globally) is offered by a handful of systems.

Why it matters: durability has to account for single points of failure and multiple simultaneous hardware failures. &emsp;Systems that rely on a single writer or a single leader fail this test at scale. &emsp;Leaderless strict-serializable consensus (Accord, Spanner) is the architectural answer; everything else is a trade-off against one of correctness, availability, or latency under failure.

This stack offers strict-serializable ACID across the entire data platform via Accord. &emsp;That claim is defensible, bounded, and testable. &emsp;See [ARCHITECTURE.md](ARCHITECTURE.md#b-how-strict-serializability-is-achieved-and-what-availability-means) for the mechanism and the bounds.

---

## SQL compatibility, honestly

SQL, particularly the Postgres dialect, is the lingua franca of developers and data analysts. &emsp;It plays a valuable role early in the application lifecycle (when the domain model and schema change frequently) and throughout the lifecycle for data exploration, reporting, and business intelligence (BI) tooling.

**A trade-off worth naming**: for applications with static, prepared access patterns, persisting a rigid relational schema to disk carries overhead that a wide-column or key-value layout does not. &emsp;Whether that overhead matters depends on your workload: it is nearly invisible for many transactional workloads and meaningful for high-throughput write paths. &emsp;This is why Cassandra's data model is what it is.

SQL, however, is an **interface layer**. &emsp;It does not dictate storage. &emsp;SQL can be implemented on top of many storage engines given transactions and a key-value store, and the same mechanism extends to document, graph, and other modalities. &emsp;The design puts application SQL on the transaction layer as a Postgres wire-protocol and dialect adapter, using Apache Calcite, beside the analytical SQL that Spark and Presto give.

This stack runs two of those three SQL interfaces against one data store, and the third is designed but not built:

- **Partition-based analytical SQL** (Spark / Presto via Cassandra connector): for targeted analytical queries on known partitions
- **File-direct analytical SQL** (Spark Bulk Reader, and cqlite over the live files): for wide scans and bulk analytics
- **Application SQL** (Accord SQL, the Postgres adapter): deferred. &emsp;It needs Accord, and Accord is here: the stack runs Cassandra 6.0-alpha2, which ships it and refuses a transaction only because no table declares `transactional_mode`.&emsp;The adapter itself is what remains

Different SQL interfaces for different access patterns. &emsp;Federation with existing data sources becomes an integration problem rather than an architectural one.

**What this does not claim**: full Postgres parity. &emsp;The wire protocol and dialect adapter implement a subset. &emsp;Validate against your application's actual SQL requirements; don't assume "Postgres-compatible" means "all of Postgres."

---

## Vector similarity search: a compounding case against separation

Vector search is where dual-system architecture fails most visibly, and fails fastest. &emsp;Retrieval-augmented generation, semantic search, recommendation systems, and agentic retrieval all depend on approximate nearest-neighbour (ANN) search over high-dimensional embeddings. &emsp;The default industry response has been to stand up a dedicated vector database (Pinecone, Weaviate, Milvus, Qdrant) alongside the existing OLTP and OLAP stores. &emsp;This is the third system bolted onto what was already a dual-system architecture, and it arrives with a pipeline, a governance surface and a consistency problem of its own.

### The consistency problem, made concrete

Embeddings are derived data. &emsp;Every document, product description, customer interaction, or support ticket that gets embedded must first exist in the system of record, then be read, transformed by an embedding model, and written to the vector store. &emsp;That pipeline has the same properties as every other ETL pipeline: it adds latency, it can fail, it introduces staleness, and it creates a second source of truth that must be reconciled with the first.

The consistency problem that emerges is subtle and consequential. &emsp;When a row in the source system changes (a product description is updated, a document revised, a customer record corrected), the embedding derived from that row is now stale. &emsp;The vector store returns results based on the old embedding until the pipeline catches up.

In retrieval-augmented generation this is a correctness failure: the agent retrieves content that no longer matches the source of truth, and then confidently generates text from it. &emsp;The user sees an answer grounded in yesterday's version of the data.

Most teams underestimate the rate of change in their source data, discover this gap in production, and respond by building a second CDC pipeline to keep the vector store in sync. &emsp;The pipeline-to-manage-the-pipeline problem. &emsp;Each added consumer (search index, cache tier, feature store) compounds the reconciliation burden.

**The architectural alternative: vector indexes live in the system of record, alongside the source data they index.** A write to the source row updates the vector index through the same code path, with the same consistency guarantees, under the same transactional semantics. &emsp;There is no pipeline to fail and no reconciliation to perform, so there is no staleness to explain to end users. &emsp;Governance for the vector column is the governance that already applies to the source data, rather than a second set of policies federated across a second system.

### Hybrid search: the structural advantage of co-located vectors and data

The consistency argument is necessary but not sufficient. &emsp;The stronger argument is hybrid search: queries that combine vector similarity with structured predicates.

Real retrieval workloads are almost never pure vector similarity. &emsp;They look like this:

> "Find the 10 most similar support tickets to this query, but only from this customer, only in the last 30 days, and only where the status is 'resolved' and the product line is 'database'."

In a separated vector store, that query is architecturally broken. &emsp;You either:

- **filter-then-search:** query the OLTP store for matching rows, extract their IDs, send them to the vector store as a filter parameter, which is latency-expensive and often bumps into vector-store limits on filter-list size; or
- **search-then-filter:** ask the vector store for top-K similar items, then go back to the OLTP store to re-filter, which produces incomplete or wrong results when the filtered-out items were precisely the ones you wanted (the "blocked graph traversal" problem documented in comparative literature on HNSW-based systems with filtering); or
- **post-filter:** ask the vector store for a much larger top-K (say, 1000) and filter in the application tier, which wastes compute, increases latency, and still silently drops results when the true matches fall outside the oversample window.

Each approach is a workaround for the architectural mismatch of storing the structured data in one system and the vectors in another.

**Co-located vectors and data solve this directly.** In Cassandra 5.0, a vector column is just another column on an existing table. &emsp;The vector index is a Storage-Attached Index (SAI), the same indexing infrastructure that supports scalar columns, and it lives alongside scalar indexes on the same table. &emsp;A hybrid query is written natively in CQL:
```sql
SELECT * FROM support_tickets
WHERE customer_id = ?
  AND status = 'resolved'
  AND product_line = 'database'
  AND created_at > '2025-10-01'
ORDER BY query_vector ANN OF ?
LIMIT 10;
```

The SAI query engine resolves scalar predicates and the vector predicate through the same execution path, against the same storage engine, at the same consistency level. &emsp;There is no cross-system hop and no oversample-and-filter workaround, so no consistency gap opens between the structured filters and the vector similarity result. &emsp;Hybrid search is a first-class access pattern rather than an architectural afterthought.

### Schema evolution: adding vectors without ceremony

The dual-system architecture treats "adding vector search" as an infrastructure project. &emsp;A new service must be provisioned, a new pipeline built, a new governance boundary drawn, new operational runbooks written. &emsp;For many enterprises this takes months and meaningful capital expense before the first retrieval query runs.

In the co-located architecture, adding vector search to an existing table is an `ALTER` statement and a `CREATE INDEX`:

```sql
ALTER TABLE documents ADD content_vector VECTOR<FLOAT, 1536>;
CREATE INDEX ON documents(content_vector) USING 'sai';
```

This is important in ways that are easy to understate. &emsp;There is no cluster-wide schema-change lock and no stop-the-world propagation ceremony: Cassandra's schema changes are eventually consistent and gossip-propagated, designed to be tolerable in a live production cluster. &emsp;There is no need to pre-provision spare capacity for a new service, because the vector index storage grows with the existing table's storage, using the nodes, replication factor and disk budget it already has. &emsp;There is no cross-team coordination with a platform group to stand up new infrastructure.

SAI itself was designed with this property in mind: it is fully compatible with zero-copy streaming, so bootstrapping or decommissioning nodes streams indexes along with SSTables rather than rebuilding them. &emsp;Schema evolution for indexing, historically one of the most painful operational events in a database's life, becomes routine.

For teams whose existing tables already hold the data they need to embed, adding vector search is a day's work, not a quarter's project. &emsp;That velocity difference compounds: it changes which vector-search use cases are worth attempting.

### JVector and DiskANN: why relevancy holds as datasets exceed memory

The architectural arguments above hold for any database that co-locates vectors with source data. &emsp;The performance argument is more specific, and it matters because it determines whether the architecture is viable at enterprise scale.

The standard in-memory ANN algorithm, HNSW (Hierarchical Navigable Small World), is fast and accurate so long as the index fits in RAM. &emsp;At roughly 1.3× the raw vector size in memory (for the index plus full-precision vectors), HNSW achieves recall@10 above 0.95 and latencies in the single-digit milliseconds. &emsp;This is the happy path most vector-database benchmarks operate in.

The problem: **HNSW's performance falls off a cliff when the index exceeds available memory**. &emsp;At 100 million 1536-dimensional vectors, the in-memory footprint runs into hundreds of gigabytes. &emsp;Either you provision machines large enough to hold the entire working set in RAM (expensive, and a scaling ceiling rather than a scaling strategy), or HNSW's access patterns begin hitting disk, and because HNSW's graph traversal is randomly-ordered from disk's perspective, the I/O amplification is severe. &emsp;Recall and latency both degrade, and the degradation is non-linear.

DiskANN, developed by Microsoft Research, was designed specifically for this regime. &emsp;It combines two things: the Vamana graph algorithm, which produces denser and more robustly-connected graphs than HNSW's multi-layer structure and so needs fewer hops per search, and product quantisation (PQ) compression of the vectors themselves. &emsp;The compressed vectors stay in memory; the full-precision vectors live on disk.

A query traverses the PQ-compressed graph in memory to identify candidate neighbours, then reads only the handful of candidates' full-precision vectors from disk for final ranking. &emsp;Disk I/O is bounded by a small constant per query rather than scaling with graph-traversal depth.

**JVector** is the Java implementation of DiskANN, originally built by DataStax for Astra DB and now integrated into Apache Cassandra 5.0. &emsp;It is a pure-Java embedded vector search engine, Apache-licensed, and has several architectural properties worth naming directly:

- **Multi-layer DiskANN**: JVector combines the hierarchical structure of HNSW with Vamana within each layer, inheriting the connectivity advantages of DiskANN's graph structure while preserving the fast-traversal properties that made HNSW popular.
- **Product quantisation with re-ranking**: JVector keeps PQ-compressed vectors in memory for fast candidate selection, then re-ranks the top candidates using full-precision vectors fetched from disk. &emsp;This preserves recall in the larger-than-memory regime where HNSW's recall collapses.
- **SIMD-accelerated operations** via Java's Panama Vector API: index-build and query operations use single-instruction-multiple-data (SIMD) hardware instructions, with an internal kmeans implementation reported at 5× the performance of Apache Commons Math's equivalent.
- **Incremental construction**: the index is queryable while being built. &emsp;There is no separate indexing phase and no stop-the-world rebuild, so no delay opens between "vector written" and "vector searchable." &emsp;This matters for transactional workloads where writes arrive continuously.
- **Linear-scaling concurrent builds**: index construction scales linearly to at least 32 threads, and doubling threads halves build time. &emsp;Most HNSW implementations serialise or lock extensively during construction.

JVector is reported to implement the FreshDiskANN variant of DiskANN, which is the property that suits it to an OLTP database's write patterns.

The empirical result is that JVector maintains high recall as working sets exceed RAM, precisely where HNSW-based implementations degrade. &emsp;That property determines whether vector search at enterprise embedding-corpus scale is affordable. &emsp;For a 100-million-vector corpus, HNSW implementations require provisioning RAM for the full working set; JVector can serve the same workload with roughly a tenth of the RAM by keeping only PQ-compressed graphs in memory. &emsp;The cost difference at cloud-pricing RAM rates is substantial, and it grows with corpus size.

Astra DB (DataStax's managed Cassandra offering, built on JVector) has been recognised as a Forrester Leader for vector search capabilities.

---

## Why Presto and Apache Spark: different analytical access patterns, one shared dataset

Analytics is not a single workload. &emsp;Interactive BI queries, ad-hoc exploration, scheduled reporting, ML feature generation, and batch aggregations all share the label "analytics" while having very different execution profiles. &emsp;A platform that collapses OLTP and OLAP onto one dataset must still offer the right engine for each analytical access pattern, or the unification becomes a regression for the analytical users.

This stack provides both Presto and Apache Spark as first-class analytical engines, reading the same persisted data through different paths optimised for different workloads. &emsp;The division of labour is deliberate.

### Presto: interactive SQL at BI concurrency

Presto was built at Facebook for interactive, federated SQL over heterogeneous data sources. &emsp;Its strengths are low-latency query planning, cost-based optimisation over joins, and concurrency models designed for many simultaneous BI users. &emsp;In this stack, Presto connects to Cassandra via its Cassandra connector, runs queries that target specific partitions or bounded token ranges, and returns results with the latency profile BI tools expect.

Presto is the right engine when:
 - users are interactive and expect sub-minute responses
 - queries hit specific partitions or narrow ranges
 - the workload is read-only SQL with standard aggregations and joins
 - concurrency is high (many analysts, dashboards and agents at once)

### Apache Spark: bulk compute over the entire dataset

Spark was built for large-scale distributed computation and has matured into the standard tool for ETL, ML feature generation, and analytical workloads that touch substantial fractions of a dataset. &emsp;In this stack, Spark has two distinct paths into Cassandra: the standard spark-cassandra-connector (for per-partition queries that benefit from Spark's distributed execution but not from bulk I/O), and the Spark Bulk Reader via the Sidecar (CEP-28) for wide scans that read SSTable files directly from snapshots. &emsp;Spark is the right engine when:
 - the workload touches large fractions of the dataset (wide scans, cross-partition aggregations, full-table statistics)
 - the computation is non-trivial (feature engineering, ML training, complex transformations)
 - batch latency (minutes to hours) is acceptable
 - the read throughput requirement exceeds what the OLTP path can sustain without impact

**The architectural point is what these engines share.** Both read the same persisted data that the OLTP path writes to. &emsp;There is no separate analytical copy and no reshaping into a columnar warehouse, so there is no ETL window during which analytics are stale. &emsp;The two engines give analytical users the right tool for their access pattern while preserving the core property of the unified architecture: one record of truth, accessed through paths optimised for each workload's service-level objectives.

In a dual-system architecture, the OLAP engine owns the storage layer. &emsp;Snowflake's compute and storage are co-designed. &emsp;Databricks' Photon engine assumes Delta Lake. &emsp;This coupling is what makes those systems fast for their intended workloads, but it also means the analytical engine locks in the storage decision. &emsp;Choosing Snowflake means storing data in Snowflake's format, under Snowflake's governance and operational model.

This stack decouples the analytical engine from the storage layer. &emsp;Presto and Spark are the engines in this demo; they are not the only possible engines. &emsp;Any tool that can read Cassandra's SSTables (directly or via Sidecar APIs) can serve analytical workloads against the same data. &emsp;The choice of analytical engine becomes a workload-matching decision rather than a platform-migration decision, which is what "compute/storage separation" was supposed to mean before the cloud warehouses re-coupled them under a different brand.

### Iceberg v3 as the optional columnar layer

For workloads where columnar storage is genuinely the right answer (long-term retention, cold-tier analytics, scan-heavy queries over historical data, interop with external tools), this stack exports to Apache Parquet and Apache Iceberg. &emsp;Exports are optional, not foundational: the freshest strongly-consistent view remains in the OLTP store. &emsp;When you do export, the destination matters, and Iceberg has been through a generational shift worth understanding.

Iceberg v3, ratified in mid-2025 and maturing through the 1.8, 1.9, and 1.10 releases, closes most of the expressiveness gap between open table formats and traditional data warehouses. &emsp;Four changes stand out:

**Deletion vectors.** Row-level deletes in Iceberg v2 used position delete files, a sensible design that degraded under high-churn workloads because readers had to merge many small delete files against large data files on every query. &emsp;V3 replaces them with binary deletion vectors: a Roaring-bitmap sidecar file per data file, encoding deleted row positions compactly. &emsp;Query engines check the bitmap with minimal overhead and skip deleted rows without merging delete files.

CDC and streaming-upsert workloads, the ones that were hardest on Iceberg v2, become much cheaper to maintain. &emsp;Compaction pressure drops and the read path stabilises. &emsp;The write-amplification problem that made Iceberg v2 unsuitable for mutation-heavy pipelines largely disappears.

**The Variant type.** Semi-structured data has haunted analytical systems for a decade. &emsp;Both of the usual workarounds fail: flattening JSON into many nullable columns explodes schema width, and storing JSON as string blobs eliminates filter pushdown. &emsp;Iceberg v3 introduces a Variant binary encoding, defined jointly with the Parquet project, that stores semi-structured data natively with schema flexibility and engine-level pushdown.

Engines can filter on nested Variant fields without parsing entire JSON strings or scanning every row. &emsp;Logs, telemetry, event data and configuration workloads, the use cases that previously forced enterprises into separate document stores, can now live in Iceberg tables without either of the old workarounds.

**Row lineage.** Every row in a v3 table carries stable metadata indicating when it was added and last modified. &emsp;For incremental processing, CDC replication, and regulated-environment audit, this eliminates the need to compute row-level change detection from scratch on every downstream job.

**Default values and instant schema evolution.** Adding a column to a petabyte table no longer requires a backfill. &emsp;The default value lives in metadata; existing data files are read as if the column were present with that default. &emsp;ALTER TABLE becomes O(1) on a table with any number of rows. &emsp;This is the Iceberg parallel to the schema-evolution argument made earlier for adding vector columns to Cassandra tables via SAI: schema change becomes cheap enough to stop being an architectural event.

The significance for this stack is specific. &emsp;Cassandra holds the record of truth, and analytical workloads read SSTable storage directly via the Bulk Reader for live queries. &emsp;Where columnar storage, time travel or external-tool interop is genuinely the right answer, they can export to Iceberg v3, a format that has just closed the expressiveness gap with proprietary warehouses while remaining open and vendor-neutral. &emsp;The export target went through a generational shift in 2025 rather than staying frozen in 2020.

### Velox and Prestissimo

Presto 2.0, internally named Prestissimo, is a full rewrite of the Presto execution engine on Velox, Meta's open-source composable query execution library. &emsp;It ships today as a drop-in replacement for workers in the same Presto cluster. &emsp;The coordinator remains unchanged; worker processes are swapped; the same SQL runs against the same data with the same connectors.

Meta, Uber, and IBM run it at production scale. &emsp;IBM's published TPC-DS benchmarks showed roughly 3× speedup with Prestissimo at 1TB, and at 100TB delivered price/performance competitive with Databricks Photon at less than 60% of the cost. &emsp;Fleet-size reductions of 2–3× are commonly reported. &emsp;For batch and interactive analytical workloads, this is the difference between "acceptable" and "best in class" on price/performance.

Velox itself is the deeper point. &emsp;It is a composable execution library, shared rather than specific to Presto, that factors out the runtime primitives common to query engines: hash joins, aggregations, windows, filter/project operators, memory management, vectorized scan paths. &emsp;Apache Spark's Gluten project uses Velox to accelerate Spark execution. &emsp;The same runtime library, tuned once, benefits multiple engines.

In a traditional lakehouse architecture, each engine has its own execution primitives and operators, and therefore its own performance characteristics, which is why moving a workload from Spark to Presto to Snowflake to Databricks produces different results with different tuning properties and different cost profiles. &emsp;Velox replaces that fragmentation with one shared implementation. &emsp;An optimisation contributed to Velox (a better hash-join implementation, a vectorized aggregation, a smarter I/O prefetcher) propagates to every engine built on it. &emsp;The economics of open-source contribution change, because the contribution surface is shared.

### GPU-native execution

Velox's composability has enabled something that would have been nearly impossible for a monolithic query engine: hardware-accelerated execution without rewriting the engine. &emsp;NVIDIA and IBM have integrated cuDF (NVIDIA's GPU-native DataFrame library) as a Velox backend, producing GPU-executed Presto queries and hybrid CPU/GPU execution for Spark.

The early published results are notable. &emsp;TPC-DS queries running on an NVIDIA GH200 Grace Hopper system completed in a fraction of the time of CPU-only execution on high-end silicon. &emsp;For workloads dominated by hash joins, aggregations, and scans over columnar data, GPU execution has been known to be promising for years; what Velox enables is the ability to drop it into existing Presto and Spark deployments without a fork, a rewrite or a new engine.

The relevance to this stack is indirect but important. &emsp;The architecture this repo demonstrates (Cassandra as the storage of record, Iceberg v3 as the optional columnar export, Presto and Spark as the analytical engines) is positioned to benefit from GPU acceleration as it matures, because the execution layer is already the one that GPU acceleration targets. &emsp;A future where analytical scans run on GPU workers consuming the same data that CPU workers consume today requires no architectural change to this stack. &emsp;It requires a worker binary swap.

### The compounding picture

The shifts behind this stack all landed within roughly the same 24-month window: Accord for strict-serializable transactions, JVector for larger-than-memory vector search, native storage-layer CDC, Iceberg v3 for expressive open table formats, Velox for composable high-performance execution, and cuDF for GPU acceleration. &emsp;That is not a coincidence. &emsp;Each was pursuing the same underlying goal: decoupling the semantic layer of data management (transactions, consistency, schemas, types) from the physical layer (storage format, execution engine, hardware target) so that each can evolve independently.

When decoupling succeeds, the effects compound. &emsp;A write to the system of record flows through strict-serializable consensus into SSTable storage; is indexed in place by SAI for scalar filters and by JVector for vector similarity; is streamed through CDC to Kafka with replication-factor-aware dedup; is read directly from SSTables by Spark for bulk analytics; is queried through Presto Native running on Velox primitives that will soon run on GPUs; and is optionally exported to Iceberg v3 with row-level lineage and deletion vectors where columnar scans are the right tool. &emsp;Each stage uses the component best suited to its workload. &emsp;None of them locks the others in.

This is what the cloud warehouse vendors cannot match without abandoning the architectural bets their businesses are built on. &emsp;Snowflake's execution engine is proprietary; it cannot adopt Velox without becoming a different product. &emsp;Databricks' Photon is coupled to Delta Lake; they have publicly committed to Iceberg v3 interoperability, but Delta remains the house format and the optimiser is tuned for it. &emsp;Neither can plausibly offer the per-request availability model that a leaderless Cassandra+Accord deployment provides, because their storage architectures were not designed for it.

The open stack wins on assembly rather than on any single component. &emsp;Velox-based Presto will likely trail Snowflake on some workloads for years. &emsp;JVector will trail specialist vector databases on pure ANN benchmarks on pre-sized in-memory corpora. &emsp;Iceberg will trail Delta on certain ecosystem integrations until the v3 rollout completes across engines.

What it offers instead is components that can be assembled, replaced and accelerated independently, by an industry-wide contribution surface, without vendor permission. &emsp;Every new access pattern, modality, downstream consumer and hardware target is served from the same record of truth, without a new pipeline and without a new platform migration. &emsp;That optionality is what an enterprise data platform needs in order to still be relevant in ten years.

The argument for the unified architecture rests on the combination rather than on the parts: strict-serializable transactions, in-place vector search, storage-layer CDC, decoupled analytical engines, generational open table formats, composable execution and hardware acceleration. &emsp;Each by itself is an optimisation. &emsp;Together they are an architectural property that dual-system stacks cannot replicate without recreating the duplication they were meant to avoid.

---

## Broken opinions

Two typical software-engineering assumptions break down at the data-platform layer: YAGNI, and the separation of OLTP from OLAP. &emsp;Both are worth naming, because both are materially to blame for many a data-platform crisis.

### "YAGNI: don't design for scale until you need to"

**Where it applies.** YAGNI ("you aren't going to need it") is sound advice for application code. &emsp;You can refactor a service, swap a framework, or replace an API layer in weeks. &emsp;Over-engineering the application tier is a real and common failure mode, and YAGNI is a good corrective.

**Where it breaks.** Data platforms don't refactor in weeks. &emsp;Migrating a petabyte-scale OLTP database to a different storage model is a multi-year project that often spans organisational restructuring. &emsp;By the time "later" arrives, the technical debt is structural, the team that built it has moved on, and the business has grown around its limitations.

The cost of scaling a data platform late is qualitatively different from the cost of scaling an application late:

- **Business-growth stalls** while the migration runs, and data-platform migrations that take 2–3 years are normal rather than exceptional
- **Incidents multiply** during the migration because two platforms run in parallel
- **Opportunity cost compounds** because analytics, AI, and product initiatives stall waiting for the platform to catch up

The sunk cost of designing for scale early is, mostly, learning. &emsp;The cost of scaling late is growth limitation, and sometimes business failure. &emsp;These are not symmetric.

**The defensible version**: don't over-engineer your application schema, but do pick a data platform that can grow with the business. &emsp;Choosing a platform that cannot scale is a decision that compounds for the life of the company.

### "Analytics is a separate responsibility and platform"

**Where it applies.** When analytics workloads are genuinely offline (daily batch reports, quarterly aggregations) and staleness measured in hours is acceptable, this was most enterprises until roughly five years ago, and is still many enterprises today.

**Where it breaks.** "Analytics" now takes in real-time personalisation, fraud detection, operational dashboards, feature generation for ML, and AI retrieval, all of them workloads where staleness is a bug. &emsp;The separation between "application data" and "analytical data" then becomes a source of incidents rather than a clean architectural boundary.

The symptoms are predictable:

- Data scientists struggle with stale, fragmented, poor-quality data on insufficient tooling
- Engineering teams push analytics off as "not their problem" while being unable to deliver features that require fresh cross-system data
- Data mesh and data products appear as attempts to reconcile the split; they help in principle, but the current tactics do not address the core duplication
- Governance fragments across systems, creating compliance and security surface area

The era of agentic AI forces this to surface, because agent workloads are indistinguishable from transactional application stacks in their data access requirements. &emsp;Enterprise data platforms need to be designed accordingly, ideally from the beginning, but increasingly, *now*, even if it requires rework.

**The defensible position**: analytics as a separate responsibility is a valid choice for some workloads and an increasingly expensive one for others. &emsp;Recognise which category your workload falls into before accepting the default.

---

## What to take from this

The proposal of this repo is that the trade-offs have shifted for a growing class of applications, and that when they shift, the cost of the inherited architecture becomes the dominant line item, both in dollars (see [TCO-Comparisons.md](TCO-Comparisons.md)) and in organisational capacity (the data platform becomes the thing that blocks everything else).

The harder argument: once you accept that the trade-offs have shifted, there is no graceful way to delay the reckoning. &emsp;Dual-system architectures compound. &emsp;Every new AI initiative, every new data modality, every new compliance requirement adds infrastructure and pipelines.

*Turn database sprawl into something much simpler.*

![fun but serious illustration of data platform debt](simplification.png)
