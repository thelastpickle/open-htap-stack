# Vendor-Neutral Open Source HTAP Data Platform

_Proof of Concept that takes 4 minutes to demonstrate._

An enterprise-grade Hybrid Transactional/Analytical Processing (HTAP) data platform, built from Apache-licensed components you already know.

Comes with a [web drone dashboard demo](docs/MISSION-CONTROL.md) of realtime data filtering and visualisation.

**Key characteristics**

- **Record of Truth** — one dataset, one store, one governance surface, one schema; no ETL copies, no reconciliation debt
- **Strict-Serializable ACID transactions** via Accord (CEP-15) — the same isolation class Google Spanner offers.&emsp;Shipped here and not turned on: the stack runs Cassandra 6.0-alpha2, whose `lib/` carries Accord, and a transaction is refused by the node rather than the parser because no table declares `transactional_mode`
- **OLTP** first capabilities: high concurrency and horizontal scaling; write p99 under 5ms and read p99 under 50ms
- **Multiple SQL interfaces over the same data**:
  - SparkSQL and Presto for analytics, both running here
  - Postgres wire-protocol and dialect adapter for application SQL, via Apache Calcite: designed, not built.&emsp;See `accord-sql/Dockerfile`
- **Resource isolation by construction** — OLAP reads via persisted-structure paths that do not contend with the OLTP request path
- **Native CDC to Kafka** via the Sidecar, easy to plug into existing platforms, ecosystems, and migration paths
- **Ecosystem integration**: Apache Kafka, Apache Spark, Presto, Apache Parquet, Apache Iceberg
- **Freedom to operate** — Apache-licensed, deployable anywhere, no per-credit or per-DBU licensing
- **80%+ Lower TCO** than OLTP + ETL + warehouse stacks — see the [TCO worksheet](docs/TCO-Comparisons.md)


---

## Documentation

1. [Demo Quick Start](#demo-quick-start)
2. [Architecture at a glance](#architecture-at-a-glance)
3. [Why this stack](docs/WHY.md) — the vision and the argument
4. [Architecture deep-dive](docs/ARCHITECTURE.md) — scope, consistency, enterprise considerations
5. [Mission Control dashboard](docs/MISSION-CONTROL.md) — the demo you can show a boardroom
6. [TCO Comparisons](docs/TCO-Comparisons.md) — worksheet and sensitivity analysis
7. [Hard Questions FAQ](docs/ARCHITECTURE.md#hard-questions-faq) — direct answers

---

## Demo Quick Start

### Prerequisites

Check that your container runtime has at least 12 GB of memory allocated:

```shell
podman machine inspect --format "{{.Resources.Memory}}" # must be greater than 12287 (12 GB)
```

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for how to increase the memory limit.

### Bring up the whole stack in under 4 minutes, and start ingesting event data

```shell
podman compose -f podman-compose.yml up
```

| Service                  | URL                                            |
| ------------------------ | ---------------------------------------------- |
| Mission Control dashboard| <http://localhost:4000>                        |
| Dashboard API and docs   | <http://localhost:8000/docs>                   |
| Presto UI                | <http://localhost:8088/ui/>                    |
| Spark master UI          | <http://localhost:8080>                        |
| Spark application UI     | <http://localhost:4040>                        |

### The 4-minute demonstration

Open <http://localhost:4000>. Every figure on every page is a live query against the stack you just
started; nothing is seeded or pre-rendered.

1. **Overview** — the fleet, the ingest rate, and the events already stored.
2. **Map** — live positions against restricted airspace. Click an asset for its recorded flight path,
   read back out of Cassandra.
3. **Explore → Compare engines** — run one query down the access paths you choose: Cassandra,
   Presto, SparkSQL through the Cassandra connector, the Analytics bulk reader going straight to
   SSTable files through the Sidecar, and cqlite parsing the live SSTable files inside the
   dashboard's own backend. This is the argument of the whole stack in one screen: same data, five
   paths, no ETL between them. Switch to the *Group the fleet* preset and Cassandra declines the
   query outright, which states plainly why the other four exist. Under each result is the
   point-read latency measured while that path was working, so the isolation the two file readers
   claim is shown rather than asserted. Run the paths one at a time to see what each costs; run them
   all at once to see what they cost each other.
4. **Explore → Vector search** — semantic search over the assets' text payloads, through Cassandra
   SAI, with each hit's live position fetched by point read. Turn on **Live embedding** and the index
   follows the snippets as they are rewritten, in a loop behind the writes; the panel says how far
   behind it is, and the point read on the Health page says what it cost the request path.
5. **Settings → Trigger breach scenario** — write a real alert and watch the map, the KPIs and the
   alert feed pick it up.

See [docs/MISSION-CONTROL.md](docs/MISSION-CONTROL.md) for what each page queries, how the demo
controls reach the data producer, and how to run either half from source.

### Example CQL queries (plumbing)

```shell
podman exec cassandra \
  cqlsh cassandra -e "SELECT * FROM demo.events LIMIT 3;"
```

The demo ingests drone telemetry events via Kafka into a Cassandra table partitioned by a 15-minute time bucket and a shard, clustered by event id — the ordinary way to model an event stream so that a question about a period of time reads only the partitions holding it. Every access path exploits that differently, which is half of what the dashboard's comparison shows. See [docs/DATA-MODEL.md](docs/DATA-MODEL.md) for the full schema and why it is shaped this way.

### Example Presto queries

```shell
podman exec presto \
  presto-cli --execute "SHOW SCHEMAS FROM cassandra;"

podman exec presto \
  presto-cli --execute "SELECT * FROM cassandra.demo.events LIMIT 100;"

podman exec presto \
  presto-cli --execute "SELECT entity_id, COUNT(*) FROM cassandra.demo.events GROUP BY entity_id LIMIT 10;"
```

To watch query progress in the browser: <http://localhost:8088/ui/>

### Example per-partition SparkSQL query

A simple query using the Cassandra-Spark-Connector (requires creating a temp view first):

```shell
podman exec -it spark \
  spark-sql --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    --conf spark.cassandra.connection.host=cassandra
```

```sql
CREATE TEMPORARY VIEW events_for_partition_queries USING org.apache.spark.sql.cassandra
OPTIONS (keyspace 'demo', table 'events');

SELECT * FROM events_for_partition_queries LIMIT 3;
```

The `spark-cassandra-connector` is best for per-partition (or per-index) queries. Reads and writes go through Cassandra's CQL interface and its JVM.

### Example bulk (direct-to-SSTable) SparkSQL query

Queries using the Cassandra Spark Bulk Reader via the Cassandra Sidecar:

```shell
podman exec -it spark \
  spark-sql \
    --packages org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.5-mck0,org.apache.cassandra:analytics-sidecar-vertx-client-all:0.5-mck0,org.apache.cassandra:cassandra-bridge_spark3_2.12:0.5-mck0
```

```sql
CREATE TEMPORARY VIEW events_for_bulk_queries
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

SELECT entity_id, COUNT(*) AS cnt, MIN(event_time) AS first_seen, MAX(event_time) AS last_seen
  FROM events_for_bulk_queries GROUP BY entity_id ORDER BY cnt DESC LIMIT 10;
```

To watch query progress in the browser: <http://localhost:4040/>

The Cassandra Bulk Reader/Writer interfaces directly to the data directories on Cassandra nodes. Reads go directly against snapshot SSTable files on disk, providing point-in-time consistency. Direct file access yields high throughput for bulk or analytics-style reads and writes **without impacting the latency of other requests to the Cassandra cluster**: this is the mechanism that enables OLAP resource isolation by construction.

### Dump the whole database to Parquet

To write to a single Parquet file, continue from the `spark-sql` example above:

```sql
INSERT OVERWRITE DIRECTORY '/var/lib/cassandra/parquet-exports/demo_events' USING parquet
SELECT /*+ COALESCE(1) */ * FROM events_for_bulk_queries;
```

This writes the entire `demo.events` table to a single Parquet file in the `cassandra-data` directory. The `COALESCE(1)` reduces all partitions to one before writing, producing a single output file.

### Move Parquet files quickly into the database

> FIXME: still broken, and no longer with the error recorded here before. &emsp;`DecoratedKey … not serializable result: java.nio.HeapByteBuffer` does not reproduce; two other obstacles were measured, in this order. &emsp;First, every task failed startup validation with "Sidecar is unreachable", because the shaded Vert.x client resolves `cassandra` through its own Netty resolver rather than the JDK's, and in this container that resolver queries the host's nameserver instead of podman's. &emsp;`-Dvertx.disableDnsResolver=true`, in the command below, gets past it: `SidecarValidation` and `CassandraValidation` then both pass. &emsp;Second, the write reaches its shuffle and the executor dies with `OutOfMemoryError: unable to create native thread`, exit code 52, and retries until it is stopped. &emsp;That is container sizing rather than a version problem, and the bulk **reader** is unaffected throughout: it reaches the same Sidecar from the same container.

```shell
podman exec -it spark \
  spark-shell \
    --packages org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.5-mck0,org.apache.cassandra:analytics-sidecar-vertx-client-all:0.5-mck0,org.apache.cassandra:cassandra-bridge_spark3_2.12:0.5-mck0 \
    --conf spark.executor.extraJavaOptions=-Dvertx.disableDnsResolver=true
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

### CDC (Change Data Capture) to Kafka

Simple configuration to CDC all database writes into a Kafka topic:

```
todo
```

### Example Accord transactions

None runs in this stack yet, and the reason is no longer Cassandra's version.

The stack runs 6.0-alpha2, and that build carries Accord: `lib/cassandra-accord-6.0-alpha2.jar`.&emsp;cqlsh parses the grammar, and the node refuses the statement for one reason only:

```
BEGIN TRANSACTION SELECT * FROM demo.drone_latest_status LIMIT 1; COMMIT TRANSACTION;
InvalidRequest: code=2200 [Invalid query] message="Accord transactions are disabled on
table (See transactional_mode in table options); SELECT statement at [1:19]"
```

So what remains is a table option and a demonstration worth measuring, which is a change of its own rather than part of this upgrade.&emsp;See the comment in `cassandra/entrypoint.sh`.

### Example Application (OLTP) SQL

The Postgres-dialect prototype, Accord SQL, is deferred for the same reason and for now is a placeholder directory: `accord-sql/`.

---

## Architecture at a glance

The stack composes five well-understood Apache-licensed components:

```
Kafka (ingest)  →  Cassandra (storage of record)  →  Spark / Presto (analytics)
                         ↑                                   ↑
                         │                                   │
                    Accord (CEP-15)                  Sidecar Bulk Reader (CEP-28)
                    strict-serializable              direct SSTable access,
                    ACID transactions                snapshot-coordinated
```

Three access paths share the same persisted data:

- **OLTP path** — point reads and bounded partition reads through Cassandra's request path. Latency performance: p99 write < 5ms, p99 read < 50ms.
- **OLAP path** — wide scans and aggregations via the Spark Bulk Reader, reading SSTable files directly from coordinated snapshots. Does not contend with OLTP. Measured on this one node, counting the whole table with no predicate: 452,446,775 bytes of snapshot in 13.4 s, so 33.9 MB/s, and the snapshot itself cost 323 ms of that. Read it as a floor rather than a throughput figure, because the measurement is a laptop running eight containers on seven cores; the mechanism is what scales per node, and this demo does not measure that. No write rate is quoted, because the bulk writer does not work here yet — see the note above. The dashboard also reads the same files with no snapshot and no JVM, in its own process, through cqlite: the same count took 32.9 s over 450,318,008 bytes of live files, single-threaded against the bulk reader's four cores, and answers as of the last flush.
- **CDC path** — change streams to Kafka via the Sidecar, with RF-aware deduplication.

The architectural property that makes this work, that analytical scans do not touch the OLTP hot path — holds **by construction**, not by tuning. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full technical treatment.

---

## References

- Accord / CEP-15 :: transactions, strict serializability, failure tolerance goals
- CEP-28 :: Spark bulk reader/writer via Sidecar to persisted storage
- Cassandra Analytics :: bulk reader/writer examples
- cqlite :: a Rust library that parses Cassandra SSTable files, <https://github.com/pmcfadin/cqlite>
- SQL prototype repo :: Postgres wire protocol + Calcite-based dialect coverage
