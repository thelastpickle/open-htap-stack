# Vendor-Neutral Open Source HTAP Data Platform

_Proof of Concept that takes 4 minutes to demonstrate._
An enterprise-grade Hybrid Transactional/Analytical Processing (HTAP) data platform, built from Apache-licensed components you already know.

## Key characteristics

- **Record of Truth** — one dataset, one governance surface, one schema; no ETL copies, no reconciliation debt
- **Strict-Serializable ACID transactions** via Accord (CEP-15) — the same isolation class Google Spanner offers
- **OLTP store** with high concurrency and horizontal scaling; write p99 under 5ms and read p99 under 50ms
- **Multiple SQL interfaces over the same data**:
  - SparkSQL and Presto for analytics
  - Postgres wire-protocol + dialect adapter for application SQL (PoC subset, via Apache Calcite)
- **Resource isolation by construction** — OLAP reads via persisted-structure paths do not contend with the OLTP request path
- **Native CDC to Kafka** via the Sidecar, with replication-factor-aware deduplication
- **Ecosystem integration**: Apache Kafka, Apache Spark, Presto, Apache Parquet, Apache Iceberg
- **Freedom to operate** — Apache-licensed, deployable anywhere, no per-credit or per-DBU licensing
- **80%+ Lower TCO** than OLTP + ETL + warehouse stacks — see the [TCO worksheet](docs/TCO-Comparisons.md)


---

## Documentation

1. [Demo Quick Start](#demo-quick-start)
2. [Architecture at a glance](#architecture-at-a-glance)
3. [Why this stack](docs/WHY.md) — the vision and the argument
4. [Architecture deep-dive](docs/ARCHITECTURE.md) — scope, consistency, enterprise considerations
5. [TCO Comparisons](docs/TCO-Comparisons.md) — worksheet and sensitivity analysis
6. [Hard Questions FAQ](docs/ARCHITECTURE.md#hard-questions-faq) — direct answers

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

### Example CQL queries (plumbing)

```shell
podman exec cassandra \
  cqlsh cassandra -e "SELECT * FROM demo.events LIMIT 3;"
```

The demo ingests drone telemetry events via Kafka into a Cassandra table keyed by `entity_id` and `event_time` — a typical wide-partition time-series shape. See [docs/DATA-MODEL.md](docs/DATA-MODEL.md) for the full schema.

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
    --packages org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.4.0-mck0,org.apache.cassandra:analytics-sidecar-vertx-client-all:0.4.0-mck0,org.apache.cassandra:cassandra-bridge_spark3_2.12:0.4.0-mck0
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

> FIXME: currently broken with `DecoratedKey … not serializable result: java.nio.HeapByteBuffer`

```shell
podman exec -it spark \
  spark-shell \
    --packages org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.4.0-mck0,org.apache.cassandra:analytics-sidecar-vertx-client-all:0.4.0-mck0,org.apache.cassandra:cassandra-bridge_spark3_2.12:0.4.0-mck0
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

See `mck/cassandra-6` branch.

### Example Application (OLTP) SQL

See `mck/cassandra-6` branch.

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
- **OLAP path** — wide scans and aggregations via the Spark Bulk Reader, reading SSTable files directly from coordinated snapshots. Does not contend with OLTP. Scale-out performance at 1.7Gb/s reads and 7Gb/s writes per node.
- **CDC path** — change streams to Kafka via the Sidecar, with RF-aware deduplication.

The architectural property that makes this work, that analytical scans do not touch the OLTP hot path — holds **by construction**, not by tuning. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full technical treatment.

---

## References

- Accord / CEP-15 :: transactions, strict serializability, failure tolerance goals
- CEP-28 :: Spark bulk reader/writer via Sidecar to persisted storage
- Cassandra Analytics :: bulk reader/writer examples
- SQL prototype repo :: Postgres wire protocol + Calcite-based dialect coverage
