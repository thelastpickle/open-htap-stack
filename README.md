# Vendor-Neutral Open Source HTAP Data Platform

_Proof of Concept that takes 4 minutes to demonstrate._

An enterprise-grade Hybrid Transactional/Analytical Processing (HTAP) data platform, built from Apache-licensed components you already know.

Comes with a [web drone dashboard demo](docs/MISSION-CONTROL.md) of realtime data filtering and visualisation.

**Key characteristics**

- **Record of Truth** — one dataset, one store, one governance surface, one schema; no ETL copies, no reconciliation debt
- **Strict-Serializable ACID transactions** via Accord (CEP-15) — the same isolation class Google Spanner offers.&emsp;Running here, on three tables that declare `transactional_mode='full'`: a conditional write across three partitions in three tables, whose median cost is 1.66 – 1.87 ms against 0.43 – 0.56 ms for a plain insert.&emsp;One node at `replication_factor: 1`, so that is a floor and not a distributed figure
- **OLTP** first capabilities: high concurrency and horizontal scaling; write p99 under 5ms and read p99 under 50ms
- **Multiple SQL interfaces over the same data**:
  - SparkSQL and Presto for analytics, both running here
  - Postgres wire-protocol and dialect adapter for application SQL, via Apache Calcite: GEICO's cassandra-sql, running here on its own keyspaces, with joins, subqueries and `BEGIN`/`COMMIT`.&emsp;A proof of concept by its own account, and the section below reports what it does and does not hold
- **Resource isolation by construction** — OLAP reads via persisted-structure paths that do not contend with the OLTP request path
- **Native CDC to Kafka** via the Sidecar, easy to plug into existing platforms and migration paths.&emsp;Running here, off the request path: one table's mutations reach Kafka as registered Avro at 2,718 records/s, read from the commit log rather than by querying the node
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
6. [Accord transactions](docs/ACCORD-TRANSACTIONS.md) — the two demonstrations, their steps and their cost
7. [Application SQL](docs/APPLICATION-SQL.md) — cassandra-sql over Cassandra, and what it does not hold
8. [CDC to Kafka](docs/CDC-TO-KAFKA.md) — the change stream, its record format and its cost
9. [TCO Comparisons](docs/TCO-Comparisons.md) — worksheet and sensitivity analysis
10. [Hard Questions FAQ](docs/ARCHITECTURE.md#hard-questions-faq) — direct answers
11. [The Java services](docs/JAVA-PORT.md) — the architect's review the rewrite started from, and what it found

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
| Apicurio schema registry | <http://localhost:8085>                        |

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
   all at once to see what they cost each other. Each box fills as its path answers, and the quickest
   path is asked first, so the first result arrives in milliseconds.
4. **Explore → Vector search** — semantic search over the assets' text payloads, through Cassandra
   SAI, with each hit's live position fetched by point read. Turn on **Live embedding** and the index
   follows the snippets as they are rewritten, in a loop behind the writes; the panel says how far
   behind it is, and the point read on the Health page says what it cost the request path.
5. **Transactions → Accord** — run a transaction whose condition lives in other partitions. Of the six
   steps, two are refused, and the row count after each is what proves a refusal changed nothing. A
   second demonstration grants airspace clearance from a semaphore held in three tables.
6. **Transactions → SQL** — Postgres-dialect SQL over Cassandra, through GEICO's cassandra-sql. Joins,
   subqueries and `BEGIN`/`COMMIT`, on tables of its own. Run a preset, then the same statement with
   `ROLLBACK`, and no row is left behind.
7. **Transactions → Schema** — both data models, read from the engines that own them. The CQL side
   marks which tables are Accord tables; the SQL side names what its catalog reports stale.
8. **Streaming** — the change stream, as it arrives. Each row is one mutation the Sidecar read out of a
   discarded commit log segment and published to Kafka; the panel shows the registered Avro schema
   beside them, and nothing on the page queries Cassandra. `isPartial` on each row is the honest part:
   the stream carries the cells a write touched, not the row as it now stands.
9. **Health** — reachability per service, latency per access path, and the query in flight. Cancel a
   running comparison from here.
10. **Settings → Trigger breach scenario** — write a real alert and watch the map, the KPIs and the
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


```shell
podman exec -it spark \
  spark-shell \
    --packages org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.5-mck0,org.apache.cassandra:analytics-sidecar-vertx-client-all:0.5-mck0,org.apache.cassandra:cassandra-bridge_spark3_2.12:0.5-mck0 \
    --conf "spark.executor.extraJavaOptions=-Dcassandra.releaseVersion=6.0-alpha2 -Dcassandra.analytics.bridges.sstable_format=bti -Dvertx.disableDnsResolver=true"
```

That `--conf` names three properties, and how it names them matters.&emsp;`-Dvertx.disableDnsResolver=true` is here because every task once failed startup validation with "Sidecar is unreachable": the shaded Vert.x client resolves `cassandra` through its own Netty resolver rather than the JDK's, and that resolver queried the host's nameserver instead of podman's.&emsp;The other two repeat what `spark/conf/spark-defaults.conf` already sets, because `--conf spark.executor.extraJavaOptions` **replaces** that value rather than adding to it, and a bare `-Dvertx.disableDnsResolver=true` therefore takes the release version and the SSTable format off the executor.&emsp;The write succeeded without those two, so neither is required here; naming them keeps the flag from being a silent subtraction.

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

Cassandra's Change Data Capture (CEP-8) hard-links each commit log segment into `cdc_raw` as it discards it.&emsp;The Sidecar beside the node reads those segments and publishes one table's mutations to Kafka as Avro, with the schema registered in Apicurio.&emsp;Nothing queries Cassandra to do it: the change stream is taken from the write path's own log, so it is one more way the demo keeps machinery off the request path.

It runs here, and the **Streaming** page at <http://localhost:4000/streaming> shows the mutations arriving.&emsp;Turning it on is three settings, one of which is a table option:

```yaml
# cassandra.yaml — cdc_raw must share a filesystem with the commit log, because it is hard links
cdc_enabled: true
cdc_raw_directory: /var/lib/cassandra/cdc_raw
cdc_total_space: 4096MiB
cdc_block_writes: false          # trim the oldest segment rather than reject the write
```

```sql
-- the table opts in; unlike transactional_mode, cdc can be turned on after the fact
ALTER TABLE demo.drone_latest_status WITH cdc = true;

-- the Sidecar reads its publisher settings from a table it creates itself
INSERT INTO sidecar_internal.configs (service, config) VALUES ('cdc', {
  'cdc_enabled': 'true', 'topic': 'cdc-mutations', 'topic_format_type': 'STATIC',
  'jobid': 'htap-demo', 'datacenter': 'datacenter1',
  'watermark_seconds': '1800', 'micro_batch_delay_millis': '500', 'max_commit_logs': '2'
}) IF NOT EXISTS;
```

**Measured** on a fresh stack: the first mutation reached Kafka 95.7 s after `up -d`, and over the following 1,447 s the topic took **2,718 records/s** with **no decode failure in 5.4 million records** and no growth in consumer lag.&emsp;End-to-end latency is **seconds, and not milliseconds**: p50 had a median of 8.0 s and a range of 2.7 to 21.1 s at 2,000 events a second, and 2.2 to 4.7 s over ten samples at 400.&emsp;The publisher's own poll interval is 500 ms, so it is not the bound; nor is the commit log segment, since the segment completed every 9.3 s at the higher rate and every 45 to 46 s at the lower one, which is the opposite order to the delays.&emsp;Read either figure as a best case rather than as the demo's latency: the publisher's ceiling is the same order as the node's write rate to the table, so while the sink drains a Kafka backlog the age of the newest published record reaches minutes, measured at 92 to 156 s in one run, 400 to 480 s in a second and 836 to 848 s in a third.&emsp;An age once built stays: in that third run the publisher led the writer by only 7%, 2,882 records/s against 2,703, which needs hours to close a fourteen-minute backlog.

Two limitations are worth stating here.&emsp;`cdc_raw` is bounded, and with `cdc_block_writes: false` the node deletes the oldest segment at the bound rather than refusing the write, so a publisher that falls far enough behind loses changes; measured, the directory held at the bound for thirteen minutes while both publication and the request path continued.&emsp;And the stream carries **mutations, not rows**: `operationType` reads `UPDATE` for a CQL `INSERT`, and `isPartial` is `true`, because a mutation carries the cells it wrote and not the row as it now stands.

See [docs/CDC-TO-KAFKA.md](docs/CDC-TO-KAFKA.md) for the record format, the full measurement table, the deletion that logs only at DEBUG, and the two local fixes the Sidecar needed.

### Example SQL Transactions

Accord runs here, on the **Accord** subtab of the Transactions page and at `/api/transactions`.&emsp;Six `demo` tables declare `transactional_mode='full'`; `events` does not, so consensus is never in front of 2,000 writes a second.

The subtab runs two transactions that CQL cannot express, each conditioning a write on rows in **other partitions**: a sequence apply that refuses both a replay and a gap, and the airspace semaphore below, which grants a fixed capacity under contention.

```sql
BEGIN TRANSACTION
  LET occ  = (SELECT capacity, remaining FROM demo.zone_occupancy WHERE zone_id = ?);
  LET held = (SELECT zone_id FROM demo.drone_clearance WHERE entity_id = ?);
  SELECT occ.remaining, occ.capacity, held.zone_id;
  IF occ.remaining IS NOT NULL AND occ.remaining > 0 AND held.zone_id IS NULL THEN
    UPDATE demo.zone_occupancy SET remaining -= 1 WHERE zone_id = ?;
    INSERT INTO demo.zone_clearance (zone_id, entity_id, granted_at) VALUES (?, ?, ?);
    INSERT INTO demo.drone_clearance (entity_id, zone_id, granted_at) VALUES (?, ?, ?);
  END IF
COMMIT TRANSACTION;
```

Two partitions of two tables are read and three are written, so a lightweight transaction cannot express it.&emsp;**Measured**: 8, 16 and 32 concurrent askers against a capacity of 2 each granted exactly 2, and a grant's p50 is 1.31 ms.&emsp;One node at `replication_factor: 1` pays no round trip, so read that as a floor.

See [docs/ACCORD-TRANSACTIONS.md](docs/ACCORD-TRANSACTIONS.md) for both statements, their step tables, the contention run and the CEP-15 caveats.

### Example Application (OLTP) SQL

GEICO's [cassandra-sql](https://github.com/geico/cassandra-sql) runs here, on the **SQL** subtab of the Transactions page and at `/api/sql-console`.&emsp;It speaks the Postgres wire protocol, plans with Apache Calcite, and stores rows in Cassandra as an ordered key-value encoding of its own, so an application gets joins, subqueries, aggregates over non-key columns and multi-statement transactions.&emsp;It reads its own three keyspaces and not `demo.events`, which is why it is absent from the five-path comparison.

The statement below writes and updates rows in four tables, and `ROLLBACK` in place of `COMMIT` leaves none of them behind:

```sql
BEGIN;
INSERT INTO flights (flight_id, operator_id, drone_id, departed_at, status, purpose, distance_km, duration_min, energy_wh, fee_nok, route_summary)
VALUES (9001, 1001, 2003, 1704499200000, 'cleared', 'survey', 21.40, 38.00, 742.00, 1290.00, 'Fornebu to Gardermoen, north transit');
INSERT INTO flight_legs (leg_id, flight_id, zone_id, leg_no, distance_km, dwell_min, leg_energy_wh)
VALUES (10001, 9001, 3003, 1, 6.20, 9.00, 198.00), (10002, 9001, 3001, 2, 15.20, 12.00, 544.00);
UPDATE drones SET battery_cycles = battery_cycles + 1 WHERE drone_id = 2003;
UPDATE operators SET flight_hours = flight_hours + 1 WHERE operator_id = 1001;
COMMIT;
```

**Measured** over two warm sweeps: p50 31.0 / 34.0 ms for the transaction, 9.3 / 9.4 ms for the same statement rolled back, and 19.1 / 15.2 ms for a four-join select.&emsp;These are tables of five and eight rows, so read the figures as the cost of planning and of one round trip.

**It is a proof of concept by its own account**, "not production-ready" at "~40% (core features only)" SQL compliance, and eleven behaviours were measured here.&emsp;An integer bound parameter silently returns no rows, `UNIQUE` is the one declared constraint the engine enforces, arithmetic promotes an integer column to a double, and four further defects are join defects.

See [docs/APPLICATION-SQL.md](docs/APPLICATION-SQL.md) for each measured behaviour, the full timing table, the container patch and the partitioner finding.

### The schema explorer

The third subtab reads both data models from the engines that own them, at `/api/schema/cql` and `/api/schema/sql`.&emsp;Two routes rather than one, so that a stopped `accord-sql` blanks half the page instead of all of it.

**`DESCRIBE KEYSPACE demo` works through the driver, server-side, in one round trip**, which is what makes the CQL side cheap: 16 rows, each with a `create_statement`, covering the keyspace, its 14 tables and its one index.&emsp;`transactional_mode` is in that text although `system_schema.tables` has no such column, so the route reads the mode from the statement and the key structure from `system_schema.columns`, whose `kind`, `position` and `clustering_order` are what a `PRIMARY KEY` line needs.&emsp;It currently reports six tables as `full` and eight as `off`, with `payload_vector_idx` a `StorageAttachedIndex` on `drone_text_embeddings.payload_vector`.&emsp;A previous edition of this repository's notes said a script must parse `DESCRIBE` output or ask behaviourally without establishing that the first works; it does.

**On the cassandra-sql side the catalog is partly stale, and the route says which parts.**&emsp;It reads `pg_class WHERE relkind = 'r'` and one `pg_attribute` per `oid`, which are accurate: exactly the five live tables and their 40 columns, in `attnum` order.&emsp;It does not read `pg_tables`, which still lists `customers`, `orders`, `order_items` and `products` long after they were dropped, and it reports that staleness as a warning rather than hiding it.&emsp;Three more gaps come back the same way: there is no `information_schema`, `pg_constraint` is empty, so `UNIQUE` is the one constraint this engine enforces and the one its catalog does not report, and `pg_enum` and `pg_sequence` do not exist although the schema declares two ENUMs and a sequence.&emsp;The route also names the three keyspaces those rows encode into, so a reader can see this is SQL over Cassandra rather than a second database.&emsp;**`pg_attribute` is created on first use**, so a service that has restarted refuses every read of it while `pg_class` still answers, and the route then reports five tables with no columns and a note each; pressing Reset is the `CREATE TABLE` that registers it.

**The route joins in the backend rather than in SQL**, and the reason is in [docs/APPLICATION-SQL.md](docs/APPLICATION-SQL.md): four of the defects the SQL subtab reproduces are this engine's joins.

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
- **CDC path** — change streams to Kafka via the Sidecar, read from discarded commit log segments rather than by querying the node. Measured here: 2,718 records/s to the topic, and a p50 median of 8.0 s end to end at 2,000 events a second when the publisher is ahead of the writer, against 2.2 to 4.7 s at 400. Under a sink backlog the publisher is the bound instead and the age reaches minutes. The replication-factor-aware deduplication is configured, `watermark_seconds: 1800`, and this one node at RF=1 does not exercise it.

The architectural property that makes this work, that analytical scans do not touch the OLTP hot path — holds **by construction**, not by tuning. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full technical treatment.

---

## References

- Accord / CEP-15 :: transactions, strict serializability, failure tolerance goals
- CEP-28 :: Spark bulk reader/writer via Sidecar to persisted storage
- Cassandra Analytics :: bulk reader/writer examples
- cqlite :: a Rust library that parses Cassandra SSTable files, <https://github.com/pmcfadin/cqlite>
- cassandra-sql :: Postgres wire protocol and a Calcite-planned dialect over Cassandra, <https://github.com/geico/cassandra-sql>
