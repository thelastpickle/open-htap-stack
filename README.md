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

Accord runs here, on the **Accord** subtab of the Transactions page and at `/api/transactions`.&emsp;The stack starts with `accord.enabled: true`, and six tables are created with `transactional_mode='full'`: `sessions_open`, `session_seq_applied` and `session_timeline` for the sequence demonstration below, and `zone_occupancy`, `zone_clearance` and `drone_clearance` for the clearance one.&emsp;`events` is deliberately left out, so consensus is never in front of 2,000 writes a second; see [docs/DATA-MODEL.md](docs/DATA-MODEL.md) for why that matters more than it sounds.

What the transaction demonstrates is a conditional write whose condition lives in **other partitions**.&emsp;A batch is atomic but not conditional, and a lightweight transaction conditions on a single partition, so neither can refuse a replay.&emsp;This one reads three partitions in three tables and writes two of them, and it applies only if the session is open, this sequence number has not been applied, and its predecessor has:

```sql
BEGIN TRANSACTION
  LET session_ok = (SELECT session_id FROM demo.sessions_open WHERE user_id = ? AND session_id = ?);
  LET already    = (SELECT seq FROM demo.session_seq_applied WHERE user_id = ? AND session_id = ? AND seq = ?);
  LET prev_ok    = (SELECT seq FROM demo.session_seq_applied WHERE user_id = ? AND session_id = ? AND seq = ?);
  SELECT session_ok.session_id, already.seq, prev_ok.seq;
  IF session_ok IS NOT NULL AND already IS NULL AND prev_ok IS NOT NULL THEN
    INSERT INTO demo.session_timeline (user_id, session_id, seq, event_id, event_time, event_type, payload)
      VALUES (?, ?, ?, ?, ?, ?, ?);
    INSERT INTO demo.session_seq_applied (user_id, session_id, seq) VALUES (?, ?, ?);
  END IF
COMMIT TRANSACTION;
```

Every timeuuid and timestamp is bound by the caller.&emsp;`now()` inside a transaction would be evaluated per replica, and Accord requires a deterministic statement.&emsp;The `prev_ok` guard is omitted for `seq=0`, which has no predecessor.

The demo drives six steps against a session of its own and reports the row count after each.&emsp;The two refusals are the point: they are reported as refusals rather than errors, and what proves they changed nothing is the count, not the response.

| Step | Applied | `session_timeline` after it |
| --- | --- | --- |
| open the session | yes | 0 rows |
| apply `seq=0` | yes | 1 row |
| replay `seq=0` | **no** — already applied | 1 row |
| attempt `seq=2` out of order | **no** — `seq=1` would leave a gap | 1 row |
| apply `seq=1` | yes | 2 rows |
| apply `seq=2` | yes | 3 rows |

An Accord transaction returns no `[applied]` column, only the row its own `SELECT` projects, so the backend reads the guard values back out of that projection and decides.&emsp;That is why the assertion in CI is the row count and not the field.

**Measured**, four runs of 2,000 applied transactions each, on a seven-core laptop with the ingest running at 2,000 events/s.&emsp;The two references write the same row into `session_timeline_plain`, which has the same columns and key and no transactional mode, at the same QUORUM:

| Write | p50 across four runs | max across four runs |
| --- | --- | --- |
| the transaction above | 1.66 – 1.87 ms | 6.2 – 26.7 ms |
| `IF NOT EXISTS` lightweight transaction | 0.83 – 1.00 ms | 4.7 – 32.0 ms |
| plain `INSERT` | 0.43 – 0.56 ms | 3.2 – 28.4 ms |

At the median the transaction costs about twice a lightweight transaction and about four times a plain insert, for three partition reads and two writes rather than one of each.&emsp;**The maxima say nothing.**&emsp;Every one of the three wanders across an order of magnitude between runs and none separates from the others, so on this stack a single maximum is a measure of what compaction and the sink were doing, not of the write path.&emsp;Four runs is what it took to see that; one run had suggested the transaction's tail was the interesting figure.

The point read stays put while the transactions run.&emsp;Over 52 to 57 reads of one asset during each run, p50 moved from an idle 2.1 – 2.4 ms to 2.6 – 3.2 ms, with no failures; the same runs' worst single read was between 8.2 and 52.7 ms.

#### Airspace clearance: a semaphore across three tables

The second demonstration on that subtab is admission control, which is the harder claim.&emsp;Three more tables opt in — `zone_occupancy`, `zone_clearance` and `drone_clearance` — and a grant reads the zone's remaining slots and the asset's existing clearance, in two partitions of two tables, then writes three:

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

**It counts down rather than up, and that is Accord's constraint rather than a preference.**&emsp;`IF occ.granted < occ.capacity` compares one `LET` reference to another, which Accord refuses with `SyntaxException … IllegalArgumentException null`; so the invariant is held as a decrementing `remaining` and the counter's agreement with the holder rows is checked afterwards, on every response, as `capacity == remaining + holders`.

The demo drives seven steps against the Gardermoen zone, whose capacity the sink seeds at 2:

| Step | Applied | Slots left after it |
| --- | --- | --- |
| grant `asset-000000` | yes | 1 of 2, one held |
| replay that grant | **no** — the asset already holds a clearance | 1 of 2 |
| grant the same asset a second zone | **no** — one clearance per asset | the royal palace untouched, 3 of 3 |
| grant `asset-000001` the last slot | yes | 0 of 2, two held |
| grant `asset-000002` into a full zone | **no** — all 2 slots are held | 0 of 2 |
| release `asset-000000` | yes | 1 of 2, one held |
| release it again | **no** — it holds no clearance | 1 of 2 |

`consistent: true` at every step.&emsp;**Measured** over 100 repeats: a grant's p50 is 1.31 ms and its maximum 5.07 ms, a release's 1.24 ms and 4.07 ms.

**The contention run is what a semaphore exists for.**&emsp;Asking concurrently with 8, 16 and 32 askers against a capacity of 2 granted exactly 2 every time, with no errors, and the two winners differed between runs; a lightweight transaction cannot express this, because the count and the holder live in different partitions.

**What this does not measure.**&emsp;One node, `replication_factor: 1`.&emsp;Accord's cost is the wide-area round trip it saves, and a single replica pays no round trip at all, so these figures are a floor and carry nothing about consensus at scale.&emsp;CEP-15 describes its own implementation as "incomplete and not ready for production use".

### Example Application (OLTP) SQL

GEICO's [cassandra-sql](https://github.com/geico/cassandra-sql) runs here, on the **SQL** subtab of the Transactions page and at `/api/sql-console`.&emsp;It speaks the Postgres wire protocol, plans with Apache Calcite, and stores rows in Cassandra as an ordered key-value encoding of its own.&emsp;An application connects with `psql` or any Postgres driver and gets joins, subqueries, aggregates over non-key columns, and multi-statement transactions.

**It reads its own three keyspaces and not `demo.events`, so it is absent from the five-path comparison.**&emsp;A timing beside those five would compare different data.&emsp;Its tables are Accord tables: `DESCRIBE TABLE cassandra_sql.kv_store` reports `transactional_mode = 'full'`, which is what its transactions are built on.

The schema is this repository's own, and it is the fleet's: five tables named `operators`, `drones`, `zones`, `flights` and `flight_legs`, with two ENUM types, a sequence, four foreign keys and two indexes, seeded with five Norwegian operators, eight drones under `asset-NNNNNN` serials and the three real Oslo zones the map draws.&emsp;**No row here is a copy of a `demo` row.**&emsp;These are cassandra-sql's own tables under its own encoding, written by this page and by nothing else; the fleet names are there so the two data models read as one domain, not because anything synchronises them.

The statement below is the one Cassandra has no answer to:

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

Replace `COMMIT` with `ROLLBACK` and no row is left behind, which is what shows the writes were held rather than applied as they went.&emsp;The Accord section above conditions a write on other partitions; this one discards a whole multi-table write on a client's change of mind, and neither a CQL batch nor a lightweight transaction can do that.

**Measured** on the running stack, five runs of each statement, in three sweeps, against a freshly restarted service.&emsp;The columns are the two warm sweeps' medians, and they are quoted as a pair rather than averaged so that their agreement is visible:

| Statement | p50, two warm sweeps | Worst single run, first sweep | Rows |
| --- | --- | --- | --- |
| the transaction above | 31.0 / 34.0 ms | 38.3 ms | no result set |
| the same, rolled back | 9.3 / 9.4 ms | 14.9 ms | 0 |
| four joins, over legs, flights, operators, drones and zones | 19.1 / 15.2 ms | 21.6 ms | 2 |
| `GROUP BY airframe` with five aggregates | 7.5 / 7.4 ms | 13.4 ms | 4 |
| drones above the register's own average range | 12.5 / 12.6 ms | 18.5 ms | 4 |
| `GROUP BY` with `HAVING`, over a join | 7.8 / 8.7 ms | 11.1 ms | 1 |
| `EXPLAIN` of the join | 4.0 / 3.3 ms | 240.0 ms | 32 |

These are tables of five and eight rows, so read the warm figures as the cost of planning and of one round trip rather than as a throughput measure.&emsp;The two transactions climb the same two counters on every run, so each run changes the rows the next would read; the timings are unaffected and the row counts are not cumulative, the flight's primary key overwriting rather than duplicating.

**One statement pays a first-use cost, and it is `EXPLAIN`.**&emsp;An earlier sweep on a warm service found no first-sweep effect at all, which is what a service that has already answered every statement shape should show; the sweep above was therefore taken after restarting `accord-sql`, and there `EXPLAIN`'s first run cost 240.0 ms against a 3.3 ms warm median, a factor of 70.&emsp;No other statement's first run separated from its own median, and `EXPLAIN` ran last of the seven, so the six shapes before it had already warmed whatever a process warms.&emsp;A previous edition of this table read the whole first sweep as several times slower and called the cause unestablished; on the drone schema the effect is one statement's, once per service.

**What it does not hold.**&emsp;The project calls itself a proof of concept, "not production-ready", at "~40% (core features only)" SQL compliance, and warns that "Accord doesn't support variable sized keys — Byte Order Partitioner + Accord is poorly tested, journals are not compacting, gets slower over time".&emsp;That last warning does not apply here: this stack runs Murmur3, and no code under `src/main/` reads the partitioner (see below).&emsp;Measured against the service itself on the schema above:

- **A bound parameter of an integer type silently returns no rows.**&emsp;`WHERE operator_id = 1001` written as a literal returns `Oslo Survey AS`; the same statement binding the integer 1001 returns an empty list, and raises nothing.&emsp;Binding the string `"1001"` returns the row, and a text column binds correctly either way, so the comparison is a text one that a typed bind misses.&emsp;The console therefore offers no parameters, because offering one would offer a silent wrong answer.
- **A duplicate PRIMARY KEY overwrites**, and a **FOREIGN KEY**, **NOT NULL** and an **ENUM** are each accepted and not enforced.&emsp;A flight naming drone 9999 is stored although no such drone exists, and a status declared `flight_status` stores the string `'nonsense'`.&emsp;**UNIQUE is held**, by name: a second seed is refused with "UNIQUE constraint violation: operators_licence_unique on columns (licence)", which is why the page carries a Reset button and why re-running the schema does not restore it.&emsp;So the layer can hold a constraint over Cassandra, and the other three are a gap in the prototype rather than something the storage engine forbids.
- **Arithmetic promotes an integer column to a double.**&emsp;Drone 2003's battery cycles read back `74` as seeded and `75.0` once the transaction's `SET battery_cycles = battery_cycles + 1` has run.&emsp;It is the arithmetic and not the `UPDATE`: `SET battery_cycles = 74` puts back `74`.&emsp;`DECIMAL` is a double as well, so a `DECIMAL(10,2)` fee of `18.00` reads back `18.0`; money is therefore inexact here.&emsp;Every value arrives as text, and `SELECT 1/0` answers `Infinity` rather than raising.
- **`COUNT(*)` over an empty table raises** rather than answering zero: "Aggregation failed: Index 0 out of bounds for length 0".&emsp;Inside a `UNION ALL` that failure takes the whole statement with it, so the page counts one table per statement.&emsp;`UNION ALL` itself is sound, contrary to what this file said before.

**Four defects are join defects, and the page reproduces all four beside a control.**&emsp;Single-table arithmetic and single-table `ORDER BY` are both exact, which is what places each of these in the join and not in the expression:

1. **A column name held by two joined tables resolves to one of them for the whole statement**, and an alias does not help.&emsp;`l.distance_km` over `flight_legs` joined to `flights` answers the flight's 21.4 for both legs; dropping the `flights` join answers 6.2 and 15.2.
2. **`ORDER BY` is ignored on a grouped result.**&emsp;`GROUP BY airframe ORDER BY airframe` and `ORDER BY n DESC` return the same engine-chosen order.&emsp;The same clause on an ungrouped `SELECT` sorts correctly.
3. **Binary arithmetic across two joined tables returns its right-hand operand and discards the operator.**&emsp;`SUM(l.dwell_min * z.fee_per_min)` answers 45.0 and 18.0, which are the fees; reversing the operands answers the dwells, and `+` behaves as `*` does.&emsp;A flat literal rate in place of the joined column gives the correct product.
4. **Arithmetic against a literal inside a join projection drops the column.**&emsp;`SELECT l.leg_no, l.dwell_min * 1, z.capacity` returns two columns, `leg_no` and `capacity`, so a client reading by position gets the wrong one.

The presets are written around all four, and each says in its own description what it therefore omits.&emsp;CI prints these and asserts nothing about them, because an assertion on a defect fails on the release that fixes it; it does assert each control, since a control that raised would leave its probe evidencing nothing.

Two more findings, both about resetting the schema.&emsp;**`DROP TYPE` ignores its `IF EXISTS`** and raises "DROP TYPE failed: ENUM type does not exist" exactly as the bare form does, where `DROP TABLE IF EXISTS` and `DROP SEQUENCE IF EXISTS` are silent, and **`DROP INDEX` is unimplemented in both forms**, answering "Unsupported SQL type"; a `DROP TABLE` takes the table's indexes with it, which is how the reset gets rid of them.&emsp;And **the ENUM declarations do not survive a restart of the service although the tables do**: after restarting the container the five `DROP TABLE` statements each took real time and succeeded, so their definitions had persisted into Cassandra, while both `DROP TYPE` statements reported the type missing.&emsp;A caller must therefore judge a reset by its `CREATE` and `INSERT` statements rather than by an error count.

Three notes for anyone reproducing this.&emsp;The Postgres port is `private static final int POSTGRES_PORT = 5432` in the source and is not configurable, whatever the project's README implies.

**One patch is needed to run it in a container at all**, and `accord-sql/patches/` holds the whole diff.&emsp;cassandra-sql opens two Cassandra sessions: the one in `CassandraConfig` reads `cassandra.contact-points`, and the one in `CassandraExecutor` hard-codes `localhost:9042`.&emsp;The second is a `@Component` with an unconditional `@PostConstruct`, so with Cassandra in another container the bean fails and the application never starts.&emsp;The patch gives it the four properties the first session already reads.&emsp;It is worth upstreaming and has not been yet.

**The partitioner needed no patch**, which is the one thing about the requirement worth stating.&emsp;The project's prerequisites demand `ByteOrderedPartitioner` and this stack runs Murmur3: no code under `src/main/` reads the partitioner, the one range scan is legal under Murmur3, and row order is discarded anyway.&emsp;`CassandraConfigTest` does assert that `system_views.settings` reports the byte-ordered partitioner, and that test therefore fails here, which is why the build runs `-x test`.&emsp;Byte-ordered partitioning was measured here and rejected for the rest of the stack: it costs the `spark` and `spark_bulk` paths, both of which refuse it in libraries this repository does not own, and the vector search page, because storage-attached indexes refuse it too.

### The schema explorer

The third subtab reads both data models from the engines that own them, at `/api/schema/cql` and `/api/schema/sql`.&emsp;Two routes rather than one, so that a stopped `accord-sql` blanks half the page instead of all of it.

**`DESCRIBE KEYSPACE demo` works through the Python driver, server-side, in one round trip**, which is what makes the CQL side cheap: 16 rows, each with a `create_statement`, covering the keyspace, its 14 tables and its one index.&emsp;`transactional_mode` is in that text although `system_schema.tables` has no such column, so the route reads the mode from the statement and the key structure from `system_schema.columns`, whose `kind`, `position` and `clustering_order` are what a `PRIMARY KEY` line needs.&emsp;It currently reports six tables as `full` and eight as `off`, with `payload_vector_idx` a `StorageAttachedIndex` on `drone_text_embeddings.payload_vector`.&emsp;A previous edition of this repository's notes said a script must parse `DESCRIBE` output or ask behaviourally without establishing that the first works; it does.

**On the cassandra-sql side the catalog is partly stale, and the route says which parts.**&emsp;It reads `pg_class WHERE relkind = 'r'` and one `pg_attribute` per `oid`, which are accurate: exactly the five live tables and their 40 columns, in `attnum` order.&emsp;It does not read `pg_tables`, which still lists `customers`, `orders`, `order_items` and `products` long after they were dropped, and it reports that staleness as a warning rather than hiding it.&emsp;Three more gaps come back the same way: there is no `information_schema`, `pg_constraint` is empty, so `UNIQUE` is the one constraint this engine enforces and the one its catalog does not report, and `pg_enum` and `pg_sequence` do not exist although the schema declares two ENUMs and a sequence.&emsp;The route also names the three keyspaces those rows encode into, so a reader can see this is SQL over Cassandra rather than a second database.&emsp;**`pg_attribute` is created on first use**, so a service that has restarted refuses every read of it while `pg_class` still answers, and the route then reports five tables with no columns and a note each; pressing Reset is the `CREATE TABLE` that registers it.

**The route joins in Python**, and the reason is the section above: this engine's joins are four of the defects the page reproduces.

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
- cassandra-sql :: Postgres wire protocol and a Calcite-planned dialect over Cassandra, <https://github.com/geico/cassandra-sql>
