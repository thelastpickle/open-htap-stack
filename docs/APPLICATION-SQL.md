# Application SQL over Cassandra: GEICO's cassandra-sql

GEICO's [cassandra-sql](https://github.com/geico/cassandra-sql) runs here, on the **SQL** subtab of the Transactions page and at `/api/sql-console`.&emsp;It speaks the Postgres wire protocol, plans with Apache Calcite, and stores rows in Cassandra as an ordered key-value encoding of its own.&emsp;An application connects with `psql` or any Postgres driver and gets joins, subqueries, aggregates over non-key columns, and multi-statement transactions.

**It reads its own three keyspaces and not `demo.events`, so it is absent from the five-path comparison.**&emsp;A timing beside those five would compare different data.&emsp;Its tables are Accord tables: `DESCRIBE TABLE cassandra_sql.kv_store` reports `transactional_mode = 'full'`, which is what its transactions are built on.

The schema is this repository's own, and it is the fleet's: five tables named `operators`, `drones`, `zones`, `flights` and `flight_legs`, with two ENUM types, a sequence, four foreign keys and two indexes, seeded with five Norwegian operators, eight drones under `asset-NNNNNN` serials and the three real Oslo zones the map draws.&emsp;**No row here is a copy of a `demo` row.**&emsp;These are cassandra-sql's own tables under its own encoding, written by this page and by nothing else; the fleet names are there so the two data models read as one domain, not because anything synchronises them.

## The statement Cassandra has no answer to

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

Replace `COMMIT` with `ROLLBACK` and no row is left behind, which is what shows the writes were held rather than applied as they went.&emsp;An Accord transaction conditions a write on other partitions; this one discards a whole multi-table write on a client's change of mind, and neither a CQL batch nor a lightweight transaction can do that.

## What it costs

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

## What it does not hold

The project calls itself a proof of concept, "not production-ready", at "~40% (core features only)" SQL compliance, and warns that "Accord doesn't support variable sized keys — Byte Order Partitioner + Accord is poorly tested, journals are not compacting, gets slower over time".&emsp;That last warning does not apply here: this stack runs Murmur3, and no code under `src/main/` reads the partitioner (see below).&emsp;Measured against the service itself on the schema above:

- **A bound parameter of an integer type silently returns no rows.**&emsp;`WHERE operator_id = 1001` written as a literal returns `Oslo Survey AS`; the same statement binding the integer 1001 returns an empty list, and raises nothing.&emsp;Binding the string `"1001"` returns the row, and a text column binds correctly either way, so the comparison is a text one that a typed bind misses.&emsp;The console therefore offers no parameters, because offering one would offer a silent wrong answer.
- **A duplicate PRIMARY KEY overwrites**, and a **FOREIGN KEY**, **NOT NULL** and an **ENUM** are each accepted and not enforced.&emsp;A flight naming drone 9999 is stored although no such drone exists, and a status declared `flight_status` stores the string `'nonsense'`.&emsp;**UNIQUE is held**, by name: a second seed is refused with "UNIQUE constraint violation: operators_licence_unique on columns (licence)", which is why the page carries a Reset button and why re-running the schema does not restore it.&emsp;So the layer can hold a constraint over Cassandra, and the other three are a gap in the prototype rather than something the storage engine forbids.
- **Arithmetic promotes an integer column to a double.**&emsp;Drone 2003's battery cycles read back `74` as seeded and `75.0` once the transaction's `SET battery_cycles = battery_cycles + 1` has run.&emsp;It is the arithmetic and not the `UPDATE`: `SET battery_cycles = 74` puts back `74`.&emsp;`DECIMAL` is a double as well, so a `DECIMAL(10,2)` fee of `18.00` reads back `18.0`; money is therefore inexact here.&emsp;Every value arrives as text, and `SELECT 1/0` answers `Infinity` rather than raising.
- **`COUNT(*)` over an empty table raises** rather than answering zero: "Aggregation failed: Index 0 out of bounds for length 0".&emsp;Inside a `UNION ALL` that failure takes the whole statement with it, so the page counts one table per statement.&emsp;`UNION ALL` itself is sound, contrary to what this repository's notes said before.

## Four defects are join defects

The page reproduces all four beside a control.&emsp;Single-table arithmetic and single-table `ORDER BY` are both exact, which is what places each of these in the join and not in the expression:

1. **A column name held by two joined tables resolves to one of them for the whole statement**, and an alias does not help.&emsp;`l.distance_km` over `flight_legs` joined to `flights` answers the flight's 21.4 for both legs; dropping the `flights` join answers 6.2 and 15.2.
2. **`ORDER BY` is ignored on a grouped result.**&emsp;`GROUP BY airframe ORDER BY airframe` and `ORDER BY n DESC` return the same engine-chosen order.&emsp;The same clause on an ungrouped `SELECT` sorts correctly.
3. **Binary arithmetic across two joined tables returns its right-hand operand and discards the operator.**&emsp;`SUM(l.dwell_min * z.fee_per_min)` answers 45.0 and 18.0, which are the fees; reversing the operands answers the dwells, and `+` behaves as `*` does.&emsp;A flat literal rate in place of the joined column gives the correct product.
4. **Arithmetic against a literal inside a join projection drops the column.**&emsp;`SELECT l.leg_no, l.dwell_min * 1, z.capacity` returns two columns, `leg_no` and `capacity`, so a client reading by position gets the wrong one.

The presets are written around all four, and each says in its own description what it therefore omits.&emsp;CI prints these and asserts nothing about them, because an assertion on a defect fails on the release that fixes it; it does assert each control, since a control that raised would leave its probe evidencing nothing.

## Resetting the schema

**`DROP TYPE` ignores its `IF EXISTS`** and raises "DROP TYPE failed: ENUM type does not exist" exactly as the bare form does, where `DROP TABLE IF EXISTS` and `DROP SEQUENCE IF EXISTS` are silent, and **`DROP INDEX` is unimplemented in both forms**, answering "Unsupported SQL type"; a `DROP TABLE` takes the table's indexes with it, which is how the reset gets rid of them.

**The ENUM declarations do not survive a restart of the service although the tables do.**&emsp;After restarting the container the five `DROP TABLE` statements each took real time and succeeded, so their definitions had persisted into Cassandra, while both `DROP TYPE` statements reported the type missing.&emsp;A caller must therefore judge a reset by its `CREATE` and `INSERT` statements rather than by an error count.

## Notes for anyone reproducing this

The Postgres port is `private static final int POSTGRES_PORT = 5432` in the source and is not configurable, whatever the project's README implies.

**One patch is needed to run it in a container at all**, and `accord-sql/patches/` holds the whole diff.&emsp;cassandra-sql opens two Cassandra sessions: the one in `CassandraConfig` reads `cassandra.contact-points`, and the one in `CassandraExecutor` hard-codes `localhost:9042`.&emsp;The second is a `@Component` with an unconditional `@PostConstruct`, so with Cassandra in another container the bean fails and the application never starts.&emsp;The patch gives it the four properties the first session already reads.&emsp;It is worth upstreaming and has not been yet.

**The partitioner needed no patch**, which is the one thing about the requirement worth stating.&emsp;The project's prerequisites demand `ByteOrderedPartitioner` and this stack runs Murmur3: no code under `src/main/` reads the partitioner, the one range scan is legal under Murmur3, and row order is discarded anyway.&emsp;`CassandraConfigTest` does assert that `system_views.settings` reports the byte-ordered partitioner, and that test therefore fails here, which is why the build runs `-x test`.&emsp;Byte-ordered partitioning was measured here and rejected for the rest of the stack: it costs the `spark` and `spark_bulk` paths, both of which refuse it in libraries this repository does not own, and the vector search page, because storage-attached indexes refuse it too.
