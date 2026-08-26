# Accord transactions: conditions that span partitions

Accord runs here, on the **Accord** subtab of the Transactions page and at `/api/transactions`.&emsp;The stack starts with `accord.enabled: true`, and six tables are created with `transactional_mode='full'`: `sessions_open`, `session_seq_applied` and `session_timeline` for the sequence demonstration, and `zone_occupancy`, `zone_clearance` and `drone_clearance` for the clearance one.&emsp;`events` is deliberately left out, so consensus is never in front of 2,000 writes a second; see [DATA-MODEL.md](DATA-MODEL.md) for why that matters more than it sounds.

## Idempotent replay: a sequence across three tables

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

## Airspace clearance: a semaphore across three tables

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

## What this does not measure

One node, `replication_factor: 1`.&emsp;Accord's cost is the wide-area round trip it saves, and a single replica pays no round trip at all, so these figures are a floor and carry nothing about consensus at scale.&emsp;CEP-15 describes its own implementation as "incomplete and not ready for production use".

## Operating notes

Accord makes an unclean shutdown fatal by default.&emsp;It writes a `started` marker into `cassandra-data/accord_journal/` and a `stopped` marker on a clean stop, and finding the first without the second it refuses to open the node.&emsp;`cassandra/entrypoint.sh` therefore sets `accord.journal.stop_marker_failure_policy: ALLOW_UNSAFE_STARTUP`, which gives up the guarantee that this node knows every vote it cast.&emsp;At `replication_factor: 1` no peer can hold a conflicting vote, so **a multi-node cluster must not carry that setting**.

`transactional_mode` cannot be added to an existing table, so changing which tables opt in costs a wipe: `./stop-and-clean-data-and-schema.sh`.&emsp;The mode can be read back with `DESCRIBE KEYSPACE demo`, which the schema subtab does; `system_schema.tables` has no such column.
