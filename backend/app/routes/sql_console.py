"""cassandra-sql — a Postgres-dialect SQL console, with transactions, over Accord.

Kept apart from query.py for the same reason transactions.py is: that module's
``_validate`` rejects every write keyword, which is what keeps the read console
honest.  This console is mostly writes, so it needs its own route rather than a
hole in that check.  Nothing here touches ``demo.events``: cassandra-sql stores
SQL rows in its own keyspaces under an encoding of its own, so it is not a sixth
way to read the demo's data and it appears in no comparison.

The schema below is this repository's own drone-operations one, not GEICO's
``demo-ecommerce.sh``, which is what shipped first and has been removed.  A page in
this dashboard about customers and loyalty points said nothing about the fleet every
other page reads.  Five tables now hold operators, drones, zones, flights and the
legs a flight flies through restricted airspace, with the same three Oslo zones the
map draws and the Accord subtab admits drones to.

Every behaviour recorded here was re-measured on that schema, at cassandra-sql
revision a0257ec9a22ff84daaf6f529ae8b523fdc45b431, and each shapes either a route or
a preset.

Three change the interface:

- **A bound parameter silently returns no rows.**  ``WHERE operator_id = 1001``
  returns the row; the same statement binding the integer 1001 returns nothing and
  raises nothing.  Binding the string "1001" does return the row, so the comparison
  is a text one that a typed bind misses.  Every statement is therefore sent as one
  complete string and the presets carry literals.
- **``SELECT COUNT(*)`` over an empty table raises** rather than answering zero:
  "Aggregation failed: Index 0 out of bounds for length 0".  So /tables reports a
  count of null for ``flights`` and ``flight_legs`` before the transaction preset has
  run, and says which.  Inside a ``UNION ALL`` that failure takes the whole
  statement with it, which is why /tables asks one table at a time.
- **Every value arrives as text, and the text is not always the value stored.**  An
  ``INT`` column reads back "75" as inserted and "75.0" after an ``UPDATE`` that
  added to it, though "5" again after one that assigned a literal, so it is the
  arithmetic and not the ``UPDATE`` that promotes it.  ``DECIMAL`` is held as a
  double: 4.20 reads back "4.2".

Four are join defects, and all four are join-only: the same arithmetic on one table
is exact, and ``ORDER BY`` on an ungrouped SELECT is exact.  They are the reason two
presets had to be rewritten and the reason the ``quirks`` preset exists:

- **A column name that appears in more than one joined table resolves to one table
  for the whole statement, and the qualifier is ignored.**  ``SELECT l.distance_km``
  over ``flight_legs l JOIN flights f`` answers the flight's 21.4 for both legs
  rather than 6.2 and 15.2; dropping the ``flights`` join answers correctly.  An
  alias does not help, and the other table's column need not be projected at all.
  Project both and the two columns collapse into one; alias them distinctly and two
  columns come back holding the same value.  Which table wins is the order the join
  names them: the second one.
- **``ORDER BY`` is ignored on a grouped result.**  ``GROUP BY airframe ORDER BY
  airframe`` answers quadcopter, vtol, fixed-wing, octocopter, and ``ORDER BY n
  DESC`` answers the same order.
- **Binary arithmetic across two joined tables returns its right-hand operand** and
  discards the operator.  ``l.dwell_min * z.fee_per_min`` answers 18.0 and 45.0,
  which are the fees; reversing the operands answers 9.0 and 12.0, which are the
  dwells; and ``+`` behaves the same as ``*``.
- **Arithmetic against a literal inside a join projection drops the column
  entirely**, so a client reading by position gets the wrong column.
  ``SELECT l.leg_no, l.dwell_min * 1.0 AS times_one, z.capacity`` returns only
  ``leg_no`` and ``capacity``.

Four are constraints the SQL layer accepts and does not hold, which is Cassandra's
write path showing through a Postgres dialect.  Each was reproduced on a throwaway
row and the row deleted afterwards:

- **A duplicate PRIMARY KEY overwrites.**  Inserting flight 9900 twice with
  different totals leaves one row holding the second, with no error and no
  constraint violation.  So the presets are idempotent by upsert, not by refusal.
- **A FOREIGN KEY is accepted and not enforced.**  All four declared here succeed,
  and a flight naming operator 9999 and drone 9999 is then stored although neither
  exists.
- **NOT NULL is accepted and not enforced.**  An operator inserted with no ``name``
  is stored.
- **An ENUM is accepted and not enforced.**  ``status``, declared ``flight_status``,
  stores the string 'nonsense'.

**UNIQUE is the one declared constraint that is held**, and holding it is what makes
the seed non-idempotent: a second ``POST /schema`` is refused with "UNIQUE
constraint violation: operators_licence_unique on columns (licence)".  That is why
``POST /reset`` exists.  Worth stating the other way round too: the layer *can* hold
a constraint above the storage engine, so the foreign key going unenforced is a gap
in this prototype rather than an impossibility.

What does hold is the part the page exists to show.  ``BEGIN; INSERT ...;
ROLLBACK;`` leaves no row behind, so the write is buffered until COMMIT rather than
applied as it goes.

The project states wider limits still, and this repository quotes them verbatim in
`README.md` rather than softening them: a proof of concept, "~40% (core features
only)" SQL compliance, and journals that do not compact.
"""
import threading
import time
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.config import settings
from app.db.accord_sql_client import accord_sql_client
from app.models import SqlConsoleResult, SqlPreset, SqlQuirk, SqlStatementResult

router = APIRouter(prefix="/api/sql-console", tags=["sql-console"])

# One console statement at a time.  The client holds a single connection, so a
# second caller would queue on its lock anyway; refusing instead says so.
_lock = threading.Lock()

# The largest result the page will carry back.  cassandra-sql widens its own scan
# limit to 100,000 rows whenever ORDER BY is present, so an unbounded SELECT here
# is not a small thing to send to a browser.
MAX_ROWS = 500


class SqlRequest(BaseModel):
    sql: str = Field(..., min_length=1, description="One complete SQL string")


# ──────────────────────── The schema ────────────────────────
#
# Sent one statement at a time rather than as one string, so that a second run
# reports "already exists" per statement and continues.  cassandra-sql has no
# CREATE TABLE IF NOT EXISTS, so re-running the schema is how the page recovers
# from a half-created one.

SCHEMA_STATEMENTS: List[str] = [
    "CREATE TYPE flight_status AS ENUM ('planned', 'cleared', 'airborne', 'landed', 'aborted');",
    "CREATE TYPE flight_purpose AS ENUM ('survey', 'inspection', 'delivery', 'training', 'emergency');",
    "CREATE SEQUENCE flight_id_seq START WITH 9000 INCREMENT BY 1;",
    """CREATE TABLE operators (
    operator_id BIGINT PRIMARY KEY,
    licence TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    base_city TEXT,
    country TEXT,
    certified_at BIGINT,
    flight_hours INT DEFAULT 0
);""",
    """CREATE TABLE drones (
    drone_id BIGINT PRIMARY KEY,
    serial TEXT UNIQUE NOT NULL,
    model TEXT NOT NULL,
    airframe TEXT,
    mass_kg DECIMAL(10,2) NOT NULL,
    max_range_km DECIMAL(10,2),
    battery_cycles INT DEFAULT 0,
    tags TEXT[],
    registered_at BIGINT
);""",
    """CREATE TABLE zones (
    zone_id BIGINT PRIMARY KEY,
    zone_code TEXT UNIQUE NOT NULL,
    zone_name TEXT NOT NULL,
    severity TEXT,
    capacity INT,
    fee_per_min DECIMAL(10,2)
);""",
    """CREATE TABLE flights (
    flight_id BIGINT PRIMARY KEY,
    operator_id BIGINT NOT NULL,
    drone_id BIGINT NOT NULL,
    departed_at BIGINT NOT NULL,
    status flight_status DEFAULT 'planned',
    purpose flight_purpose,
    distance_km DECIMAL(10,2),
    duration_min DECIMAL(10,2),
    energy_wh DECIMAL(10,2),
    fee_nok DECIMAL(10,2),
    route_summary TEXT
);""",
    """CREATE TABLE flight_legs (
    leg_id BIGINT PRIMARY KEY,
    flight_id BIGINT NOT NULL,
    zone_id BIGINT NOT NULL,
    leg_no INT NOT NULL,
    distance_km DECIMAL(10,2) NOT NULL,
    dwell_min DECIMAL(10,2) DEFAULT 0.00,
    leg_energy_wh DECIMAL(10,2)
);""",
    # The foreign keys are the interesting part of the DDL, because Cassandra has
    # no such constraint: cassandra-sql holds it above the storage engine.  It
    # accepts them and does not enforce them, which the docstring above records.
    "ALTER TABLE flights ADD CONSTRAINT fk_flight_operator FOREIGN KEY (operator_id) REFERENCES operators(operator_id);",
    "ALTER TABLE flights ADD CONSTRAINT fk_flight_drone FOREIGN KEY (drone_id) REFERENCES drones(drone_id);",
    "ALTER TABLE flight_legs ADD CONSTRAINT fk_leg_flight FOREIGN KEY (flight_id) REFERENCES flights(flight_id);",
    "ALTER TABLE flight_legs ADD CONSTRAINT fk_leg_zone FOREIGN KEY (zone_id) REFERENCES zones(zone_id);",
    "CREATE INDEX idx_flights_operator ON flights(operator_id);",
    "CREATE INDEX idx_drones_airframe ON drones(airframe);",
]

# The rows are fixed, and they are cassandra-sql's own.  Nothing copies them from
# demo.drone_latest_status and nothing keeps them in step with it: cassandra-sql
# stores its rows under an ordered key-value encoding in keyspaces of its own, so a
# copy would be an ETL job, which is the one thing this repository exists to argue
# against.  The serials and the three zone codes are the live fleet's and the map's
# so that a reader can see which drone and which airspace is meant, and the page says
# plainly that these are a register beside the fleet rather than a view of it.
SEED_STATEMENTS: List[str] = [
    """INSERT INTO operators (operator_id, licence, name, base_city, country, certified_at, flight_hours)
VALUES
    (1001, 'NO-RPAS-1001', 'Oslo Survey AS', 'Oslo', 'NO', 1704067200000, 1450),
    (1002, 'NO-RPAS-1002', 'Fjord Inspection AS', 'Bergen', 'NO', 1704153600000, 890),
    (1003, 'NO-RPAS-1003', 'Nordic Air Logistics', 'Trondheim', 'NO', 1704240000000, 2310),
    (1004, 'NO-RPAS-1004', 'Politiets Droneenhet', 'Oslo', 'NO', 1704326400000, 640),
    (1005, 'SE-RPAS-2001', 'Kiruna Aerial Mapping', 'Kiruna', 'SE', 1704412800000, 120);""",
    """INSERT INTO drones (drone_id, serial, model, airframe, mass_kg, max_range_km, battery_cycles, tags, registered_at)
VALUES
    (2001, 'asset-000000', 'Kestrel M4', 'quadcopter', 4.20, 18.00, 312, ARRAY['lidar', 'survey'], 1704067200000),
    (2002, 'asset-000001', 'Kestrel M4', 'quadcopter', 4.20, 18.00, 118, ARRAY['thermal'], 1704067200000),
    (2003, 'asset-000002', 'Kestrel M8', 'octocopter', 9.80, 26.00, 74, ARRAY['lidar', 'heavy-lift'], 1704067200000),
    (2004, 'asset-000003', 'Harrier VT', 'vtol', 13.50, 92.00, 41, ARRAY['long-range', 'delivery'], 1704067200000),
    (2005, 'asset-000004', 'Harrier VT', 'vtol', 13.50, 92.00, 27, ARRAY['long-range'], 1704067200000),
    (2006, 'asset-000005', 'Skua F2', 'fixed-wing', 6.40, 140.00, 205, ARRAY['mapping'], 1704067200000),
    (2007, 'asset-000006', 'Skua F2', 'fixed-wing', 6.40, 140.00, 163, ARRAY['mapping', 'coastal'], 1704067200000),
    (2008, 'asset-000007', 'Wren N1', 'quadcopter', 1.10, 6.00, 588, ARRAY['indoor', 'training'], 1704067200000);""",
    # The same three zones the map draws and the Accord clearance ledger admits
    # drones to, with the same capacities the sink seeds, so the two halves of the
    # page are talking about the same airspace.  What differs is what holds the
    # capacity: here it is a column that any statement may overwrite, and there it is
    # a transaction that no concurrent caller can oversubscribe.
    """INSERT INTO zones (zone_id, zone_code, zone_name, severity, capacity, fee_per_min)
VALUES
    (3001, 'zone-oslo-airport', 'Oslo Lufthavn Gardermoen', 'critical', 2, 45.00),
    (3002, 'zone-royal-palace', 'Det Kongelige Slott', 'critical', 3, 60.00),
    (3003, 'zone-fornebu', 'Fornebu Tech Park', 'warning', 5, 18.00);""",
]

# ──────────────────────── The reset ────────────────────────
#
# The seed is not idempotent, because UNIQUE is the one declared constraint this
# engine holds: a second POST /schema is refused with "UNIQUE constraint violation:
# operators_licence_unique on columns (licence)".  And the oversubscribe preset
# decrements zones.capacity without reading it, which no re-seed can restore.  So the
# page needs a way back to the seeded state, and this is it.
#
# Children before parents, though nothing depends on the order: the foreign keys are
# accepted and not enforced, so a drop cannot be refused for a dangling reference.
# The order is here so that the statement list still reads correctly against an engine
# that one day does enforce them.
#
# Three measurements shape the list.  DROP TABLE removes the table's indexes with it,
# which matters because **DROP INDEX is not implemented in either form**: both `DROP
# INDEX name` and `DROP INDEX IF EXISTS name` answer "Unsupported SQL type".  DROP
# TABLE IF EXISTS and DROP SEQUENCE IF EXISTS are honoured and silent on a missing
# object.  **DROP TYPE ignores its IF EXISTS** and raises "DROP TYPE failed: ENUM type
# does not exist" exactly as the bare form does, so on a first-ever reset those two
# statements report an error while the rest succeed; the route reports per statement
# and continues rather than treating that as a failure.
#
# A restart of accord-sql produces the same pair, and finding out why cost a
# measurement: **the ENUM declarations do not survive a restart, and the tables do**.
# After restarting the container, the five DROP TABLE statements each took real time and
# succeeded, so their definitions had persisted into Cassandra; the two DROP TYPE
# statements answered "ENUM type does not exist", so the engine holds its ENUM registry
# in memory.  A caller must therefore not read `error_count` as the reset's verdict.
# Judge the CREATE and INSERT statements, and let a DROP fail: a DROP that fails is the
# engine saying the object was not there, which is the state a reset wants anyway.
RESET_STATEMENTS: List[str] = [
    "DROP TABLE IF EXISTS flight_legs;",
    "DROP TABLE IF EXISTS flights;",
    "DROP TABLE IF EXISTS drones;",
    "DROP TABLE IF EXISTS zones;",
    "DROP TABLE IF EXISTS operators;",
    "DROP TYPE IF EXISTS flight_status;",
    "DROP TYPE IF EXISTS flight_purpose;",
    "DROP SEQUENCE IF EXISTS flight_id_seq;",
]

# ──────────────────────── The presets ────────────────────────

PRESETS: List[SqlPreset] = [
    SqlPreset(
        id="transaction",
        title="File a flight, in one transaction",
        description=(
            "One flight, in five statements that commit together: the flight, its two "
            "legs through restricted airspace, the battery cycle the drone spent and the "
            "hour the operator flew.  Sent as one string, because cassandra-sql executes "
            "the whole string as one unit.  Run it twice and the flight does not "
            "duplicate, because a repeated primary key overwrites here rather than being "
            "refused; the two increments do apply again, so the cycle count and the "
            "flight hours climb by one each time."
        ),
        sql="""BEGIN;
INSERT INTO flights (flight_id, operator_id, drone_id, departed_at, status, purpose, distance_km, duration_min, energy_wh, fee_nok, route_summary)
VALUES (9001, 1001, 2003, 1704499200000, 'cleared', 'survey', 21.40, 38.00, 742.00, 1290.00, 'Fornebu to Gardermoen, north transit');
INSERT INTO flight_legs (leg_id, flight_id, zone_id, leg_no, distance_km, dwell_min, leg_energy_wh)
VALUES (10001, 9001, 3003, 1, 6.20, 9.00, 198.00), (10002, 9001, 3001, 2, 15.20, 12.00, 544.00);
UPDATE drones SET battery_cycles = battery_cycles + 1 WHERE drone_id = 2003;
UPDATE operators SET flight_hours = flight_hours + 1 WHERE operator_id = 1001;
COMMIT;""",
    ),
    SqlPreset(
        id="rollback",
        title="A flight plan that rolls back",
        description=(
            "File a flight, then discard it.  The SELECT after the ROLLBACK returns no "
            "row, which is what shows the write was held until COMMIT rather than applied "
            "as it went.  Cassandra has no such operation: a CQL batch is atomic but "
            "never undone, so a client that changes its mind has to compensate.  The "
            "column list that comes back is the whole table's, not the three the SELECT "
            "names: an empty result set here is described by the table rather than by the "
            "projection, which a result with rows in it is."
        ),
        sql="""BEGIN;
INSERT INTO flights (flight_id, operator_id, drone_id, departed_at, status, distance_km, route_summary)
VALUES (9099, 1001, 2001, 1704499200000, 'planned', 3.00, 'rolled back');
ROLLBACK;
SELECT flight_id, distance_km, route_summary FROM flights WHERE flight_id = 9099;""",
    ),
    # Every projected name is unique across the four tables, and that is a constraint
    # rather than a style: a column name that appears in two joined tables resolves to
    # the second one named, whatever the qualifier says.  `distance_km` is in both
    # `flights` and `flight_legs`, so the per-leg distance -- the number a reader most
    # wants here -- cannot be projected in this join at all.  `l.distance_km` answers
    # the flight's 21.4 for both legs, an alias does not help, and dropping the
    # `flights` join is what makes it answer 6.2 and 15.2.  The quirks preset shows
    # that, so this one stays correct and says what it omits.
    SqlPreset(
        id="join",
        title="A four-way join",
        description=(
            "Which operator flew which airframe through which restricted zone, over four "
            "tables.  No CQL statement expresses this, and neither does the schema mode of "
            "cassandra-sql itself, whose own documentation says \"No JOINs or "
            "subqueries\".  Run the transaction preset first, or there are no flights to "
            "join to.\n\n"
            "The per-leg distance is missing on purpose.  `distance_km` is a column of "
            "both joined tables, and this engine resolves such a name to one table for "
            "the whole statement, so asking for the leg's distance answers the flight's.  "
            "The quirks preset shows it."
        ),
        sql="""SELECT
    o.name AS operator,
    d.serial,
    d.airframe,
    z.zone_name,
    l.leg_no,
    l.dwell_min,
    f.status
FROM flight_legs l
INNER JOIN flights f ON l.flight_id = f.flight_id
INNER JOIN operators o ON f.operator_id = o.operator_id
INNER JOIN drones d ON f.drone_id = d.drone_id
INNER JOIN zones z ON l.zone_id = z.zone_id;""",
    ),
    SqlPreset(
        id="aggregate",
        title="GROUP BY on a non-key column",
        description=(
            "The statement the CQL path declines, in SQL over cassandra-sql's own rows.  "
            "An airframe is not part of any key, so Cassandra cannot group by it; "
            "cassandra-sql reads the rows and groups them itself."
        ),
        # No ORDER BY, because it would be ignored: this engine discards the clause on a
        # grouped result, and a preset that asked for an order it does not get would be
        # reporting a wrong answer as though it were right.  The rows come back in the
        # engine's own order.  The quirks preset shows the clause being ignored.
        sql="""SELECT
    airframe,
    COUNT(*) AS fleet_count,
    AVG(mass_kg) AS avg_mass_kg,
    MIN(max_range_km) AS min_range_km,
    MAX(max_range_km) AS max_range_km,
    SUM(battery_cycles) AS total_cycles
FROM drones
GROUP BY airframe;""",
    ),
    SqlPreset(
        id="subquery",
        title="A correlated aggregate in the WHERE clause",
        description="Drones that outrange the average of the register they are in.",
        sql="""SELECT serial, model, airframe, mass_kg, max_range_km
FROM drones
WHERE max_range_km > (SELECT AVG(max_range_km) FROM drones)
ORDER BY max_range_km DESC;""",
    ),
    # The fee is reported as a column and the dwell as a sum; the product of the two is
    # not computed, because this engine cannot multiply across two joined tables.
    # `SUM(l.dwell_min * z.fee_per_min)` answered 45.0 and 18.0, which are fee_per_min
    # alone rather than the products 540 and 162: binary arithmetic across a join
    # returns its right-hand operand and discards the operator.  So the two figures are
    # shown separately and the reader multiplies them, which is honest where a wrong
    # total dressed as a right one is not.
    SqlPreset(
        id="having",
        title="GROUP BY with HAVING, over a join",
        description=(
            "Dwell time per zone, keeping only the zones a drone loitered in for more than "
            "ten minutes; on the seeded flight that keeps Gardermoen and drops Fornebu.  "
            "Run the transaction preset first, or the join has no legs to group.\n\n"
            "The fee per minute is a column here rather than a factor.  Multiplying it by "
            "the dwell would be arithmetic across two joined tables, which this engine "
            "answers with one of the two operands; the quirks preset shows it."
        ),
        sql="""SELECT
    z.zone_name,
    z.severity,
    z.fee_per_min,
    COUNT(DISTINCT l.flight_id) AS flight_count,
    SUM(l.dwell_min) AS total_dwell_min
FROM flight_legs l
INNER JOIN zones z ON l.zone_id = z.zone_id
GROUP BY z.zone_name, z.severity, z.fee_per_min
HAVING SUM(l.dwell_min) > 10;""",
    ),
    SqlPreset(
        id="oversubscribe",
        title="The zone capacity this layer will not hold",
        description=(
            "The same three zones the Accord ledger admits drones to, with the same "
            "capacities.  Here the count is an ordinary column and the check is an "
            "ordinary statement, so nothing stops two callers reading the same free slot "
            "and both taking it: the UPDATE below decrements without reading first, and "
            "will run capacity past zero if it is repeated.  That is the difference the "
            "Accord subtab measures, on the same airspace."
        ),
        sql="""UPDATE zones SET capacity = capacity - 1 WHERE zone_code = 'zone-oslo-airport';
SELECT zone_code, zone_name, capacity FROM zones ORDER BY zone_code;""",
    ),
    SqlPreset(
        id="explain",
        title="EXPLAIN",
        description=(
            "The plan Calcite built, which is what shows the SQL layer to be a planner "
            "over a key-value store rather than a translator into CQL."
        ),
        sql="""EXPLAIN SELECT o.name, f.flight_id, f.distance_km
FROM flights f
INNER JOIN operators o ON f.operator_id = o.operator_id
WHERE f.distance_km > 10;""",
    ),
]

# ──────────────────────── The four join defects ────────────────────────
#
# Shown rather than avoided.  Two presets had been reporting a wrong number as though
# it were right, and correcting them silently would have left the page implying that
# this engine answers a join correctly.  It does not, and the four ways it does not are
# reproducible in one statement each.
#
# Each defect is paired with a control, because a defect is only a defect if something
# nearby works: the same arithmetic on one table is exact, and ORDER BY on an ungrouped
# SELECT is exact, so all four are join defects and not arithmetic or sort defects.
#
# The pair cannot be one SQL string.  A multi-statement string returns only its last
# result set here, which is what makes the rollback preset work, so each probe and each
# control is sent on its own.
#
# All measured at cassandra-sql revision a0257ec9a22ff84daaf6f529ae8b523fdc45b431,
# which podman-compose pins; nothing upstream can change under this page without the
# pin changing.  These four are the reason to read the join presets carefully rather
# than a reason to dismiss the engine: it is a proof of concept by its own account.
QUIRKS: List[dict] = [
    {
        "id": "column-resolution",
        "title": "A column name in two joined tables resolves to the wrong one",
        "summary": (
            "distance_km is a column of both flights and flight_legs.  Asking for the "
            "leg's answers the flight's, for every leg, and the l. qualifier is ignored.  "
            "An alias does not help, and the second table named is the one that wins.  "
            "Dropping the flights join is what makes the same projection answer correctly."
        ),
        "expected": "6.2 and 15.2, the two legs' own distances",
        "probe": "SELECT l.leg_no, l.distance_km FROM flight_legs l "
        "INNER JOIN flights f ON l.flight_id = f.flight_id;",
        "control": "SELECT l.leg_no, l.distance_km FROM flight_legs l "
        "INNER JOIN zones z ON l.zone_id = z.zone_id;",
    },
    {
        "id": "grouped-order-by",
        "title": "ORDER BY is ignored on a grouped result",
        "summary": (
            "The clause is accepted and discarded.  ORDER BY n DESC answers the same "
            "order as ORDER BY airframe, so it is not that one key is mishandled: the "
            "sort does not run at all.  The same clause on an ungrouped SELECT is exact."
        ),
        "expected": "fixed-wing, octocopter, quadcopter, vtol",
        "probe": "SELECT airframe, COUNT(*) AS n FROM drones GROUP BY airframe "
        "ORDER BY airframe;",
        "control": "SELECT serial, mass_kg FROM drones ORDER BY mass_kg DESC;",
    },
    {
        "id": "cross-table-arithmetic",
        "title": "Arithmetic across two joined tables returns one operand",
        "summary": (
            "The operator is discarded and the right-hand operand is returned.  Reverse "
            "the operands and the other column comes back; write + instead of * and "
            "nothing changes.  This is what made a fee-per-minute total read as the fee."
        ),
        # The control multiplies by a literal 45.0 rather than by each zone's own fee,
        # because reaching the fee is exactly what needs the join.  So its numbers are
        # not the expected ones: 540.0 and 405.0 at one flat rate, where the join should
        # answer 540.0 for Gardermoen at 45 and 162.0 for Fornebu at 18.  What it shows
        # is that the multiplication itself is exact, which is what makes the join the
        # thing at fault.
        "expected": (
            "540.0 for the Gardermoen leg at 45 NOK/min and 162.0 for the Fornebu leg "
            "at 18; the probe answers the two fees instead"
        ),
        "probe": "SELECT l.leg_no, l.dwell_min * z.fee_per_min AS fee_nok FROM flight_legs l "
        "INNER JOIN zones z ON l.zone_id = z.zone_id;",
        "control": "SELECT leg_no, dwell_min * 45.0 AS fee_at_one_flat_rate FROM flight_legs;",
    },
    {
        "id": "literal-arithmetic-dropped",
        "title": "Arithmetic against a literal inside a join drops the column",
        "summary": (
            "The column is absent from the result, not merely wrong, so a client reading "
            "by position silently reads the next column instead.  This is the worst of the "
            "four for that reason: the other three answer something, and this one answers "
            "a differently shaped row."
        ),
        "expected": "three columns: leg_no, times_one, capacity",
        "probe": "SELECT l.leg_no, l.dwell_min * 1.0 AS times_one, z.capacity FROM flight_legs l "
        "INNER JOIN zones z ON l.zone_id = z.zone_id;",
        "control": "SELECT leg_no, dwell_min * 1.0 AS times_one FROM flight_legs;",
    },
]


# ──────────────────────── Routes ────────────────────────


def _run_one(sql: str) -> SqlStatementResult:
    """Run one string and report it, turning a raised statement into a field.

    A statement that the service rejects is a result and not a server error: the
    console exists partly to show what cassandra-sql refuses.
    """
    try:
        columns, rows, duration_ms = accord_sql_client.execute(sql)
    except Exception as exc:
        return SqlStatementResult(sql=sql, error=str(exc))
    truncated = rows[:MAX_ROWS]
    return SqlStatementResult(
        sql=sql,
        columns=columns,
        rows=[[None if value is None else str(value) for value in row] for row in truncated],
        row_count=len(rows),
        duration_ms=round(duration_ms, 2),
    )


def _run_many(statements: List[str]) -> SqlConsoleResult:
    started = time.perf_counter()
    results = [_run_one(sql) for sql in statements]
    return SqlConsoleResult(
        statements=results,
        duration_ms=round((time.perf_counter() - started) * 1000, 2),
        error_count=sum(1 for r in results if r.error),
    )


def _require_connection() -> None:
    if not accord_sql_client.ensure_ready():
        raise HTTPException(
            status_code=503,
            detail=(
                f"cassandra-sql is not reachable at {settings.accord_sql_host}:"
                f"{settings.accord_sql_port}"
            ),
        )


@router.get("/status")
def status() -> dict:
    """Is cassandra-sql reachable, and what is it?"""
    if not accord_sql_client.busy:
        accord_sql_client.ensure_ready()
    return {
        "engine": "cassandra-sql",
        "connected": accord_sql_client.connected,
        "host": settings.accord_sql_host,
        "port": settings.accord_sql_port,
        "database": settings.accord_sql_database,
        # Named so the page can say plainly that these are not the demo's tables.
        "keyspaces": ["cassandra_sql", "cassandra_sql_internal", "pg_catalog"],
    }


@router.get("/presets", response_model=List[SqlPreset])
def presets() -> List[SqlPreset]:
    return PRESETS


@router.post("/schema", response_model=SqlConsoleResult)
def create_schema() -> SqlConsoleResult:
    """Create the tables and insert the rows the presets read.

    **Not idempotent**, which an earlier version of this docstring claimed it was.
    Each statement is sent on its own, so a duplicate CREATE is reported against that
    statement and the run continues; but the seed is refused outright on a second run,
    because UNIQUE is the one declared constraint this engine holds: "UNIQUE constraint
    violation: operators_licence_unique on columns (licence)", and the same for
    ``drones.serial`` and ``zones.zone_code``.  Use /reset to get back to the seeded
    state.
    """
    _require_connection()
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a statement is already running")
    try:
        return _run_many(SCHEMA_STATEMENTS + SEED_STATEMENTS)
    finally:
        _lock.release()


@router.post("/reset", response_model=SqlConsoleResult)
def reset_schema() -> SqlConsoleResult:
    """Drop everything this page owns, then create and seed it again.

    **Destructive**, and the only way back to the seeded state.  Two things make it
    necessary rather than convenient.  UNIQUE is enforced, so re-running /schema cannot
    re-seed a table that already holds its rows.  And the oversubscribe preset
    decrements ``zones.capacity`` without reading it, so a page that has run that preset
    is reporting a capacity that no longer matches the airspace the Accord subtab
    admits drones to.

    Drops nothing outside the five tables, the two ENUMs and the sequence, so a
    keyspace that cassandra-sql created for its own use is untouched.
    """
    _require_connection()
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a statement is already running")
    try:
        return _run_many(RESET_STATEMENTS + SCHEMA_STATEMENTS + SEED_STATEMENTS)
    finally:
        _lock.release()


@router.get("/quirks", response_model=List[SqlQuirk])
def quirks() -> List[SqlQuirk]:
    """The four join defects, each run beside the control that isolates it.

    Run against the live service rather than quoted from a comment, so the page cannot
    claim a defect the engine has stopped having.  Needs the transaction preset to have
    run: three of the four read ``flight_legs``, which the seed leaves empty.
    """
    _require_connection()
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a statement is already running")
    try:
        return [
            SqlQuirk(
                id=quirk["id"],
                title=quirk["title"],
                summary=quirk["summary"],
                expected=quirk["expected"],
                probe=_run_one(quirk["probe"]),
                control=_run_one(quirk["control"]),
            )
            for quirk in QUIRKS
        ]
    finally:
        _lock.release()


@router.get("/tables", response_model=SqlConsoleResult)
def table_counts() -> SqlConsoleResult:
    """A row count per table, one statement each.

    A table with no rows yet reports an error rather than zero, because COUNT(*)
    over an empty table raises here.  Left as the error it is rather than rewritten
    to zero: this route is one of the places the page shows what the engine does.

    One statement each for that reason and not because UNION ALL is broken, which
    is what an earlier version of this comment claimed: a UNION ALL of four counts
    answers correctly.  But a UNION ALL that includes the empty table fails whole,
    so a single statement here would report nothing about the three tables that do
    have rows.
    """
    _require_connection()
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a statement is already running")
    try:
        return _run_many(
            [
                f"SELECT COUNT(*) AS n FROM {table};"
                for table in ("operators", "drones", "zones", "flights", "flight_legs")
            ]
        )
    finally:
        _lock.release()


@router.post("/execute", response_model=SqlConsoleResult)
def execute(request: SqlRequest) -> SqlConsoleResult:
    """Run one SQL string, which may hold a whole BEGIN/COMMIT transaction.

    No parameters, and that is deliberate rather than a simplification: a bound
    parameter returns no rows here, with no error raised, so an interface offering
    one would be offering a silent wrong answer.
    """
    _require_connection()
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a statement is already running")
    try:
        return _run_many([request.sql])
    finally:
        _lock.release()
