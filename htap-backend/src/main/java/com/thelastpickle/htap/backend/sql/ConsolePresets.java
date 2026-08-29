package com.thelastpickle.htap.backend.sql;

import com.thelastpickle.htap.backend.api.dto.SqlPreset;
import java.util.List;

/**
 * The eight statements the console offers, each written around what this engine actually does.
 *
 * <p>Every one of them carries literals and no parameters. That is not a simplification: a bound
 * parameter returns no rows here and raises nothing, so an interface offering one would be offering
 * a silent wrong answer. Binding the <i>string</i> "1001" does return the row, so the comparison is
 * a text one that a typed bind misses.
 *
 * <p>Three of the eight are written around a join defect rather than around what SQL allows, and
 * each says in its own description what it leaves out. {@link ConsoleQuirks} reproduces the defects
 * beside their controls, which is why a preset may state plainly that a number is unreachable
 * instead of showing a wrong one.
 */
final class ConsolePresets {

    private ConsolePresets() {}

    static final List<SqlPreset> ALL = List.of(
            new SqlPreset(
                    "transaction",
                    "File a flight, in one transaction",
                    """
                    One flight, in five statements that commit together: the flight, its two legs \
                    through restricted airspace, the battery cycle the drone spent and the hour the \
                    operator flew.  Sent as one string, because cassandra-sql executes the whole \
                    string as one unit.  Run it twice and the flight does not duplicate, because a \
                    repeated primary key overwrites here rather than being refused; the two \
                    increments do apply again, so the cycle count and the flight hours climb by one \
                    each time.""",
                    """
                    BEGIN;
                    INSERT INTO flights (flight_id, operator_id, drone_id, departed_at, status,
                        purpose, distance_km, duration_min, energy_wh, fee_nok, route_summary)
                    VALUES (9001, 1001, 2003, 1704499200000, 'cleared', 'survey', 21.40, 38.00,
                        742.00, 1290.00, 'Fornebu to Gardermoen, north transit');
                    INSERT INTO flight_legs (leg_id, flight_id, zone_id, leg_no, distance_km,
                        dwell_min, leg_energy_wh)
                    VALUES (10001, 9001, 3003, 1, 6.20, 9.00, 198.00),
                        (10002, 9001, 3001, 2, 15.20, 12.00, 544.00);
                    UPDATE drones SET battery_cycles = battery_cycles + 1 WHERE drone_id = 2003;
                    UPDATE operators SET flight_hours = flight_hours + 1 WHERE operator_id = 1001;
                    COMMIT;"""),
            new SqlPreset(
                    "rollback",
                    "A flight plan that rolls back",
                    """
                    File a flight, then discard it.  The SELECT after the ROLLBACK returns no row, \
                    which is what shows the write was held until COMMIT rather than applied as it \
                    went.  Cassandra has no such operation: a CQL batch is atomic but never undone, \
                    so a client that changes its mind has to compensate.  The column list that \
                    comes back is the whole table's, not the three the SELECT names: an empty \
                    result set here is described by the table rather than by the projection, which \
                    a result with rows in it is.""",
                    """
                    BEGIN;
                    INSERT INTO flights (flight_id, operator_id, drone_id, departed_at, status,
                        distance_km, route_summary)
                    VALUES (9099, 1001, 2001, 1704499200000, 'planned', 3.00, 'rolled back');
                    ROLLBACK;
                    SELECT flight_id, distance_km, route_summary FROM flights WHERE flight_id = 9099;"""),
            // Every projected name is unique across the four tables, and that is a constraint rather
            // than a style: a column name in two joined tables resolves to the second one named,
            // whatever the qualifier says. distance_km is in both flights and flight_legs, so the
            // per-leg distance -- the number a reader most wants here -- cannot be projected in this
            // join at all. The quirks route shows that, so this preset stays correct and says what
            // it omits.
            new SqlPreset(
                    "join",
                    "A four-way join",
                    """
                    Which operator flew which airframe through which restricted zone, over four \
                    tables.  No CQL statement expresses this, and neither does the schema mode of \
                    cassandra-sql itself, whose own documentation says "No JOINs or subqueries".  \
                    Run the transaction preset first, or there are no flights to join to.

                    The per-leg distance is missing on purpose.  `distance_km` is a column of both \
                    joined tables, and this engine resolves such a name to one table for the whole \
                    statement, so asking for the leg's distance answers the flight's.  The quirks \
                    preset shows it.""",
                    """
                    SELECT
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
                    INNER JOIN zones z ON l.zone_id = z.zone_id;"""),
            // No ORDER BY, because it would be ignored: this engine discards the clause on a grouped
            // result, and a preset asking for an order it does not get would report a wrong answer
            // as though it were right. The rows come back in the engine's own order.
            new SqlPreset(
                    "aggregate",
                    "GROUP BY on a non-key column",
                    """
                    The statement the CQL path declines, in SQL over cassandra-sql's own rows.  An \
                    airframe is not part of any key, so Cassandra cannot group by it; cassandra-sql \
                    reads the rows and groups them itself.""",
                    """
                    SELECT
                        airframe,
                        COUNT(*) AS fleet_count,
                        AVG(mass_kg) AS avg_mass_kg,
                        MIN(max_range_km) AS min_range_km,
                        MAX(max_range_km) AS max_range_km,
                        SUM(battery_cycles) AS total_cycles
                    FROM drones
                    GROUP BY airframe;"""),
            new SqlPreset(
                    "subquery",
                    "A correlated aggregate in the WHERE clause",
                    "Drones that outrange the average of the register they are in.",
                    """
                    SELECT serial, model, airframe, mass_kg, max_range_km
                    FROM drones
                    WHERE max_range_km > (SELECT AVG(max_range_km) FROM drones)
                    ORDER BY max_range_km DESC;"""),
            // The fee is reported as a column and the dwell as a sum; the product is not computed,
            // because this engine cannot multiply across two joined tables. SUM(l.dwell_min *
            // z.fee_per_min) answered 45.0 and 18.0, which are fee_per_min alone rather than the
            // products 540 and 162. So the two figures are shown separately and the reader
            // multiplies them, which is honest where a wrong total dressed as a right one is not.
            new SqlPreset(
                    "having",
                    "GROUP BY with HAVING, over a join",
                    """
                    Dwell time per zone, keeping only the zones a drone loitered in for more than \
                    ten minutes; on the seeded flight that keeps Gardermoen and drops Fornebu.  Run \
                    the transaction preset first, or the join has no legs to group.

                    The fee per minute is a column here rather than a factor.  Multiplying it by \
                    the dwell would be arithmetic across two joined tables, which this engine \
                    answers with one of the two operands; the quirks preset shows it.""",
                    """
                    SELECT
                        z.zone_name,
                        z.severity,
                        z.fee_per_min,
                        COUNT(DISTINCT l.flight_id) AS flight_count,
                        SUM(l.dwell_min) AS total_dwell_min
                    FROM flight_legs l
                    INNER JOIN zones z ON l.zone_id = z.zone_id
                    GROUP BY z.zone_name, z.severity, z.fee_per_min
                    HAVING SUM(l.dwell_min) > 10;"""),
            new SqlPreset(
                    "oversubscribe",
                    "The zone capacity this layer will not hold",
                    """
                    The same three zones the Accord ledger admits drones to, with the same \
                    capacities.  Here the count is an ordinary column and the check is an ordinary \
                    statement, so nothing stops two callers reading the same free slot and both \
                    taking it: the UPDATE below decrements without reading first, and will run \
                    capacity past zero if it is repeated.  That is the difference the Accord subtab \
                    measures, on the same airspace.""",
                    """
                    UPDATE zones SET capacity = capacity - 1 WHERE zone_code = 'zone-oslo-airport';
                    SELECT zone_code, zone_name, capacity FROM zones ORDER BY zone_code;"""),
            new SqlPreset(
                    "explain",
                    "EXPLAIN",
                    """
                    The plan Calcite built, which is what shows the SQL layer to be a planner over \
                    a key-value store rather than a translator into CQL.""",
                    """
                    EXPLAIN SELECT o.name, f.flight_id, f.distance_km
                    FROM flights f
                    INNER JOIN operators o ON f.operator_id = o.operator_id
                    WHERE f.distance_km > 10;"""));
}
