package com.thelastpickle.htap.backend.sql;

import java.util.List;
import java.util.stream.Stream;

/**
 * The console's own drone-operations schema, its seed rows and the way back to them.
 *
 * <p>This repository's schema and not GEICO's {@code demo-ecommerce.sh}, which shipped first and has
 * been removed: a page in this dashboard about customers and loyalty points said nothing about the
 * fleet every other page reads. Five tables hold operators, drones, zones, flights and the legs a
 * flight flies through restricted airspace, with the same three Oslo zones the map draws and the
 * Accord subtab admits drones to.
 *
 * <p>Nothing copies these rows from {@code demo.drone_latest_status} and nothing keeps them in step
 * with it. cassandra-sql stores its rows under an ordered key-value encoding in keyspaces of its
 * own, so a copy would be an ETL job, which is the one thing this repository exists to argue
 * against. The serials and the zone codes are the live fleet's so that a reader can see which drone
 * and which airspace is meant; the page says plainly that this is a register beside the fleet rather
 * than a view of it.
 */
final class ConsoleSchema {

    private ConsoleSchema() {}

    /**
     * The five tables, in the order a count route asks about them.
     *
     * <p>{@code flights} and {@code flight_legs} are empty until the transaction preset has run,
     * and this engine raises on {@code COUNT(*)} over an empty table, so those two are the ones a
     * count reports an error for.
     */
    static final List<String> TABLES =
            List.of("operators", "drones", "zones", "flights", "flight_legs");

    /**
     * The schema, sent one statement at a time.
     *
     * <p>One at a time rather than as one string, so that a second run reports "already exists" per
     * statement and continues. There is no {@code CREATE TABLE IF NOT EXISTS} here, so re-running
     * the schema is how the page recovers from a half-created one.
     */
    static final List<String> SCHEMA = List.of(
            "CREATE TYPE flight_status AS ENUM "
                    + "('planned', 'cleared', 'airborne', 'landed', 'aborted');",
            "CREATE TYPE flight_purpose AS ENUM "
                    + "('survey', 'inspection', 'delivery', 'training', 'emergency');",
            "CREATE SEQUENCE flight_id_seq START WITH 9000 INCREMENT BY 1;",
            """
            CREATE TABLE operators (
                operator_id BIGINT PRIMARY KEY,
                licence TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                base_city TEXT,
                country TEXT,
                certified_at BIGINT,
                flight_hours INT DEFAULT 0
            );""",
            """
            CREATE TABLE drones (
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
            """
            CREATE TABLE zones (
                zone_id BIGINT PRIMARY KEY,
                zone_code TEXT UNIQUE NOT NULL,
                zone_name TEXT NOT NULL,
                severity TEXT,
                capacity INT,
                fee_per_min DECIMAL(10,2)
            );""",
            """
            CREATE TABLE flights (
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
            """
            CREATE TABLE flight_legs (
                leg_id BIGINT PRIMARY KEY,
                flight_id BIGINT NOT NULL,
                zone_id BIGINT NOT NULL,
                leg_no INT NOT NULL,
                distance_km DECIMAL(10,2) NOT NULL,
                dwell_min DECIMAL(10,2) DEFAULT 0.00,
                leg_energy_wh DECIMAL(10,2)
            );""",
            // The foreign keys are the interesting part of the DDL, because Cassandra has no such
            // constraint: cassandra-sql holds it above the storage engine. It accepts all four and
            // enforces none, which ConsoleQuirks records beside the other unenforced constraints.
            "ALTER TABLE flights ADD CONSTRAINT fk_flight_operator "
                    + "FOREIGN KEY (operator_id) REFERENCES operators(operator_id);",
            "ALTER TABLE flights ADD CONSTRAINT fk_flight_drone "
                    + "FOREIGN KEY (drone_id) REFERENCES drones(drone_id);",
            "ALTER TABLE flight_legs ADD CONSTRAINT fk_leg_flight "
                    + "FOREIGN KEY (flight_id) REFERENCES flights(flight_id);",
            "ALTER TABLE flight_legs ADD CONSTRAINT fk_leg_zone "
                    + "FOREIGN KEY (zone_id) REFERENCES zones(zone_id);",
            "CREATE INDEX idx_flights_operator ON flights(operator_id);",
            "CREATE INDEX idx_drones_airframe ON drones(airframe);");

    static final List<String> SEED = List.of(
            """
            INSERT INTO operators
                (operator_id, licence, name, base_city, country, certified_at, flight_hours)
            VALUES
                (1001, 'NO-RPAS-1001', 'Oslo Survey AS', 'Oslo', 'NO', 1704067200000, 1450),
                (1002, 'NO-RPAS-1002', 'Fjord Inspection AS', 'Bergen', 'NO', 1704153600000, 890),
                (1003, 'NO-RPAS-1003', 'Nordic Air Logistics', 'Trondheim', 'NO', 1704240000000, 2310),
                (1004, 'NO-RPAS-1004', 'Politiets Droneenhet', 'Oslo', 'NO', 1704326400000, 640),
                (1005, 'SE-RPAS-2001', 'Kiruna Aerial Mapping', 'Kiruna', 'SE', 1704412800000, 120);""",
            """
            INSERT INTO drones
                (drone_id, serial, model, airframe, mass_kg, max_range_km, battery_cycles, tags,
                 registered_at)
            VALUES
                (2001, 'asset-000000', 'Kestrel M4', 'quadcopter', 4.20, 18.00, 312,
                 ARRAY['lidar', 'survey'], 1704067200000),
                (2002, 'asset-000001', 'Kestrel M4', 'quadcopter', 4.20, 18.00, 118,
                 ARRAY['thermal'], 1704067200000),
                (2003, 'asset-000002', 'Kestrel M8', 'octocopter', 9.80, 26.00, 74,
                 ARRAY['lidar', 'heavy-lift'], 1704067200000),
                (2004, 'asset-000003', 'Harrier VT', 'vtol', 13.50, 92.00, 41,
                 ARRAY['long-range', 'delivery'], 1704067200000),
                (2005, 'asset-000004', 'Harrier VT', 'vtol', 13.50, 92.00, 27,
                 ARRAY['long-range'], 1704067200000),
                (2006, 'asset-000005', 'Skua F2', 'fixed-wing', 6.40, 140.00, 205,
                 ARRAY['mapping'], 1704067200000),
                (2007, 'asset-000006', 'Skua F2', 'fixed-wing', 6.40, 140.00, 163,
                 ARRAY['mapping', 'coastal'], 1704067200000),
                (2008, 'asset-000007', 'Wren N1', 'quadcopter', 1.10, 6.00, 588,
                 ARRAY['indoor', 'training'], 1704067200000);""",
            // The same three zones the map draws and the Accord ledger admits drones to, with the
            // capacities the sink seeds, so the two halves of the page are talking about the same
            // airspace. What differs is what holds the capacity: here a column any statement may
            // overwrite, and there a transaction no concurrent caller can oversubscribe.
            """
            INSERT INTO zones (zone_id, zone_code, zone_name, severity, capacity, fee_per_min)
            VALUES
                (3001, 'zone-oslo-airport', 'Oslo Lufthavn Gardermoen', 'critical', 2, 45.00),
                (3002, 'zone-royal-palace', 'Det Kongelige Slott', 'critical', 3, 60.00),
                (3003, 'zone-fornebu', 'Fornebu Tech Park', 'warning', 5, 18.00);""");

    /**
     * Everything the console owns, dropped.
     *
     * <p>Children before parents, though nothing depends on the order: a foreign key here is
     * accepted and not enforced, so a drop cannot be refused for a dangling reference. The order
     * stands so the list still reads correctly against an engine that one day does enforce them.
     *
     * <p>Three measurements shape the list. {@code DROP TABLE} removes the table's indexes with it,
     * which matters because <b>{@code DROP INDEX} is unimplemented in both forms</b>, answering
     * "Unsupported SQL type". {@code DROP TABLE IF EXISTS} and {@code DROP SEQUENCE IF EXISTS} are
     * honoured and silent on a missing object. And <b>{@code DROP TYPE} ignores its {@code IF
     * EXISTS}</b>, raising "DROP TYPE failed: ENUM type does not exist" exactly as the bare form
     * does, so those two statements are refused on a first-ever reset while the rest succeed.
     *
     * <p>A restart of the service produces the same pair, and finding out why cost a measurement:
     * <b>the ENUM declarations do not survive a restart and the tables do</b>. After a restart the
     * five {@code DROP TABLE} statements each took real time and succeeded, so their definitions had
     * persisted into Cassandra, while both {@code DROP TYPE} statements answered that the type was
     * missing. So the engine holds its ENUM registry in memory, and a caller must not read the error
     * count as the reset's verdict.
     */
    static final List<String> RESET = List.of(
            "DROP TABLE IF EXISTS flight_legs;",
            "DROP TABLE IF EXISTS flights;",
            "DROP TABLE IF EXISTS drones;",
            "DROP TABLE IF EXISTS zones;",
            "DROP TABLE IF EXISTS operators;",
            "DROP TYPE IF EXISTS flight_status;",
            "DROP TYPE IF EXISTS flight_purpose;",
            "DROP SEQUENCE IF EXISTS flight_id_seq;");

    /** The schema then its rows, which is what both {@code /schema} and the tail of a reset send. */
    static List<String> schemaAndSeed() {
        return Stream.concat(SCHEMA.stream(), SEED.stream()).toList();
    }

    /** One row count per table, asked one statement at a time. */
    static List<String> counts() {
        return TABLES.stream().map(table -> "SELECT COUNT(*) AS n FROM " + table + ";").toList();
    }
}
