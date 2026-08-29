package com.thelastpickle.htap.sink;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.thelastpickle.htap.sink.DemoSchema.DemoZone;

/**
 * Applies the schema and seeds its reference data.
 *
 * <p>Runs on every sink start, which is what makes {@code IF NOT EXISTS} the right spelling
 * throughout and what makes the {@code cdc} reconcile below necessary.
 */
final class SchemaOwner {

    private final CqlSession session;
    private final SinkSettings settings;

    SchemaOwner(CqlSession session, SinkSettings settings) {
        this.session = session;
        this.settings = settings;
    }

    /** The keyspace, every table, the index, and the two seeds. */
    void ensure() {
        for (String statement : DemoSchema.statements(settings)) {
            session.execute(statement);
        }
        Log.sink("session tables transactional_mode: %s", settings.accordEnabled() ? "full" : "off");
        ensureCdc();
        seedZones();
        seedZoneOccupancy();
        Log.sink("schema ensured");
    }

    /**
     * Brings {@code drone_latest_status}'s {@code cdc} option to what the environment asks for.
     *
     * <p>The {@code CREATE TABLE} carries the option, which covers a fresh keyspace and nothing
     * else: {@code IF NOT EXISTS} applies none of its options to a table that already exists, so a
     * stack running since before CDC was added would keep the option off and leave the Sidecar with
     * nothing to publish. Read first and alter only on a difference, so a restart against a table
     * that already agrees issues no schema change.
     */
    void ensureCdc() {
        boolean want = settings.cdcEnabled();
        Row row = session.execute(SimpleStatement.newInstance(
                        "SELECT cdc FROM system_schema.tables"
                                + " WHERE keyspace_name = ? AND table_name = 'drone_latest_status'",
                        settings.keyspace()))
                .one();
        boolean have = row != null && Boolean.TRUE.equals(row.getBoolean("cdc"));
        if (have == want) {
            Log.sink("drone_latest_status cdc: %s", have);
            return;
        }
        session.execute("ALTER TABLE " + settings.keyspace()
                + ".drone_latest_status WITH cdc = " + want + ";");
        Log.sink("drone_latest_status cdc: %s -> %s (altered)", have, want);
    }

    /** Inserts the demo zones if they are absent, leaving any edit in place. */
    void seedZones() {
        for (DemoZone zone : DemoSchema.ZONES) {
            try {
                session.execute(SimpleStatement.newInstance(
                        "INSERT INTO " + settings.keyspace() + ".restricted_zones"
                                + " (zone_id, zone_name, polygon_wkt, severity, enabled, updated_at)"
                                + " VALUES (?, ?, ?, ?, true, toTimestamp(now())) IF NOT EXISTS",
                        zone.zoneId(), zone.zoneName(), zone.polygonWkt(), zone.severity()));
            } catch (RuntimeException e) {
                Log.sink("could not seed zone %s: %s", zone.zoneId(), e);
            }
        }
    }

    /**
     * Gives each zone its clearance limit, without disturbing one already in use.
     *
     * <p>Three things here are not the obvious spelling, and each is forced by the table being
     * transactional.
     *
     * <p>QUORUM, because {@code transactional_mode='full'} routes even a plain {@code INSERT}
     * through Accord, and Accord refuses the driver's default: "ConsistencyLevel LOCAL_ONE is
     * unsupported with Accord for write/commit".
     *
     * <p>A read before the write rather than {@code IF NOT EXISTS}, because a lightweight
     * transaction and Accord are two consensus paths over the same row and there is no reason to
     * ask a table in Accord's care to run one. Only this sink seeds, so the read and the write need
     * not be one operation.
     *
     * <p>And {@code remaining} is left alone once the row exists. Restarting the sink must not hand
     * back clearances the zone has granted, which resetting the count would do while leaving every
     * {@code zone_clearance} row in place.
     */
    void seedZoneOccupancy() {
        for (DemoZone zone : DemoSchema.ZONES) {
            try {
                Row existing = session.execute(SimpleStatement.newInstance(
                                "SELECT zone_id FROM " + settings.keyspace()
                                        + ".zone_occupancy WHERE zone_id = ?",
                                zone.zoneId())
                        .setConsistencyLevel(ConsistencyLevel.QUORUM))
                        .one();
                if (existing != null) {
                    continue;
                }
                session.execute(SimpleStatement.newInstance(
                                "INSERT INTO " + settings.keyspace() + ".zone_occupancy"
                                        + " (zone_id, zone_name, severity, capacity, remaining)"
                                        + " VALUES (?, ?, ?, ?, ?)",
                                zone.zoneId(),
                                zone.zoneName(),
                                zone.severity(),
                                zone.capacity(),
                                zone.capacity())
                        .setConsistencyLevel(ConsistencyLevel.QUORUM));
            } catch (RuntimeException e) {
                Log.sink("could not seed zone capacity %s: %s", zone.zoneId(), e);
            }
        }
    }
}
