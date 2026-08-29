package com.thelastpickle.htap.sink;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.thelastpickle.htap.common.Geometry;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads the restricted zones and parses each boundary once.
 *
 * <p>{@code enabled} is filtered here rather than in the statement, so the table needs no index for
 * a read of three rows.
 */
final class Zones {

    private final CqlSession session;
    private final String keyspace;

    Zones(CqlSession session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
    }

    /**
     * Every enabled zone, boundary parsed.
     *
     * <p>Raises rather than answering an empty list where the read fails, so a caller can keep the
     * zones it already had: an unreadable table must not silently turn the alerting off.
     */
    List<Zone> enabled() {
        List<Zone> zones = new ArrayList<>();
        for (Row row : session.execute("SELECT zone_id, zone_name, polygon_wkt, severity, enabled"
                + " FROM " + keyspace + ".restricted_zones")) {
            if (!Boolean.TRUE.equals(row.getBoolean("enabled"))) {
                continue;
            }
            List<Geometry.LonLat> ring = Geometry.parseWktPolygon(row.getString("polygon_wkt"));
            zones.add(new Zone(
                    row.getString("zone_id"), row.getString("zone_name"), row.getString("severity"), ring));
        }
        return List.copyOf(zones);
    }
}
