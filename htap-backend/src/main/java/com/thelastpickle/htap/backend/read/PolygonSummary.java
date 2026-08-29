package com.thelastpickle.htap.backend.read;

import java.util.List;

/**
 * The fleet inside an arbitrary polygon, aggregated.
 *
 * <p>Cassandra has no spatial predicate, so containment is decided here over the bounded
 * latest-state table. The same question over history is asked of Presto on the Explore page,
 * which is the point the two paths make together.
 */
public record PolygonSummary(
        int droneCount,
        double avgSpeedMps,
        double maxSpeedMps,
        double avgAltitudeM,
        double maxAltitudeM,
        double avgTempInternalC) {

    public static PolygonSummary empty() {
        return new PolygonSummary(0, 0.0, 0.0, 0.0, 0.0, 0.0);
    }

    /** The rows already known to be inside the polygon. */
    public static PolygonSummary of(List<FleetRow> inside) {
        Extent speed = new Extent();
        Extent altitude = new Extent();
        Extent temperature = new Extent();
        for (FleetRow row : inside) {
            speed.add(row.speedMps());
            altitude.add(row.altitudeM());
            temperature.add(row.tempInternalC());
        }
        return new PolygonSummary(
                inside.size(),
                speed.mean(),
                speed.max(),
                altitude.mean(),
                altitude.max(),
                temperature.mean());
    }
}
