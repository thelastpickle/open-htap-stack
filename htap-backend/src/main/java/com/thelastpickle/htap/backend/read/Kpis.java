package com.thelastpickle.htap.backend.read;

import java.util.List;

/**
 * Every fleet key performance indicator (KPI), derived from one scan of {@code
 * drone_latest_status}.
 *
 * <p>One scan and not six aggregates: an earlier shape issued six {@code ALLOW FILTERING}
 * aggregates over the same partitions, where one scan gives the same numbers from one
 * consistent read for a fraction of the coordinator's work.
 *
 * <p>The speed and altitude figures come from the flying subset alone, and count only the
 * readings that are present; the two counters and the totals count the whole scan. That
 * asymmetry is the Python's and it is deliberate: a grounded asset's speed of zero would drag
 * the mean of a flying fleet down, while an asset near a zone is near it whether it is flying
 * or not.
 */
public record Kpis(
        int totalDrones,
        int activeFlyingDrones,
        int groundedDrones,
        double maxSpeedMps,
        double minSpeedMps,
        double avgSpeedMps,
        double maxAltitudeM,
        double minAltitudeM,
        double avgAltitudeM,
        int nearZoneCount,
        int predictedBreachCount,
        long totalEvents,
        double ingestionRatePerSec) {

    /** What an unreachable Cassandra reports: the response model's own defaults. */
    public static Kpis zero() {
        return new Kpis(0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0, 0L, 0.0);
    }

    public static Kpis of(List<FleetSample> rows, long totalEvents, double ingestionRatePerSec) {
        int flying = 0;
        int nearZone = 0;
        int predictedBreach = 0;
        Extent speed = new Extent();
        Extent altitude = new Extent();
        for (FleetSample row : rows) {
            if (row.isFlying()) {
                flying++;
                speed.add(row.speedMps());
                altitude.add(row.altitudeM());
            }
            if (row.nearRestrictedZone()) {
                nearZone++;
            }
            if (row.predictedZoneBreach()) {
                predictedBreach++;
            }
        }
        return new Kpis(
                rows.size(),
                flying,
                rows.size() - flying,
                speed.max(),
                speed.min(),
                speed.mean(),
                altitude.max(),
                altitude.min(),
                altitude.mean(),
                nearZone,
                predictedBreach,
                totalEvents,
                ingestionRatePerSec);
    }
}
