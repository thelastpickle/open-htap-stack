package com.thelastpickle.htap.backend.api.dto;

import com.thelastpickle.htap.backend.read.Kpis;
import java.util.List;

/**
 * The Overview page's whole payload: the fleet key performance indicators (KPIs), the fraction
 * of the stack reachable, and the newest few alerts.
 *
 * <p>The thirteen fleet figures are {@link Kpis}'s, restated because this response carries two
 * more that the read layer knows nothing about. {@link #of} is the one place the two meet.
 *
 * @param platformHealthScore fraction of the stack's services reachable, from the health probe
 */
public record OverviewKpis(
        int activeFlyingDrones,
        int groundedDrones,
        int totalDrones,
        double maxSpeedMps,
        double minSpeedMps,
        double avgSpeedMps,
        double maxAltitudeM,
        double minAltitudeM,
        double avgAltitudeM,
        int nearZoneCount,
        int predictedBreachCount,
        long totalEvents,
        double ingestionRatePerSec,
        double platformHealthScore,
        List<AlertSummary> latestAlerts) {

    public static OverviewKpis of(
            Kpis fleet, double platformHealthScore, List<AlertSummary> latestAlerts) {
        return new OverviewKpis(
                fleet.activeFlyingDrones(),
                fleet.groundedDrones(),
                fleet.totalDrones(),
                fleet.maxSpeedMps(),
                fleet.minSpeedMps(),
                fleet.avgSpeedMps(),
                fleet.maxAltitudeM(),
                fleet.minAltitudeM(),
                fleet.avgAltitudeM(),
                fleet.nearZoneCount(),
                fleet.predictedBreachCount(),
                fleet.totalEvents(),
                fleet.ingestionRatePerSec(),
                platformHealthScore,
                latestAlerts);
    }
}
