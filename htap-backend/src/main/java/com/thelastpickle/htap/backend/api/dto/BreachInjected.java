package com.thelastpickle.htap.backend.api.dto;

import java.util.UUID;

/**
 * What the scripted breach wrote, and to which asset.
 *
 * <p>Every field here is a real row in Cassandra afterwards, so the map, the KPIs and the alert
 * feed pick the scenario up through their ordinary queries rather than through anything the
 * dashboard holds in memory.
 */
public record BreachInjected(
        boolean success,
        String scenario,
        String entityId,
        double latitude,
        double longitude,
        String alertId,
        String severity,
        String message) {

    /** The scenario the demo scripts, which is the only one there is. */
    public static final String SCENARIO = "zone_breach";

    /** The severity the alert feed sorts to the top. */
    public static final String SEVERITY = "critical";

    /**
     * The response for an asset that was flagged.
     *
     * <p>The coordinates are passed in rather than read from the asset's row, so that they are the
     * same two numbers the alert was written with: a response disagreeing with the row it
     * announced would be the one failure this endpoint cannot report.
     */
    public static BreachInjected of(
            String entityId, double latitude, double longitude, UUID alertId) {
        return new BreachInjected(
                true,
                SCENARIO,
                entityId,
                latitude,
                longitude,
                alertId.toString(),
                SEVERITY,
                entityId + " flagged for a predicted zone breach; alert written");
    }
}
