package com.thelastpickle.htap.backend.read;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of {@code alerts_by_bucket}.
 *
 * <p>{@code zoneId} stays nullable to the API edge, where an empty string becomes null as
 * well: the Python wrote {@code str(a["zone_id"]) if a.get("zone_id") else None}, so an alert
 * the sink raised with no zone and one it raised with an empty zone read the same.
 */
public record AlertRow(
        UUID alertId,
        Instant alertTime,
        String entityId,
        String alertType,
        String severity,
        String zoneId,
        Double latitude,
        Double longitude,
        Double altitudeM,
        String message,
        Double riskScore) {}
