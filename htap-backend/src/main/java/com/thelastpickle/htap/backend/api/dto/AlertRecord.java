package com.thelastpickle.htap.backend.api.dto;

/**
 * A whole alert, as the Alerts page shows it.
 *
 * <p>The seven fields of {@link AlertSummary} are repeated here rather than inherited: the
 * Python had this model subclass that one, and a record cannot extend a record. The
 * {@link #summary()} mapping is what keeps the two from drifting.
 *
 * <p>{@code zoneId} is null for an alert that names no zone, and the API renders the null.
 */
public record AlertRecord(
        String alertId,
        String alertTime,
        String entityId,
        String alertType,
        String severity,
        String message,
        double riskScore,
        String zoneId,
        double latitude,
        double longitude,
        double altitudeM) {

    public AlertSummary summary() {
        return new AlertSummary(alertId, alertTime, entityId, alertType, severity, message, riskScore);
    }
}
