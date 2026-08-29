package com.thelastpickle.htap.backend.api.dto;

/**
 * One asset's latest state, for the live map and the asset detail panel.
 *
 * <p>Every measurement is primitive, so a column the sink has not written renders as 0.0
 * rather than null. That is the Python's {@code float(row.get(...) or 0.0)} and the frontend
 * is written against it; the distinction between absent and zero survives in the read layer,
 * where the aggregates that need it are computed.
 */
public record DronePosition(
        String entityId,
        String eventTime,
        double latitude,
        double longitude,
        double altitudeM,
        double speedMps,
        double headingDeg,
        boolean isFlying,
        double tempInternalC,
        double tempExternalC,
        boolean nearRestrictedZone,
        boolean predictedZoneBreach,
        double riskScore) {}
