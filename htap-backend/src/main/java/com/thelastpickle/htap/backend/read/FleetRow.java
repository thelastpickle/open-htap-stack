package com.thelastpickle.htap.backend.read;

import java.time.Instant;

/**
 * One row of {@code drone_latest_status}, as the table holds it.
 *
 * <p>Every measurement is a boxed {@code Double} because null and zero are different
 * answers here and the read layer is where the difference still exists: the map's DTO turns
 * a null into 0.0, but {@code /polygon-stats} and the key performance indicator (KPI)
 * reduction skip a null rather than averaging a zero into the result. Collapsing the two at
 * the read would silently pull every mean towards zero for as many assets as have no reading.
 *
 * <p>The booleans are primitive, which is faithful rather than a shortcut: the Python read
 * them with {@code bool(row.get(...))}, so a null and a false were one answer already.
 */
public record FleetRow(
        String entityId,
        Instant eventTime,
        Double latitude,
        Double longitude,
        Double altitudeM,
        Double speedMps,
        Double headingDeg,
        boolean isFlying,
        Double tempInternalC,
        Double tempExternalC,
        boolean nearRestrictedZone,
        boolean predictedZoneBreach,
        Double riskScore) {

    /** Whether the row has a position at all, which every spatial read has to ask. */
    public boolean located() {
        return latitude != null && longitude != null;
    }
}
