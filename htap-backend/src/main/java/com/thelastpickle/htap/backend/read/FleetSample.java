package com.thelastpickle.htap.backend.read;

/**
 * One row of the key performance indicator (KPI) scan, which reads six of {@code
 * drone_latest_status}'s columns rather than the sixteen {@link FleetRow} holds.
 *
 * <p>A narrower projection than the map's and deliberately so: the KPIs are polled every few
 * seconds over the whole fleet, and every column in the projection is coordinator work and
 * bytes on the wire for a figure nothing shows.
 *
 * <p>{@code entityId} is in the projection and not in this record. The Python read it and
 * used it for nothing but {@code len(rows)}, so it is kept in the statement to leave the
 * timed read exactly the Python's, and dropped here because a count needs no identifier.
 */
public record FleetSample(
        boolean isFlying,
        Double speedMps,
        Double altitudeM,
        boolean nearRestrictedZone,
        boolean predictedZoneBreach) {}
