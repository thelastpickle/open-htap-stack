package com.thelastpickle.htap.sink;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of {@code alerts_by_bucket}, decided but not yet written.
 *
 * <p>A value rather than a write, so the scoring that decides an alert can be tested without a
 * session: {@link Alerts} returns these and {@link CassandraWrites} sends them.
 *
 * @param bucket the hour partition, so the dashboard reads whole partitions
 * @param alertId minted at {@code alertTime} rather than at the wall clock, which is a small
 *     divergence from the Python's {@code uuid1()}: the row's id and its own clustering time then
 *     agree, and nothing reads the id's timestamp for anything else
 */
record AlertRow(
        String bucket,
        Instant alertTime,
        String entityId,
        UUID alertId,
        String alertType,
        String severity,
        String zoneId,
        double latitude,
        double longitude,
        double altitudeM,
        String message,
        double riskScore) {}
