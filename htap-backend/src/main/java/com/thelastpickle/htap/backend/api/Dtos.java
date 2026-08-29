package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.AlertRecord;
import com.thelastpickle.htap.backend.api.dto.DronePosition;
import com.thelastpickle.htap.backend.api.dto.IngestionBucket;
import com.thelastpickle.htap.backend.api.dto.NearbyDroneResult;
import com.thelastpickle.htap.backend.api.dto.RestrictedZone;
import com.thelastpickle.htap.backend.api.dto.TrailPoint;
import com.thelastpickle.htap.backend.read.AlertRow;
import com.thelastpickle.htap.backend.read.BucketCount;
import com.thelastpickle.htap.backend.read.FleetRow;
import com.thelastpickle.htap.backend.read.TrailRow;
import com.thelastpickle.htap.backend.read.ZoneRow;
import com.thelastpickle.htap.common.Timestamps;
import java.time.Instant;
import java.util.UUID;

/**
 * The one place a read row becomes a response.
 *
 * <p>Both conversions the boundary makes live here. A missing measurement becomes 0.0, which
 * is what the frontend is written against; a stored timestamp becomes the string spelling
 * {@link Timestamps} defines, which is what makes the five paths comparable.
 *
 * <p>A null string becomes empty, where the Python's {@code str(None)} would have sent the
 * four characters {@code None} to the browser. No row the sink writes has one, so the two
 * agree on this stack's data.
 */
public final class Dtos {

    private Dtos() {}

    public static DronePosition drone(FleetRow row) {
        return new DronePosition(
                text(row.entityId()),
                stamp(row.eventTime()),
                zero(row.latitude()),
                zero(row.longitude()),
                zero(row.altitudeM()),
                zero(row.speedMps()),
                zero(row.headingDeg()),
                row.isFlying(),
                zero(row.tempInternalC()),
                zero(row.tempExternalC()),
                row.nearRestrictedZone(),
                row.predictedZoneBreach(),
                zero(row.riskScore()));
    }

    public static RestrictedZone zone(ZoneRow row) {
        return new RestrictedZone(
                text(row.zoneId()),
                text(row.zoneName()),
                text(row.polygonWkt()),
                row.severity() == null ? "warning" : row.severity(),
                row.enabled());
    }

    public static AlertRecord alert(AlertRow row) {
        return new AlertRecord(
                id(row.alertId()),
                stamp(row.alertTime()),
                text(row.entityId()),
                text(row.alertType()),
                text(row.severity()),
                text(row.message()),
                zero(row.riskScore()),
                // The one nullable field the frontend reads as absent rather than as blank:
                // an alert with no zone is not an alert about zone "".
                row.zoneId() == null || row.zoneId().isEmpty() ? null : row.zoneId(),
                zero(row.latitude()),
                zero(row.longitude()),
                zero(row.altitudeM()));
    }

    /** The chart's label is the clock time inside the key: {@code 2026-08-29T14:30 → 14:30}. */
    public static IngestionBucket bucket(BucketCount count) {
        return new IngestionBucket(count.bucket().substring(11), count.bucket(), count.count());
    }

    public static TrailPoint trailPoint(TrailRow row) {
        return new TrailPoint(
                stamp(row.eventTime()),
                zero(row.latitude()),
                zero(row.longitude()),
                zero(row.altitudeM()),
                zero(row.speedMps()));
    }

    public static NearbyDroneResult nearby(FleetRow row, double distanceM) {
        return new NearbyDroneResult(
                text(row.entityId()),
                stamp(row.eventTime()),
                zero(row.latitude()),
                zero(row.longitude()),
                zero(row.altitudeM()),
                distanceM);
    }

    private static double zero(Double value) {
        return value == null ? 0.0 : value;
    }

    private static String text(String value) {
        return value == null ? "" : value;
    }

    private static String id(UUID value) {
        return value == null ? "" : value.toString();
    }

    private static String stamp(Instant at) {
        return at == null ? "" : Timestamps.iso(at);
    }
}
