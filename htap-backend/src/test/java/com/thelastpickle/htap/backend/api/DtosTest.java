package com.thelastpickle.htap.backend.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.AlertRecord;
import com.thelastpickle.htap.backend.api.dto.DronePosition;
import com.thelastpickle.htap.backend.api.dto.IngestionBucket;
import com.thelastpickle.htap.backend.api.dto.RestrictedZone;
import com.thelastpickle.htap.backend.read.AlertRow;
import com.thelastpickle.htap.backend.read.BucketCount;
import com.thelastpickle.htap.backend.read.FleetRow;
import com.thelastpickle.htap.backend.read.TrailRow;
import com.thelastpickle.htap.backend.read.ZoneRow;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DtosTest {

    private static final Instant AT = Instant.parse("2026-08-29T12:34:56.789Z");

    private static FleetRow fleetRow(Double latitude, Double speed, Instant at) {
        return new FleetRow(
                "drone-7", at, latitude, 2.0, null, speed, null, true, null, null, false, true,
                null);
    }

    @Test
    void aStoredTimestampCarriesNoOffsetAndSixDigits() {
        DronePosition drone = Dtos.drone(fleetRow(1.0, 10.0, AT));

        assertEquals("2026-08-29T12:34:56.789000", drone.eventTime());
    }

    @Test
    void aMissingMeasurementRendersAsZero() {
        DronePosition drone = Dtos.drone(fleetRow(1.0, null, AT));

        assertEquals(0.0, drone.speedMps());
        assertEquals(0.0, drone.altitudeM());
        assertEquals(0.0, drone.riskScore());
        assertEquals(1.0, drone.latitude());
        assertTrue(drone.isFlying());
        assertTrue(drone.predictedZoneBreach());
    }

    @Test
    void aMissingTimestampRendersAsEmptyRatherThanAsAnEpoch() {
        assertEquals("", Dtos.drone(fleetRow(1.0, 10.0, null)).eventTime());
    }

    @Test
    void aZoneWithNoSeverityIsAWarning() {
        RestrictedZone zone = Dtos.zone(
                new ZoneRow("z1", "Airport", "POLYGON((0 0, 1 0, 1 1, 0 0))", null, true));

        assertEquals("warning", zone.severity());
        assertEquals("Airport", zone.zoneName());
    }

    /** An alert with no zone is not an alert about zone "", so this field stays absent. */
    @Test
    void anAlertWithNoZoneReportsNull() {
        assertNull(Dtos.alert(alertRow(null)).zoneId());
        assertNull(Dtos.alert(alertRow("")).zoneId());
        assertEquals("z1", Dtos.alert(alertRow("z1")).zoneId());
    }

    @Test
    void anAlertSummaryIsTheFirstSevenFieldsOfTheRecord() {
        AlertRecord record = Dtos.alert(alertRow("z1"));

        assertEquals(record.alertId(), record.summary().alertId());
        assertEquals(record.alertTime(), record.summary().alertTime());
        assertEquals(record.entityId(), record.summary().entityId());
        assertEquals(record.alertType(), record.summary().alertType());
        assertEquals(record.severity(), record.summary().severity());
        assertEquals(record.message(), record.summary().message());
        assertEquals(record.riskScore(), record.summary().riskScore());
    }

    @Test
    void aBucketLabelIsTheClockTimeInsideTheKey() {
        IngestionBucket bucket = Dtos.bucket(new BucketCount("2026-08-29T14:30", 4_212L));

        assertEquals("14:30", bucket.time());
        assertEquals("2026-08-29T14:30", bucket.timestamp());
        assertEquals(4_212L, bucket.count());
    }

    @Test
    void aTrailPointDropsTheHeadingTheTrackDoesNotDraw() {
        var point = Dtos.trailPoint(new TrailRow(AT, 1.0, 2.0, null, 12.25, 90.0));

        assertEquals("2026-08-29T12:34:56.789000", point.eventTime());
        assertEquals(0.0, point.altitudeM());
        assertEquals(12.25, point.speedMps());
    }

    @Test
    void aNearbyResultCarriesTheDistanceItWasFoundAt() {
        assertEquals(412.5, Dtos.nearby(fleetRow(1.0, 10.0, AT), 412.5).distanceM());
    }

    private static AlertRow alertRow(String zoneId) {
        return new AlertRow(
                UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                AT,
                "drone-7",
                "zone_breach",
                "critical",
                zoneId,
                1.0,
                2.0,
                null,
                "Entered restricted airspace",
                0.9);
    }
}
