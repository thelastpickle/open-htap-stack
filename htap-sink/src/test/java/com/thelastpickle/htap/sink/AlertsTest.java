package com.thelastpickle.htap.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.common.Geometry;
import com.thelastpickle.htap.sink.Alerts.Proximity;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The alerting rule, over the demo's own zones.
 *
 * <p>Every figure asserted here is also the dashboard's: its what-if simulation uses the same warning
 * distance and the same risk curve, so a change on one side that is not made on the other makes the
 * page disagree with the table beside it.
 *
 * <p>The positions are east of Oslo airport's boundary at 11.15, and the distances they give were
 * computed from the same projection {@code Geometry} uses: 82.98 m, 110.65 m, 276.62 m and 553.23 m.
 */
class AlertsTest {

    private static final Instant AT = Instant.parse("2026-08-29T12:34:56Z");
    private static final String AIRPORT = "zone-oslo-airport";

    private final AtomicLong nanos = new AtomicLong();
    private final Alerts alerts = new Alerts(nanos::get);

    AlertsTest() {
        alerts.reload(demoZones());
    }

    /** A position inside a zone is the maximum risk, a breach, and a critical alert. */
    @Test
    void aPositionInsideAZoneIsABreach() {
        Proximity scored = alerts.score("asset-1", 60.20, 11.10, 120.0, AT);

        assertTrue(scored.nearZone());
        assertTrue(scored.predictedBreach());
        assertEquals(Alerts.INSIDE_RISK, scored.riskScore());
        assertEquals(AIRPORT, scored.nearestZoneId());
        assertEquals(1, scored.alerts().size());

        AlertRow alert = scored.alerts().getFirst();
        assertEquals("zone_breach", alert.alertType());
        assertEquals("critical", alert.severity());
        assertEquals("asset-1 is inside restricted zone Oslo Lufthavn Gardermoen", alert.message());
        assertEquals("2026-08-29T12", alert.bucket());
        assertEquals(AT, alert.alertTime());
        assertEquals(1, alert.alertId().version(), "the id is a timeuuid, as the column is");
        assertEquals(Alerts.INSIDE_RISK, alert.riskScore());
        assertEquals(120.0, alert.altitudeM());
    }

    /** Inside is deliberately 0.95 rather than 1.0, so a reader can tell it from a certainty. */
    @Test
    void insideIsNotCertainty() {
        assertEquals(0.95, Alerts.INSIDE_RISK);
    }

    /** Outside, the risk falls linearly to nothing at the warning distance. */
    @Test
    void theRiskFallsLinearlyWithDistance() {
        Proximity scored = alerts.score("asset-1", 60.20, 11.152, 120.0, AT);

        double distance = Geometry.distanceToPolygonMetres(60.20, 11.152, ring());
        assertEquals(1.0 - distance / Alerts.WARNING_DISTANCE_M, scored.riskScore(), 1e-12);
        assertEquals(0.7787, scored.riskScore(), 5e-5);
        assertTrue(scored.predictedBreach(), "above the breach threshold of 0.7");

        AlertRow alert = scored.alerts().getFirst();
        assertEquals("zone_proximity", alert.alertType());
        assertEquals("warning", alert.severity());
        assertEquals("asset-1 is 111m from restricted zone Oslo Lufthavn Gardermoen", alert.message());
    }

    /** Above 0.8 the same proximity is reported as high rather than warning. */
    @Test
    void aCloserProximityIsHigh() {
        Proximity scored = alerts.score("asset-1", 60.20, 11.1515, 120.0, AT);

        assertEquals(0.8340, scored.riskScore(), 5e-5);
        assertEquals("high", scored.alerts().getFirst().severity());
        assertEquals("asset-1 is 83m from restricted zone Oslo Lufthavn Gardermoen",
                scored.alerts().getFirst().message());
    }

    /**
     * A risk below the alert threshold is reported on the asset and writes no row.
     *
     * <p>Which is the distinction the two thresholds are for: the live map shows the asset as near a
     * zone, and the alert table holds only what somebody should look at.
     */
    @Test
    void aNearbyAssetWithLowRiskWritesNoRow() {
        Proximity scored = alerts.score("asset-1", 60.20, 11.155, 120.0, AT);

        assertTrue(scored.nearZone());
        assertFalse(scored.predictedBreach());
        assertEquals(0.4468, scored.riskScore(), 5e-5);
        assertEquals(AIRPORT, scored.nearestZoneId());
        assertEquals(List.of(), scored.alerts());
    }

    /** Beyond the warning distance the zone contributes nothing at all. */
    @Test
    void aDistantAssetIsNotNearAnything() {
        Proximity scored = alerts.score("asset-1", 60.20, 11.16, 120.0, AT);

        assertFalse(scored.nearZone());
        assertFalse(scored.predictedBreach());
        assertEquals(0.0, scored.riskScore());
        assertNull(scored.nearestZoneId());
        assertEquals(List.of(), scored.alerts());
    }

    /** With two zones in range, the nearer names the asset's risk. */
    @Test
    void theNearestZoneWins() {
        Alerts overlapping = new Alerts(nanos::get);
        overlapping.reload(List.of(
                zone("zone-far", "Far", 60.20, 11.1530),
                zone("zone-near", "Near", 60.20, 11.1515)));

        Proximity scored = overlapping.score("asset-1", 60.20, 11.15, 120.0, AT);

        assertEquals("zone-near", scored.nearestZoneId());
        assertEquals(2, scored.alerts().size(), "both zones are within the alert threshold");
    }

    /**
     * One alert per asset per zone per minute, whatever the reading rate.
     *
     * <p>Without it an asset parked beside a zone would be most of the alert table, at the demo's two
     * thousand readings a second.
     */
    @Test
    void theCooldownHoldsForAMinute() {
        assertEquals(1, alerts.score("asset-1", 60.20, 11.10, 120.0, AT).alerts().size());
        assertEquals(0, alerts.score("asset-1", 60.20, 11.10, 120.0, AT).alerts().size());

        nanos.set((Alerts.COOLDOWN_SECONDS - 1) * 1_000_000_000L);
        assertEquals(0, alerts.score("asset-1", 60.20, 11.10, 120.0, AT).alerts().size());

        nanos.set(Alerts.COOLDOWN_SECONDS * 1_000_000_000L);
        assertEquals(1, alerts.score("asset-1", 60.20, 11.10, 120.0, AT).alerts().size());
    }

    /** The cooldown is per asset and per zone, so one asset cannot silence another. */
    @Test
    void theCooldownIsPerAssetAndZone() {
        assertEquals(1, alerts.score("asset-1", 60.20, 11.10, 120.0, AT).alerts().size());

        assertEquals(1, alerts.score("asset-2", 60.20, 11.10, 120.0, AT).alerts().size());
        assertEquals(1, alerts.score("asset-1", 59.915, 10.73, 120.0, AT).alerts().size(),
                "the palace is a different zone for the same asset");
    }

    /** A zone whose boundary will not parse is dropped rather than treated as empty. */
    @Test
    void anUnusablePolygonIsDropped() {
        Alerts loaded = new Alerts(nanos::get);
        loaded.reload(List.of(
                new Zone("broken", "Broken", "critical", Geometry.parseWktPolygon("POLYGON((1 2))")),
                demoZones().getFirst()));

        assertEquals(1, loaded.loaded());
        assertEquals(AIRPORT, loaded.score("asset-1", 60.20, 11.10, 120.0, AT).nearestZoneId());
    }

    /** With no zones loaded, every position scores nothing and nothing is written. */
    @Test
    void withNoZonesNothingIsScored() {
        Alerts empty = new Alerts(nanos::get);

        assertEquals(Proximity.NOTHING, empty.score("asset-1", 60.20, 11.10, 120.0, AT));
        assertEquals(0, empty.loaded());
    }

    /** A reload replaces the zones and keeps the cooldowns, so it cannot be used to spam alerts. */
    @Test
    void aReloadKeepsTheCooldowns() {
        assertEquals(1, alerts.score("asset-1", 60.20, 11.10, 120.0, AT).alerts().size());

        alerts.reload(demoZones());

        assertEquals(0, alerts.score("asset-1", 60.20, 11.10, 120.0, AT).alerts().size());
    }

    private static List<Zone> demoZones() {
        return DemoSchema.ZONES.stream()
                .map(zone -> new Zone(
                        zone.zoneId(),
                        zone.zoneName(),
                        zone.severity(),
                        Geometry.parseWktPolygon(zone.polygonWkt())))
                .toList();
    }

    private static List<Geometry.LonLat> ring() {
        return Geometry.parseWktPolygon(DemoSchema.ZONES.getFirst().polygonWkt());
    }

    /** A one-metre zone at a given point, for the cases where only the distance matters. */
    private static Zone zone(String id, String name, double lat, double lon) {
        double side = 0.000005;
        return new Zone(id, name, "warning", List.of(
                new Geometry.LonLat(lon - side, lat - side),
                new Geometry.LonLat(lon + side, lat - side),
                new Geometry.LonLat(lon + side, lat + side),
                new Geometry.LonLat(lon - side, lat + side),
                new Geometry.LonLat(lon - side, lat - side)));
    }
}
