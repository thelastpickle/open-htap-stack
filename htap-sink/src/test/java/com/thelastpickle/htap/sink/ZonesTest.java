package com.thelastpickle.htap.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.cql.Row;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Which zones reach the alerting, and what an unusable boundary becomes.
 *
 * <p>The filter is here rather than in the statement, so it is worth pinning: a zone the operator
 * disabled must not raise alerts, and a boundary that cannot be parsed must arrive as a zone the
 * caller can drop rather than as an exception that would leave the alerting with no zones at all.
 */
class ZonesTest {

    private static final String SQUARE =
            "POLYGON((10.72 59.91, 10.74 59.91, 10.74 59.92, 10.72 59.92, 10.72 59.91))";

    private final SinkFakes.RecordingSession node = new SinkFakes.RecordingSession();
    private final Zones zones = new Zones(node.session(), "demo");

    @Test
    void anEnabledZoneArrivesWithItsBoundaryParsed() {
        node.answers = cql -> List.of(zone("zone-palace", "Royal Palace", SQUARE, true));

        List<Zone> enabled = zones.enabled();

        assertEquals(1, enabled.size());
        assertEquals("zone-palace", enabled.getFirst().zoneId());
        assertEquals("critical", enabled.getFirst().severity());
        assertEquals(5, enabled.getFirst().ring().size(), "the ring keeps its closing point");
        assertTrue(enabled.getFirst().usable());
    }

    /** A disabled zone is dropped here, so nothing downstream has to know about the column. */
    @Test
    void aDisabledZoneIsDropped() {
        node.answers = cql -> List.of(
                zone("zone-palace", "Royal Palace", SQUARE, true),
                zone("zone-off", "Turned off", SQUARE, false));

        assertEquals(List.of("zone-palace"), zones.enabled().stream().map(Zone::zoneId).toList());
    }

    /** A null {@code enabled} is a disabled zone: the column is absent rather than false. */
    @Test
    void aZoneWithNoEnabledFlagIsDropped() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("zone_id", "zone-unset");
        values.put("zone_name", "No flag");
        values.put("polygon_wkt", SQUARE);
        values.put("severity", "critical");
        node.answers = cql -> List.of(SinkFakes.row(values));

        assertEquals(List.of(), zones.enabled());
    }

    /**
     * A boundary that cannot be read gives an empty ring rather than raising.
     *
     * <p>Which is what {@code Alerts.reload} drops: one unreadable zone must not take the other two
     * with it, and the alerting reporting no zones at all is the failure that matters.
     */
    @Test
    void anUnreadableBoundaryIsAnUnusableZone() {
        node.answers = cql -> List.of(
                zone("zone-null", "No boundary", null, true),
                zone("zone-circle", "Not a polygon", "CIRCLE(10.7 59.9, 500)", true));

        List<Zone> enabled = zones.enabled();

        assertEquals(2, enabled.size());
        assertTrue(enabled.stream().noneMatch(Zone::usable));
        assertEquals(List.of(), enabled.getFirst().ring(), "a null boundary parses to no ring");
        assertEquals(List.of(), enabled.get(1).ring(), "and so does one that is not a polygon");
    }

    /** One statement, over the keyspace the sink was given. */
    @Test
    void theZonesAreReadFromTheSinksOwnKeyspace() {
        zones.enabled();

        assertEquals(1, node.executed.size());
        assertTrue(node.executed.getFirst().contains("FROM demo.restricted_zones"),
                node.executed.getFirst());
    }

    private static Row zone(String id, String name, String wkt, boolean enabled) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("zone_id", id);
        values.put("zone_name", name);
        values.put("polygon_wkt", wkt);
        values.put("severity", "critical");
        values.put("enabled", enabled);
        return SinkFakes.row(values);
    }
}
