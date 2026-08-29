package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

/** What this process reads from its environment, and what it does with a value it cannot use. */
class ProducerSettingsTest {

    /** The Dockerfile's own defaults, which are what an unconfigured container runs on. */
    @Test
    void theDefaultsAreTheOnesTheImageDeclares() {
        ProducerSettings settings = ProducerSettings.from(env(Map.of()));

        assertEquals("kafka:19092", settings.bootstrap());
        assertEquals("demo-events", settings.topic());
        assertEquals(2000, settings.eventsPerSec());
        assertEquals(100, settings.nEntities());
        assertEquals(2000, settings.maxEntities());
        assertEquals(5.0, settings.outlierPercent());
        assertEquals(Duration.ofMillis(50), settings.batchPeriod());
        assertEquals(12, settings.topicPartitions());
        assertEquals((short) 1, settings.topicReplication());
        assertEquals("0", settings.acks());
        assertEquals("", settings.textFile(), "no corpus unless one is named");
        assertEquals("", settings.settingsUrl(), "the dashboard is opt-in");
    }

    /** A value that is no number keeps the default rather than stopping the fleet. */
    @Test
    void anUnreadableValueFallsBackRatherThanFailing() {
        ProducerSettings settings = ProducerSettings.from(env(Map.of(
                "EVENTS_PER_SEC", "quite fast",
                "OUTLIER_PERCENT", "",
                "BATCH_PERIOD_MS", "0x32")));

        assertEquals(2000, settings.eventsPerSec());
        assertEquals(5.0, settings.outlierPercent());
        assertEquals(Duration.ofMillis(50), settings.batchPeriod());
    }

    /**
     * The ceiling can never be below the starting fleet.
     *
     * <p>The fleet's arrays are allocated once for the ceiling, so a stack asking for 500 assets
     * with a ceiling of 200 would index past the end of them.
     */
    @Test
    void theCeilingIsRaisedToTheStartingFleet() {
        ProducerSettings settings = ProducerSettings.from(
                env(Map.of("N_ENTITIES", "500", "MAX_ENTITIES", "200")));

        assertEquals(500, settings.nEntities());
        assertEquals(500, settings.maxEntities());
    }

    /** Neither the rate nor the fleet may be zero: a batch of nothing would idle in silence. */
    @Test
    void theRateAndTheFleetAreHeldAtOneOrMore() {
        ProducerSettings settings = ProducerSettings.from(
                env(Map.of("EVENTS_PER_SEC", "0", "N_ENTITIES", "0")));

        assertEquals(1, settings.eventsPerSec());
        assertEquals(1, settings.nEntities());
    }

    /** A cadence below five milliseconds is more wakeups than motion, so five is the floor. */
    @Test
    void theCadenceHasAFloor() {
        assertEquals(
                Duration.ofMillis(5),
                ProducerSettings.from(env(Map.of("BATCH_PERIOD_MS", "1"))).batchPeriod());
        assertEquals(
                Duration.ofMillis(200),
                ProducerSettings.from(env(Map.of("BATCH_PERIOD_MS", "200"))).batchPeriod());
    }

    /** How many events one turn of the loop sends, which is the rate over the cadence. */
    @Test
    void aBatchIsTheRateForOnePeriod() {
        ProducerSettings settings = ProducerSettings.from(env(Map.of()));

        assertEquals(100, settings.eventsPerBatch(2000));
        assertEquals(1, settings.eventsPerBatch(1), "a rate too low for one event a period still sends");
        assertEquals(500, settings.eventsPerBatch(10_000));
    }

    /** The area is configurable, because a fleet elsewhere is a supported demo. */
    @Test
    void theFleetsAreaReachesTheConfig() {
        ProducerSettings settings = ProducerSettings.from(env(Map.of(
                "FLEET_CENTER_LAT", "51.5",
                "FLEET_CENTER_LON", "-0.12",
                "FLEET_LAT_SPREAD_DEG", "0.2",
                "FLEET_LON_SPREAD_DEG", "0.4",
                "FLEET_SEED", "7",
                "N_ENTITIES", "5",
                "MAX_ENTITIES", "10")));
        FleetConfig fleet = settings.fleet();

        assertEquals(51.5, fleet.centerLat());
        assertEquals(-0.12, fleet.centerLon());
        assertEquals(0.2, fleet.latSpreadDeg());
        assertEquals(0.4, fleet.lonSpreadDeg());
        assertEquals(7L, fleet.seed());
        assertEquals(10, fleet.nEntities(), "the fleet holds state for the ceiling, not the start");
    }

    /** Compression is off unless named, which is what an unset variable meant in the Python. */
    @Test
    void compressionIsOffUnlessNamed() {
        assertTrue(ProducerSettings.from(env(Map.of())).compression().isEmpty());
        assertEquals(
                "lz4", ProducerSettings.from(env(Map.of("KAFKA_COMPRESSION", "lz4"))).compression());
    }

    private static UnaryOperator<String> env(Map<String, String> values) {
        return values::get;
    }
}
