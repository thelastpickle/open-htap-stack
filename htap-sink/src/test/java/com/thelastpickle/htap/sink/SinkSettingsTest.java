package com.thelastpickle.htap.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** What the sink reads from its environment, and what it does with a value it cannot read. */
class SinkSettingsTest {

    /** The defaults are the compose file's, so a stack with no overrides needs none of them. */
    @Test
    void theDefaultsAreWhatComposeDeclares() {
        SinkSettings settings = SinkSettings.from(name -> null);

        assertEquals("kafka:19092", settings.bootstrap());
        assertEquals("demo-events", settings.topic());
        assertEquals("demo-cassandra-sink", settings.groupId());
        assertEquals("cassandra", settings.cassandraHost());
        assertEquals(9042, settings.cassandraPort());
        assertEquals("datacenter1", settings.datacenter());
        assertEquals("demo", settings.keyspace());
        assertEquals("events", settings.table());
        assertEquals(200, settings.batchSize());
        assertEquals(Duration.ofSeconds(5), settings.reportEvery());
        assertEquals(Duration.ofSeconds(60), settings.zoneReload());
        assertEquals(15, settings.bucketMinutes());
        assertEquals(16, settings.shards());
        assertTrue(settings.accordEnabled());
        assertTrue(settings.cdcEnabled());
    }

    /** Every variable is read under the name this service is given, not the backend's spelling. */
    @Test
    void everyVariableIsReadUnderItsOwnName() {
        Map<String, String> environment = Map.ofEntries(
                Map.entry("KAFKA_BOOTSTRAP", "broker:9092"),
                Map.entry("TOPIC", "other-events"),
                Map.entry("GROUP_ID", "other-sink"),
                Map.entry("CASSANDRA_HOST", "node"),
                Map.entry("CASSANDRA_PORT", "9142"),
                Map.entry("CASSANDRA_DATACENTER", "dc2"),
                Map.entry("KEYSPACE", "other"),
                Map.entry("TABLE", "readings"),
                Map.entry("BATCH_SIZE", "50"),
                Map.entry("REPORT_EVERY_S", "2.5"),
                Map.entry("ZONE_RELOAD_S", "30"),
                Map.entry("EVENT_BUCKET_MINUTES", "5"),
                Map.entry("EVENT_SHARDS", "4"),
                Map.entry("CASSANDRA_ACCORD_ENABLED", "false"),
                Map.entry("CASSANDRA_CDC_ENABLED", "false"));
        SinkSettings settings = SinkSettings.from(environment::get);

        assertEquals("broker:9092", settings.bootstrap());
        assertEquals("other-events", settings.topic());
        assertEquals("other-sink", settings.groupId());
        assertEquals("node:9142", settings.cassandraHost() + ":" + settings.cassandraPort());
        assertEquals("dc2", settings.datacenter());
        assertEquals("other.readings", settings.keyspace() + "." + settings.table());
        assertEquals(50, settings.batchSize());
        assertEquals(Duration.ofMillis(2500), settings.reportEvery());
        assertEquals(Duration.ofSeconds(30), settings.zoneReload());
        assertEquals(5, settings.bucketMinutes());
        assertEquals(4, settings.shards());
        assertFalse(settings.accordEnabled());
        assertFalse(settings.cdcEnabled());
    }

    /**
     * A value that is not a number is the default, as the Python's {@code env_int} was.
     *
     * <p>A typo in compose gives a running sink on defaults rather than a container that will not
     * start; the two values where a default would be dangerous are refused further in, by
     * {@code EventPartitions}.
     */
    @Test
    void anUnreadableNumberFallsBackToTheDefault() {
        Map<String, String> environment =
                Map.of("CASSANDRA_PORT", "nine thousand", "REPORT_EVERY_S", "soon", "EVENT_SHARDS", "");
        SinkSettings settings = SinkSettings.from(environment::get);

        assertEquals(9042, settings.cassandraPort());
        assertEquals(Duration.ofSeconds(5), settings.reportEvery());
        assertEquals(16, settings.shards());
    }

    /** A batch of nothing would poll for ever and write nothing, so one is the floor. */
    @Test
    void aBatchSizeBelowOneIsRaisedToOne() {
        assertEquals(1, SinkSettings.from(Map.of("BATCH_SIZE", "0")::get).batchSize());
        assertEquals(1, SinkSettings.from(Map.of("BATCH_SIZE", "-20")::get).batchSize());
    }

    /** The flags read as the Python read them: "true" in any case, and everything else false. */
    @Test
    void onlyTrueIsTrue() {
        assertTrue(SinkSettings.from(Map.of("CASSANDRA_CDC_ENABLED", "TRUE")::get).cdcEnabled());
        assertTrue(SinkSettings.from(Map.of("CASSANDRA_CDC_ENABLED", "True")::get).cdcEnabled());
        assertFalse(SinkSettings.from(Map.of("CASSANDRA_CDC_ENABLED", "1")::get).cdcEnabled());
        assertFalse(SinkSettings.from(Map.of("CASSANDRA_CDC_ENABLED", "yes")::get).cdcEnabled());
    }

    /** The startup line places a run: which broker, which topic, which keyspace, which batch. */
    @Test
    void theStartupLineNamesEveryAddress() {
        assertEquals(
                "kafka=kafka:19092 topic=demo-events group_id=demo-cassandra-sink"
                        + " cassandra=cassandra:9042 demo.events batch_size=200",
                SinkSettings.from(name -> null).toString());
    }
}
