package com.thelastpickle.htap.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The schema itself, statement by statement.
 *
 * <p>The one thing in this module that has no second source: nothing else in the stack defines these
 * tables, so a key or an option wrong here is wrong everywhere and shows up as a query that matches
 * nothing. {@code DESCRIBE KEYSPACE demo} on a stack is the other half of this check, and it reports
 * sixteen rows: the keyspace, fourteen tables and one index.
 */
class DemoSchemaTest {

    private static final SinkSettings DEFAULTS = SinkSettings.from(name -> null);

    /** The fourteen tables, the keyspace and the index: sixteen objects, in a runnable order. */
    @Test
    void theKeyspaceComesFirstAndTheIndexAfterItsTable() {
        List<String> statements = DemoSchema.statements(DEFAULTS);

        assertEquals(16, statements.size());
        assertTrue(statements.getFirst().startsWith("CREATE KEYSPACE IF NOT EXISTS demo"),
                statements.getFirst());
        int embeddings = indexOf(statements, "CREATE TABLE IF NOT EXISTS demo.drone_text_embeddings");
        int index = indexOf(statements, "CREATE CUSTOM INDEX IF NOT EXISTS payload_vector_idx");
        assertTrue(embeddings < index, "the index cannot be created before its table");
    }

    /** Every table the dashboard and the demos read, named once. */
    @Test
    void everyTableIsCreated() {
        List<String> statements = DemoSchema.statements(DEFAULTS);

        List<String> tables = List.of(
                "events",
                "drone_latest_status",
                "drone_text_embeddings",
                "drone_events_by_entity",
                "restricted_zones",
                "alerts_by_bucket",
                "ingestion_counts",
                "sessions_open",
                "session_seq_applied",
                "session_timeline",
                "session_timeline_plain",
                "zone_occupancy",
                "zone_clearance",
                "drone_clearance");
        assertEquals(14, tables.size());
        for (String table : tables) {
            assertEquals(
                    1,
                    statements.stream()
                            .filter(cql -> cql.contains("CREATE TABLE IF NOT EXISTS demo." + table + " ")
                                    || cql.contains("CREATE TABLE IF NOT EXISTS demo." + table + "\n"))
                            .count(),
                    table + " is not created exactly once");
        }
    }

    /** Every statement is {@code IF NOT EXISTS}, which is what makes a restart cost nothing. */
    @Test
    void nothingIsCreatedUnconditionally() {
        for (String statement : DemoSchema.statements(DEFAULTS)) {
            assertTrue(statement.contains("IF NOT EXISTS"), statement);
        }
    }

    /**
     * The keys, which are the whole of what a query can prune on.
     *
     * <p>{@code events} is partitioned by the window and a shard of it and clustered by the event;
     * the history table is per asset, newest first; the alerts are per hour, newest first.
     */
    @Test
    void theKeysAreTheOnesEveryPathQueriesBy() {
        List<String> statements = DemoSchema.statements(DEFAULTS);

        assertTrue(statement(statements, "demo.events")
                .contains("PRIMARY KEY ((event_bucket, shard), event_id)"));
        assertTrue(statement(statements, "demo.drone_events_by_entity")
                .contains("PRIMARY KEY ((entity_id), event_time, event_id)"));
        assertTrue(statement(statements, "demo.drone_events_by_entity")
                .contains("CLUSTERING ORDER BY (event_time DESC, event_id DESC)"));
        assertTrue(statement(statements, "demo.alerts_by_bucket")
                .contains("PRIMARY KEY ((bucket), alert_time, entity_id, alert_id)"));
        assertTrue(statement(statements, "demo.alerts_by_bucket")
                .contains("CLUSTERING ORDER BY (alert_time DESC, entity_id ASC, alert_id DESC)"));
        assertTrue(statement(statements, "demo.session_seq_applied")
                .contains("PRIMARY KEY ((user_id, session_id), seq)"));
        assertTrue(statement(statements, "demo.zone_clearance")
                .contains("PRIMARY KEY ((zone_id), entity_id)"));
    }

    /**
     * Six tables are born transactional and eight are not.
     *
     * <p>The count is the one {@code /api/schema/cql} reports from {@code DESCRIBE}, and
     * {@code events} being outside is the demo's whole point: a consensus protocol in front of 2,000
     * writes a second is the opposite of what the stack argues for.
     */
    @Test
    void sixTablesOptIntoAccordAndEventsIsNotOneOfThem() {
        List<String> transactional = DemoSchema.statements(DEFAULTS).stream()
                .filter(cql -> cql.contains("transactional_mode='full'"))
                .toList();

        assertEquals(6, transactional.size());
        for (String table : List.of("sessions_open", "session_seq_applied", "session_timeline",
                "zone_occupancy", "zone_clearance", "drone_clearance")) {
            assertEquals(1, transactional.stream().filter(cql -> cql.contains("demo." + table)).count(),
                    table + " does not opt into Accord");
        }
        assertFalse(statement(DemoSchema.statements(DEFAULTS), "demo.events")
                .contains("transactional_mode"));
        assertFalse(statement(DemoSchema.statements(DEFAULTS), "demo.session_timeline_plain")
                .contains("transactional_mode"),
                "the reference table for the measurement must not be transactional");
    }

    /**
     * With Accord off, no table carries the option at all.
     *
     * <p>Not a nicety: the node refuses the option outright when the subsystem is off, and being the
     * schema step, the refusal would stop the sink before a single row was written.
     */
    @Test
    void withAccordOffNothingCarriesTheOption() {
        List<String> statements =
                DemoSchema.statements(SinkSettings.from(Map.of("CASSANDRA_ACCORD_ENABLED", "false")::get));

        assertTrue(statements.stream().noneMatch(cql -> cql.contains("transactional_mode")));
        assertEquals(16, statements.size(), "the tables are the same tables either way");
    }

    /** One table opts into CDC, and the raw event table deliberately does not. */
    @Test
    void onlyTheLiveStatusTableOptsIntoCdc() {
        List<String> statements = DemoSchema.statements(DEFAULTS);
        List<String> withCdc = statements.stream().filter(cql -> cql.contains("cdc = true")).toList();

        assertEquals(1, withCdc.size());
        assertTrue(withCdc.getFirst().contains("demo.drone_latest_status"));
        assertFalse(statement(statements, "demo.events").contains("cdc"));
    }

    @Test
    void withCdcOffNoTableAsksToBePublished() {
        List<String> statements =
                DemoSchema.statements(SinkSettings.from(Map.of("CASSANDRA_CDC_ENABLED", "false")::get));

        assertTrue(statements.stream().noneMatch(cql -> cql.contains("cdc = true")));
    }

    /** The vector's width is one declaration in two processes: the backend embeds to it. */
    @Test
    void theEmbeddingWidthIsWrittenIntoTheColumn() {
        assertEquals(1536, DemoSchema.EMBEDDING_DIMS);
        assertTrue(statement(DemoSchema.statements(DEFAULTS),
                        "CREATE TABLE IF NOT EXISTS demo.drone_text_embeddings")
                .contains("payload_vector vector<float, 1536>"));
    }

    /** The index is the storage-attached one, with no options, so it takes the server's default. */
    @Test
    void theVectorIndexIsStorageAttachedAndUnconfigured() {
        String index = statement(DemoSchema.statements(DEFAULTS), "payload_vector_idx");

        assertTrue(index.contains("USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'"));
        assertFalse(index.contains("WITH OPTIONS"));
    }

    /** The keyspace names one datacenter and one replica, which is what a laptop runs. */
    @Test
    void theKeyspaceIsOneReplicaInOneDatacenter() {
        assertTrue(DemoSchema.statements(DEFAULTS).getFirst().contains(
                "{'class': 'NetworkTopologyStrategy', 'datacenter1': 1 }"));
    }

    /** The table the raw stream goes to follows the setting, since compose can move it. */
    @Test
    void theRawTableIsTheOneTheSettingsName() {
        List<String> statements =
                DemoSchema.statements(SinkSettings.from(Map.of("TABLE", "readings")::get));

        assertTrue(statements.stream().anyMatch(
                cql -> cql.startsWith("CREATE TABLE IF NOT EXISTS demo.readings (")));
    }

    /**
     * The three zones, their capacities and their polygons.
     *
     * <p>The capacities are small so that the transaction demo can exhaust a zone in a handful of
     * steps, and the clearance page reads each one back rather than naming a number of its own.
     */
    @Test
    void theZonesCarryTheirOwnClearanceLimits() {
        assertEquals(3, DemoSchema.ZONES.size());
        assertEquals(
                List.of("zone-oslo-airport", "zone-royal-palace", "zone-fornebu"),
                DemoSchema.ZONES.stream().map(DemoSchema.DemoZone::zoneId).toList());
        assertEquals(List.of(2L, 3L, 5L),
                DemoSchema.ZONES.stream().map(DemoSchema.DemoZone::capacity).toList());
        assertEquals(List.of("critical", "critical", "warning"),
                DemoSchema.ZONES.stream().map(DemoSchema.DemoZone::severity).toList());
        for (DemoSchema.DemoZone zone : DemoSchema.ZONES) {
            assertTrue(zone.polygonWkt().startsWith("POLYGON(("), zone.zoneId());
            assertEquals(5, zone.polygonWkt().split(",").length, zone.zoneId() + " is not a closed ring");
        }
    }

    private static int indexOf(List<String> statements, String beginning) {
        for (int at = 0; at < statements.size(); at++) {
            if (statements.get(at).startsWith(beginning)) {
                return at;
            }
        }
        throw new AssertionError("no statement begins " + beginning);
    }

    /** The one statement naming this object, which is what makes the assertions above readable. */
    private static String statement(List<String> statements, String object) {
        return statements.stream()
                .filter(cql -> cql.contains(object))
                .reduce((first, second) -> {
                    throw new AssertionError("two statements name " + object);
                })
                .orElseThrow(() -> new AssertionError("no statement names " + object));
    }
}
