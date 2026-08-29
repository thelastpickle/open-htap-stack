package com.thelastpickle.htap.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.thelastpickle.htap.sink.SinkFakes.RecordingSession;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** What the sink sends the node at startup, and what it sends only when it has to. */
class SchemaOwnerTest {

    private static final SinkSettings SETTINGS = SinkSettings.from(name -> null);

    private final RecordingSession node = new RecordingSession();

    /** Every statement of the schema, then the two seeds, in that order. */
    @Test
    void theWholeSchemaIsAppliedBeforeAnythingIsSeeded() {
        node.answers = cql -> List.of();
        owner().ensure();

        List<String> statements = DemoSchema.statements(SETTINGS);
        assertEquals(statements, node.executed.subList(0, statements.size()));
        assertTrue(node.executed.size() > statements.size(), "the seeds follow the schema");
        assertTrue(node.matching("INSERT INTO demo.restricted_zones").size() == 3);
        assertTrue(node.matching("INSERT INTO demo.zone_occupancy").size() == 3);
    }

    /**
     * The zone seed is conditional and the capacity seed is not.
     *
     * <p>The first can be, because {@code restricted_zones} is an ordinary table; the second cannot,
     * because {@code zone_occupancy} is in Accord's care and a lightweight transaction is a second
     * consensus path over the same row.
     */
    @Test
    void theZoneSeedIsConditionalAndTheCapacitySeedReadsFirst() {
        node.answers = cql -> List.of();
        owner().ensure();

        assertTrue(node.matching("INSERT INTO demo.restricted_zones").getFirst()
                .endsWith("IF NOT EXISTS"));
        assertFalse(node.matching("INSERT INTO demo.zone_occupancy").getFirst()
                .contains("IF NOT EXISTS"));
        assertEquals(3, node.matching("SELECT zone_id FROM demo.zone_occupancy").size());
    }

    /**
     * Both halves of the capacity seed carry QUORUM.
     *
     * <p>Forced by the table being transactional: {@code full} routes even a plain read or write
     * through Accord, which refuses the driver's default with "ConsistencyLevel LOCAL_ONE is
     * unsupported with Accord for write/commit".
     */
    @Test
    void theCapacitySeedIsAtQuorum() {
        node.answers = cql -> List.of();
        owner().ensure();

        assertEquals(
                ConsistencyLevel.QUORUM,
                node.consistency.get(node.matching("SELECT zone_id FROM demo.zone_occupancy").getFirst()));
        assertEquals(
                ConsistencyLevel.QUORUM,
                node.consistency.get(node.matching("INSERT INTO demo.zone_occupancy").getFirst()));
    }

    /**
     * A capacity already in use is left alone.
     *
     * <p>Restarting the sink must not hand back clearances a zone has granted, which resetting the
     * count would do while leaving every {@code zone_clearance} row in place.
     */
    @Test
    void aZoneThatAlreadyHasACapacityIsNotSeededAgain() {
        node.answers = cql -> cql.startsWith("SELECT zone_id FROM demo.zone_occupancy")
                ? List.of(SinkFakes.row(Map.of("zone_id", "zone-oslo-airport")))
                : List.of();

        owner().seedZoneOccupancy();

        assertEquals(3, node.matching("SELECT zone_id FROM demo.zone_occupancy").size());
        assertEquals(0, node.matching("INSERT INTO demo.zone_occupancy").size());
    }

    /**
     * One zone the node refuses does not stop the others being seeded.
     *
     * <p>The zone that raises is the second of three, so the third being attempted at all is the
     * property: a seeding loop that gave up on the first refusal would leave a zone the alerting
     * never scores against.
     */
    @Test
    void aRefusedSeedLeavesTheRestToBeSeeded() {
        node.answers = cql -> List.of();
        node.failing.add("zone-royal-palace");

        owner().seedZones();

        assertEquals(3, node.matching("INSERT INTO demo.restricted_zones").size());
        assertEquals(
                List.of("zone-oslo-airport", "zone-royal-palace", "zone-fornebu"),
                node.bound.stream().map(values -> (String) values[0]).toList());
    }

    /** With the option already right, the reconcile issues no schema change. */
    @Test
    void cdcIsNotAlteredWhenItAlreadyAgrees() {
        node.answers = cql -> List.of(SinkFakes.row(Map.of("cdc", true)));

        owner().ensureCdc();

        assertEquals(1, node.executed.size());
        assertTrue(node.executed.getFirst().startsWith("SELECT cdc FROM system_schema.tables"));
    }

    /**
     * A table that predates the option is altered onto it.
     *
     * <p>Which is the whole reason the reconcile exists: {@code CREATE TABLE IF NOT EXISTS} applies
     * none of its options to a table that already exists, so a stack running since before CDC was
     * added would otherwise keep the option off and leave the Sidecar with nothing to publish.
     */
    @Test
    void cdcIsAlteredOnWhenTheTablePredatesIt() {
        node.answers = cql -> List.of(SinkFakes.row(Map.of("cdc", false)));

        owner().ensureCdc();

        assertEquals(
                "ALTER TABLE demo.drone_latest_status WITH cdc = true;",
                node.executed.get(1));
    }

    /** And off again, so turning CDC off leaves no table asking a publisher to follow it. */
    @Test
    void cdcIsAlteredOffWhenTheEnvironmentSaysSo() {
        node.answers = cql -> List.of(SinkFakes.row(Map.of("cdc", true)));

        new SchemaOwner(
                        node.session(),
                        SinkSettings.from(Map.of("CASSANDRA_CDC_ENABLED", "false")::get))
                .ensureCdc();

        assertEquals(
                "ALTER TABLE demo.drone_latest_status WITH cdc = false;",
                node.executed.get(1));
    }

    /** A table that does not exist yet reads as the option being off, and needs no alter. */
    @Test
    void anAbsentTableIsNotAltered() {
        node.answers = cql -> List.of();

        new SchemaOwner(
                        node.session(),
                        SinkSettings.from(Map.of("CASSANDRA_CDC_ENABLED", "false")::get))
                .ensureCdc();

        assertEquals(1, node.executed.size());
    }

    private SchemaOwner owner() {
        return new SchemaOwner(node.session(), SETTINGS);
    }
}
