package com.thelastpickle.htap.backend.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.transaction.Clearance;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The schema's own invariants, and the ones its comments claim. */
class ConsoleSchemaTest {

    @Test
    void everyTableIsCreated_droppedAndCounted() {
        assertEquals(5, ConsoleSchema.TABLES.size());
        for (String table : ConsoleSchema.TABLES) {
            assertTrue(
                    ConsoleSchema.SCHEMA.stream()
                            .anyMatch(sql -> sql.startsWith("CREATE TABLE " + table + " (")),
                    table + " has no CREATE TABLE");
            assertTrue(
                    ConsoleSchema.RESET.contains("DROP TABLE IF EXISTS " + table + ";"),
                    table + " is not dropped by a reset");
        }
        assertEquals(
                ConsoleSchema.TABLES.stream()
                        .map(table -> "SELECT COUNT(*) AS n FROM " + table + ";")
                        .toList(),
                ConsoleSchema.counts());
    }

    /**
     * A reset drops the referring tables first.
     *
     * <p>Nothing enforces that here, since a foreign key in this engine is accepted and ignored; the
     * order is asserted so the list stays correct against an engine that one day holds one.
     */
    @Test
    void aResetDropsChildrenBeforeParents() {
        int legs = ConsoleSchema.RESET.indexOf("DROP TABLE IF EXISTS flight_legs;");
        int flights = ConsoleSchema.RESET.indexOf("DROP TABLE IF EXISTS flights;");
        assertTrue(legs < flights, "flight_legs must be dropped before flights");
        for (String parent : List.of("drones", "zones", "operators")) {
            assertTrue(
                    flights < ConsoleSchema.RESET.indexOf("DROP TABLE IF EXISTS " + parent + ";"),
                    "flights must be dropped before " + parent);
        }
    }

    /** {@code DROP INDEX} is unimplemented in both forms, so a {@code DROP TABLE} is what removes one. */
    @Test
    void aResetNeverDropsAnIndex() {
        assertTrue(ConsoleSchema.SCHEMA.stream().anyMatch(sql -> sql.startsWith("CREATE INDEX ")));
        assertTrue(ConsoleSchema.RESET.stream().noneMatch(sql -> sql.contains("DROP INDEX")));
    }

    /**
     * No {@code IF NOT EXISTS} on a create, which is what makes re-running the schema report
     * "already exists" per statement rather than passing silently.
     */
    @Test
    void noCreateGuardsItselfWithIfNotExists() {
        assertTrue(ConsoleSchema.SCHEMA.stream().noneMatch(sql -> sql.contains("IF NOT EXISTS")));
    }

    @Test
    void theSeedRunsAfterEveryCreate() {
        List<String> all = ConsoleSchema.schemaAndSeed();
        assertEquals(ConsoleSchema.SCHEMA.size() + ConsoleSchema.SEED.size(), all.size());
        int lastCreate = 0;
        int firstInsert = all.size();
        for (int i = 0; i < all.size(); i++) {
            if (all.get(i).startsWith("CREATE ") || all.get(i).startsWith("ALTER ")) {
                lastCreate = i;
            } else if (all.get(i).startsWith("INSERT ") && i < firstInsert) {
                firstInsert = i;
            }
        }
        assertTrue(lastCreate < firstInsert, "a seed row is inserted before the schema is complete");
    }

    @Test
    void everyStatementIsTerminated() {
        for (String sql : ConsoleSchema.schemaAndSeed()) {
            assertTrue(sql.endsWith(";"), sql);
        }
        for (String sql : ConsoleSchema.RESET) {
            assertTrue(sql.endsWith(";"), sql);
        }
    }

    /**
     * The zones this register names are the airspace the Accord ledger admits drones to.
     *
     * <p>The claim the page makes is that the two halves differ in how they hold a capacity and not
     * in what they are about, so a zone code drifting apart here would make that claim false.
     */
    @Test
    void theZonesAreTheOnesTheClearanceDemoUses() {
        String zones = String.join("\n", ConsoleSchema.SEED);
        assertTrue(zones.contains("'" + Clearance.DEMO_ZONE + "'"), Clearance.DEMO_ZONE);
        assertTrue(
                zones.contains("'" + Clearance.DEMO_MEASURE_ZONE + "'"),
                Clearance.DEMO_MEASURE_ZONE);
    }

    /** A bound parameter answers no rows here and raises nothing, so the schema binds none. */
    @Test
    void nothingIsParameterised() {
        for (String sql : ConsoleSchema.schemaAndSeed()) {
            assertFalse(sql.contains("?"), sql);
        }
    }
}
