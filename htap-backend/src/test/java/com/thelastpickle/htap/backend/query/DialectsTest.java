package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The rewriting that makes one question run five ways.
 *
 * <p>What is worth pinning is the two ways a naive rewrite goes wrong: a table name that is also an
 * alias, and a bound the caller already wrote.
 */
class DialectsTest {

    @Test
    void aTableReferenceIsRewrittenAndAnAliasOfTheSameNameIsNot() {
        assertEquals(
                "SELECT count(*) AS events FROM demo.events",
                Dialects.rewriteTables("SELECT count(*) AS events FROM events", "demo."));
    }

    @Test
    void aKeyspaceAlreadyOnTheReferenceIsReplacedRatherThanPrefixed() {
        assertEquals(
                "SELECT * FROM cassandra.demo.events",
                Dialects.rewriteTables("SELECT * FROM demo.events", "cassandra.demo."));
    }

    @Test
    void aTableTheConsoleDoesNotExposeIsLeftForTheEngineToRefuse() {
        assertEquals(
                "SELECT * FROM system.local",
                Dialects.rewriteTables("SELECT * FROM system.local", "demo."));
    }

    @Test
    void theCassandraClauseIsRemovedForTheEnginesWithoutIt() {
        assertEquals(
                "SELECT * FROM events WHERE shard = 1",
                Dialects.withoutAllowFiltering("SELECT * FROM events WHERE shard = 1 ALLOW FILTERING"));
    }

    @Test
    void aBoundTheCallerWroteIsLeftInPlace() {
        assertTrue(Dialects.hasLimit("SELECT * FROM events LIMIT 5"));
        assertFalse(Dialects.hasLimit("SELECT * FROM events WHERE limit_col = 5"));
        assertEquals("SELECT * FROM events LIMIT 5", Dialects.bounded("SELECT * FROM events LIMIT 5", 10));
        assertEquals("SELECT * FROM events LIMIT 10", Dialects.bounded("SELECT * FROM events", 10));
    }

    /** CQL wants the two clauses in one order, so a bound the caller wrote has to move. */
    @Test
    void theCqlSpellingReordersTheBoundAndAlwaysFilters() {
        assertEquals(
                "SELECT * FROM events LIMIT 10 ALLOW FILTERING",
                Dialects.cql("SELECT * FROM demo.events ALLOW FILTERING LIMIT 5", 10));
    }

    @Test
    void theFourSqlEnginesGetTheirOwnTablePrefixAndABound() {
        assertEquals(
                "SELECT * FROM cassandra.demo.events LIMIT 10",
                Dialects.sql("SELECT * FROM events ALLOW FILTERING", "cassandra.demo.", 10));
        assertEquals(
                "SELECT * FROM events LIMIT 10",
                Dialects.sql("SELECT * FROM demo.events", "", 10));
    }
}
