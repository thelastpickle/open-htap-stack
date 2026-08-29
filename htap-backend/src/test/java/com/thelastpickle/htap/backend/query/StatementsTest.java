package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** The one thing between a browser and a write on any of the five paths. */
class StatementsTest {

    @Test
    void aSelectPassesAndIsStrippedOfWhatCqlshWouldHaveAdded() {
        assertEquals("SELECT * FROM events", Statements.validate("  SELECT * FROM events ;  "));
        assertEquals("SELECT 1", Statements.validate("SELECT 1;;"));
    }

    @Test
    void twoStatementsAreRefused() {
        assertEquals(
                "Only a single statement is allowed",
                refusal("SELECT 1; SELECT 2"));
    }

    @Test
    void nothingIsRefusedRatherThanSentOn() {
        assertEquals("Empty query", refusal(null));
        assertEquals("Empty query", refusal("   "));
        assertEquals("Empty query", refusal(";"));
    }

    @Test
    void aStatementThatDoesNotReadIsRefused() {
        assertEquals("Only SELECT queries are allowed", refusal("WITH t AS (SELECT 1) SELECT * FROM t"));
        assertEquals("Only SELECT queries are allowed", refusal("SHOW TABLES"));
    }

    /** Refused although it starts with SELECT: a subquery is where a write would hide. */
    @Test
    void aWriteWordAnywhereIsRefusedAndNamed() {
        assertEquals(
                "Forbidden keyword in a read-only console: DELETE",
                refusal("SELECT * FROM (delete from events) x"));
    }

    /**
     * The keywords are matched on word boundaries, so a name that contains one is not a write: a
     * substring test would refuse the first of these for holding CREATE and the second for
     * TRUNCATE.
     */
    @Test
    void aNameThatContainsAKeywordIsNotAWrite() {
        assertEquals(
                "SELECT created_at, updated_ts FROM events",
                Statements.validate("SELECT created_at, updated_ts FROM events"));
        assertEquals(
                "SELECT * FROM truncate_log",
                Statements.validate("SELECT * FROM truncate_log"));
    }

    /**
     * A write word inside a string literal is refused as well, and that is the rule's cost:
     * nothing here parses SQL, so the alternative would be to read the literal as text the engine
     * will not act on and thereby to trust a parser this console does not have.
     */
    @Test
    void aWriteWordInsideALiteralIsRefusedToo() {
        assertEquals(
                "Forbidden keyword in a read-only console: INSERT",
                refusal("SELECT * FROM events WHERE event_type = 'insert'"));
    }

    private static String refusal(String sql) {
        return assertThrows(Statements.Refused.class, () -> Statements.validate(sql)).getMessage();
    }
}
