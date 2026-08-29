package com.thelastpickle.htap.cqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CqliteExceptionTest {

    @Test
    void nestedPrefixesAreStrippedRepeatedly() {
        assertEquals(
                "no SSTable files in /data/demo/events-1234",
                CqliteException.readable(
                        "DataFusion error: External error: cqlite: no SSTable files in"
                                + " /data/demo/events-1234"));
    }

    /**
     * The two prefixes the other case does not reach, in the nesting each arrives in: a
     * failure the reader raises mid-drain comes back through Arrow's own stream, and a
     * DataFusion failure at execution rather than at planning says so.
     */
    @Test
    void theDrainsOwnPrefixesAreStrippedToo() {
        assertEquals(
                "no SSTable files",
                CqliteException.readable("Arrow error: External error: cqlite: no SSTable files"));
        assertEquals(
                "the merge stopped",
                CqliteException.readable("Execution error: cqlite: the merge stopped"));
    }

    @Test
    void aMessageWithNoPrefixIsLeftAlone() {
        assertEquals(
                "cannot parse CREATE TABLE: unexpected token",
                CqliteException.readable("cannot parse CREATE TABLE: unexpected token"));
    }

    /** DataFusion spans lines, and the dashboard shows a message in one. */
    @Test
    void whitespaceIsCollapsed() {
        assertEquals(
                "No field named x. Valid fields are a, b.",
                CqliteException.readable("  No field named x.\n  Valid fields are\ta, b.  "));
    }

    @Test
    void aLongMessageIsCutAtTheLimit() {
        String raw = "x".repeat(CqliteException.MESSAGE_LIMIT + 50);
        assertEquals(CqliteException.MESSAGE_LIMIT, CqliteException.readable(raw).length());
    }

    @Test
    void noMessageReadsAsEmpty() {
        assertEquals("", CqliteException.readable(null));
    }

    @Test
    void anUnknownCodeKeepsItsNumberBesideTheStatus() {
        CqliteException failure = new CqliteException(CqliteStatus.of(-99), -99, false, "who knows");
        assertEquals(CqliteStatus.ERROR, failure.status());
        assertEquals(-99, failure.code());
        assertFalse(failure.cancelled());
    }

    @Test
    void aCancelledStatementSaysSo() {
        CqliteException failure = new CqliteException(
                CqliteStatus.ERROR, CqliteStatus.ERROR.code(), true, CqliteStatement.CANCELLED_MESSAGE);
        assertTrue(failure.cancelled());
        assertTrue(failure.getMessage().startsWith("Cancelled: "));
    }
}
