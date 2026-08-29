package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/**
 * The coordinator's own sentence, recovered from the driver's wrapping.
 *
 * <p>The driver prefixes the message with the query id and the error name, which the pages would
 * otherwise show ahead of the sentence that says what was wrong.
 */
class PrestoPathTest {

    @Test
    void theQueryIdAndTheErrorNameAreDropped() {
        assertEquals(
                "line 1:8: Column 'nope' cannot be resolved",
                PrestoPath.readableError(new SQLException(
                        "Query failed (#20260828_121500_00007_abcde): line 1:8:"
                                + " Column 'nope' cannot be resolved")));
    }

    /** A message the driver did not wrap is passed through, trimmed. */
    @Test
    void aMessageWithNoWrappingIsKeptAsItIs() {
        assertEquals(
                "Error connecting to localhost:8088",
                PrestoPath.readableError(new SQLException("  Error connecting to localhost:8088 ")));
    }

    /** An unterminated prefix is left alone rather than cut at a guess. */
    @Test
    void anUnclosedPrefixIsNotCut() {
        assertEquals(
                "Query failed (#20260828 no closing bracket",
                PrestoPath.readableError(
                        new SQLException("Query failed (#20260828 no closing bracket")));
    }

    @Test
    void aFailureWithNoMessageIsNamedByItsType() {
        assertEquals("SQLException", PrestoPath.readableError(new SQLException()));
        assertEquals("SQLException", PrestoPath.readableError(new SQLException("   ")));
    }
}
