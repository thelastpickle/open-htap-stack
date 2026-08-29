package com.thelastpickle.htap.backend.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.thelastpickle.htap.backend.api.dto.SqlQueryRequest;
import org.junit.jupiter.api.Test;

/**
 * What the console's request means when it leaves fields out.
 *
 * <p>Normalised in the record rather than in the route, because a browser posting only {@code sql}
 * is the commonest request the console makes and the Python's model carried the same three
 * defaults.
 */
class SqlQueryRequestTest {

    @Test
    void aRequestCarryingOnlyAStatementTakesTheDefaults() {
        SqlQueryRequest request = new SqlQueryRequest("SELECT * FROM events", 0, null, false);

        assertEquals(10, request.limit());
        assertEquals("cassandra", request.engine());
        assertFalse(request.reuseSnapshot());
    }

    /** Clamped rather than refused: asking for more than the maximum means the maximum. */
    @Test
    void aLimitAboveTheMaximumIsTheMaximum() {
        assertEquals(1000, limit(1_000_000));
        assertEquals(1000, limit(1001));
        assertEquals(1000, limit(1000));
    }

    /** A limit of nothing is the default rather than a statement that returns nothing. */
    @Test
    void aLimitOfNothingOrLessIsTheDefault() {
        assertEquals(10, limit(0));
        assertEquals(10, limit(-5));
        assertEquals(1, limit(1));
    }

    @Test
    void aBlankEngineIsTheTransactionalPath() {
        assertEquals("cassandra", new SqlQueryRequest("SELECT 1", 10, "   ", false).engine());
        assertEquals("cqlite", new SqlQueryRequest("SELECT 1", 10, "cqlite", false).engine());
    }

    /** An unknown name is kept, so the route can name it in its refusal. */
    @Test
    void anUnknownEngineNameIsKeptForTheRefusalToQuote() {
        assertEquals("duckdb", new SqlQueryRequest("SELECT 1", 10, "duckdb", false).engine());
    }

    private static int limit(int asked) {
        return new SqlQueryRequest("SELECT 1", asked, null, false).limit();
    }
}
