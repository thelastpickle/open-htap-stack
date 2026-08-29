package com.thelastpickle.htap.backend.api.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The one thing a console body must carry. */
class SqlConsoleRequestTest {

    @Test
    void aStatementIsEnough() {
        assertEquals(Optional.empty(), new SqlConsoleRequest("SELECT 1").outOfRange());
    }

    /** A missing field and a blank one are the same refusal: neither is a statement. */
    @Test
    void nothingToRunIsRefused() {
        assertTrue(new SqlConsoleRequest(null).outOfRange().isPresent());
        assertEquals(
                Optional.of("sql must not be empty"), new SqlConsoleRequest("   \n").outOfRange());
    }
}
