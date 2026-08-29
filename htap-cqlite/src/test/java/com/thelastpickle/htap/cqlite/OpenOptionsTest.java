package com.thelastpickle.htap.cqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class OpenOptionsTest {

    @Test
    void defaultsLeaveEveryFieldToTheLibrary() {
        assertEquals(0L, OpenOptions.DEFAULTS.splits());
        assertEquals(0L, OpenOptions.DEFAULTS.batchRows());
        assertEquals(0L, OpenOptions.DEFAULTS.keyChunk());
    }

    @Test
    void theBoundIsAccepted() {
        OpenOptions options =
                new OpenOptions(OpenOptions.MAX_COUNT, OpenOptions.MAX_COUNT, OpenOptions.MAX_COUNT);
        assertEquals(OpenOptions.MAX_COUNT, options.splits());
    }

    @Test
    void aCountAboveTheBoundIsRefusedByField() {
        assertField("splits", () -> new OpenOptions(OpenOptions.MAX_COUNT + 1, 0L, 0L));
        assertField("batchRows", () -> new OpenOptions(0L, OpenOptions.MAX_COUNT + 1, 0L));
        assertField("keyChunk", () -> new OpenOptions(0L, 0L, OpenOptions.MAX_COUNT + 1));
    }

    /**
     * A negative is the case Java alone has: it crosses as a {@code uint64_t} near its
     * maximum, so the library would see a length rather than a mistake.
     */
    @Test
    void aNegativeCountIsRefusedByField() {
        assertField("splits", () -> new OpenOptions(-1L, 0L, 0L));
        assertField("batchRows", () -> new OpenOptions(0L, -1L, 0L));
        assertField("keyChunk", () -> new OpenOptions(0L, 0L, Long.MIN_VALUE));
    }

    private static void assertField(String field, Runnable construction) {
        IllegalArgumentException refusal =
                assertThrows(IllegalArgumentException.class, construction::run);
        assertTrue(
                refusal.getMessage().startsWith(field + " is "),
                "the refusal names the field, and said: " + refusal.getMessage());
    }
}
