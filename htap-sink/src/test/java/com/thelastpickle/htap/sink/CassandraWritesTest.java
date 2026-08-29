package com.thelastpickle.htap.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * The one derivation in the write layer.
 *
 * <p>What each statement binds is settled by running the sink against the stack, since a wrong column
 * type is the node refusing and nothing a fake would notice.
 */
class CassandraWritesTest {

    private static final Instant AT = Instant.parse("2026-08-29T12:00:00Z");

    /** How far behind the reading is by the time it is written, truncated to whole seconds. */
    @Test
    void theTelemetryAgeIsWholeSecondsBehind() {
        assertEquals(0, CassandraWrites.telemetryAgeSeconds(AT, AT));
        assertEquals(0, CassandraWrites.telemetryAgeSeconds(AT, AT.plusMillis(999)));
        assertEquals(1, CassandraWrites.telemetryAgeSeconds(AT, AT.plusMillis(1001)));
        assertEquals(1205, CassandraWrites.telemetryAgeSeconds(AT, AT.plusSeconds(1205)));
    }

    /**
     * A reading from the future is not a negative age.
     *
     * <p>A producer whose clock is ahead of the sink's would otherwise write a negative number into a
     * column the dashboard reads as staleness.
     */
    @Test
    void aReadingFromTheFutureIsNotNegative() {
        assertEquals(0, CassandraWrites.telemetryAgeSeconds(AT.plusSeconds(30), AT));
        assertEquals(0, CassandraWrites.telemetryAgeSeconds(AT.plusMillis(500), AT));
    }
}
