package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Reading the two engines' start stamps into one age.
 *
 * <p>Spark's spelling is the reason this exists: a zone abbreviation where an offset belongs is not
 * ISO-8601, and the stamps arrive from two engines that agree about nothing else.
 */
class StampsTest {

    private static final Instant NOW = Instant.parse("2026-08-17T12:02:52.041Z");

    /** Spark's own spelling, which no ISO-8601 parser accepts unaided. */
    @Test
    void aSparkStampCarriesAZoneAbbreviationWhereAnOffsetBelongs() {
        assertEquals(60.0, Stamps.ageS("2026-08-17T12:01:52.041GMT", NOW));
    }

    /** Presto's spelling, which passes through the swap untouched. */
    @Test
    void aPrestoStampCarriesARealOffset() {
        assertEquals(60.0, Stamps.ageS("2026-08-17T14:01:52.041+02:00", NOW));
        assertEquals(60.0, Stamps.ageS("2026-08-17T12:01:52.041Z", NOW));
    }

    @Test
    void anAgeIsRoundedToATenthAsThePageShowsIt() {
        assertEquals(0.1, Stamps.ageS("2026-08-17T12:02:51.987GMT", NOW));
    }

    /**
     * A stamp that cannot be read gives 0 rather than hiding the row, which is the point of reading
     * the page at all. Absent, blank and nonsense are the three ways it arrives.
     */
    @Test
    void anUnreadableStampIsNoAgeRatherThanNoRow() {
        assertEquals(0.0, Stamps.ageS(null, NOW));
        assertEquals(0.0, Stamps.ageS("  ", NOW));
        assertEquals(0.0, Stamps.ageS("null", NOW));
        assertEquals(0.0, Stamps.ageS("2026-08-17 12:01:52", NOW));
    }

    /**
     * A clock skew between this backend and an engine must not report a job as starting in the
     * future, which the page would draw as a negative age.
     */
    @Test
    void aStampFromTheFutureIsZeroRatherThanNegative() {
        assertEquals(0.0, Stamps.ageS("2026-08-17T12:09:00.000GMT", NOW));
    }
}
