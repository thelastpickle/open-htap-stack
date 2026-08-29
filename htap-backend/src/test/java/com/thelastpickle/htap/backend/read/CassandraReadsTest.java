package com.thelastpickle.htap.backend.read;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The bucket arithmetic, which needs no session. */
class CassandraReadsTest {

    private static final Instant AT = Instant.parse("2026-08-29T14:47:31Z");

    @Test
    void theHistoryCoversTwoBucketsAnHourOldestFirst() {
        List<String> keys = CassandraReads.historyBuckets(AT, 2);

        assertEquals(
                List.of(
                        "2026-08-29T13:00",
                        "2026-08-29T13:30",
                        "2026-08-29T14:00",
                        "2026-08-29T14:30"),
                keys);
    }

    /** The last key is the bucket now filling, so the chart's newest bar is the live one. */
    @Test
    void theLastKeyIsTheBucketHoldingTheGivenInstant() {
        List<String> keys = CassandraReads.historyBuckets(AT, 8);

        assertEquals(16, keys.size());
        assertEquals("2026-08-29T14:30", keys.get(15));
        assertEquals("2026-08-29T07:00", keys.getFirst());
    }

    /** Half-hour steps, so a window crossing midnight rolls the date rather than the hour. */
    @Test
    void aWindowCrossingMidnightRollsTheDate() {
        List<String> keys = CassandraReads.historyBuckets(Instant.parse("2026-09-01T00:10:00Z"), 1);

        assertEquals(List.of("2026-08-31T23:30", "2026-09-01T00:00"), keys);
    }
}
