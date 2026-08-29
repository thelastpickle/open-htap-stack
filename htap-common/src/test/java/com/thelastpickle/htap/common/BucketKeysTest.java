package com.thelastpickle.htap.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The keys here were produced by running the Python this class replaces:
 * {@code _thirty_min_bucket} at {@code ingress/consumer/consumer.py:588} and the
 * {@code strftime("%Y-%m-%dT%H")} at {@code consumer.py:908}. They are exact, because a
 * reader that spells a key differently from the writer reads an empty chart and reports
 * nothing wrong.
 */
class BucketKeysTest {

    private static Instant at(String iso) {
        return OffsetDateTime.parse(iso).toInstant();
    }

    @Test
    void theHalfHourIsChosenAtMinuteThirty() {
        assertEquals("2026-08-28T14:00", BucketKeys.thirtyMinute(at("2026-08-28T14:00:00Z")));
        assertEquals("2026-08-28T14:00", BucketKeys.thirtyMinute(at("2026-08-28T14:29:59.999999Z")));
        assertEquals("2026-08-28T14:30", BucketKeys.thirtyMinute(at("2026-08-28T14:30:00Z")));
        assertEquals("2026-08-28T14:30", BucketKeys.thirtyMinute(at("2026-08-28T14:59:59Z")));
    }

    @Test
    @DisplayName("the suffix is the window and never the real minute")
    void theMinuteFieldIsNotFormatted() {
        // What a `mm` pattern would get wrong, and the reason the suffix is two literal
        // strings: :47 must read as :30 and :07 as :00.
        assertEquals("2026-08-28T14:30", BucketKeys.thirtyMinute(at("2026-08-28T14:47:31Z")));
        assertEquals("2026-08-28T14:00", BucketKeys.thirtyMinute(at("2026-08-28T14:07:31Z")));
    }

    @Test
    void theHourKeyStopsAtTheHour() {
        assertEquals("2026-08-28T14", BucketKeys.hour(at("2026-08-28T14:00:00Z")));
        assertEquals("2026-08-28T14", BucketKeys.hour(at("2026-08-28T14:59:59.999999Z")));
        assertEquals("2026-08-28T15", BucketKeys.hour(at("2026-08-28T15:00:00Z")));
    }

    @Test
    @DisplayName("both keys are zero-padded, so they sort as they order in time")
    void singleDigitFieldsArePadded() {
        // The dashboard walks these keys backwards and the sink writes them forwards, and a
        // bucket named 2026-1-1T9 would neither match nor sort.
        assertEquals("2026-01-02T09", BucketKeys.hour(at("2026-01-02T09:05:00Z")));
        assertEquals("2026-01-02T09:00", BucketKeys.thirtyMinute(at("2026-01-02T09:05:00Z")));
    }

    @Test
    @DisplayName("an offset-carrying instant is keyed by its UTC hour")
    void theKeysAreUtc() {
        // 19:44+05:30 is 14:14 UTC. As in EventPartitions, the parameter is an Instant and so
        // carries no offset to go wrong; what this pins is that neither key consults the
        // default zone, which is what a `LocalDateTime.now()` in either would have done.
        assertEquals("2026-08-28T14", BucketKeys.hour(at("2026-08-28T19:44:00+05:30")));
        assertEquals("2026-08-28T14:00", BucketKeys.thirtyMinute(at("2026-08-28T19:44:00+05:30")));
        assertEquals("2026-08-28T14:30", BucketKeys.thirtyMinute(at("2026-08-28T20:14:00+05:30")));
    }

    @Test
    @DisplayName("the hour key is the thirty-minute key without its suffix")
    void theTwoKeysShareTheirPrefix() {
        // Their agreement is what lets one formatter serve both, and it is worth a case
        // because a later change to either pattern would be silent otherwise.
        for (int minute = 0; minute < 60; minute++) {
            Instant at = at("2026-08-28T14:00:00Z").plusSeconds(minute * 60L);
            String hour = BucketKeys.hour(at);
            assertEquals(hour, BucketKeys.thirtyMinute(at).substring(0, hour.length()));
        }
    }
}
