package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.config.EventSettings;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * Which window the compare page is told to ask about, and what is claimed of it.
 *
 * <p>Both lookups are seams, so the walk backwards is settled here without a cluster or a broker.
 * The cluster and the broker are passed as null for the same reason: neither is reached on this
 * path, and a test that supplied them would be testing the container instead.
 */
class WindowsTest {

    /** Inside the 12:30 window, so the newest closed one is 12:15. */
    private static final Instant NOW = Instant.parse("2026-08-28T12:37:00Z");

    private final List<Instant> ends = new ArrayList<>();

    @Test
    void theNewestClosedWindowHoldingEventsIsChosenAndItsEndIsWhereItsLabelSays() {
        WindowChoice choice = windows(15, 16).choose(
                NOW, holding("2026-08-28T12:15", "2026-08-28T12:00"), settled(true));

        assertEquals("2026-08-28T12:30", choice.current());
        assertEquals("2026-08-28T12:15", choice.bucket());
        assertTrue(choice.closed());
        assertTrue(choice.settled());
        assertEquals(List.of(Instant.parse("2026-08-28T12:30:00Z")), ends);
    }

    /** The configured width and shard count travel with the answer, so a page can label it. */
    @Test
    void theWidthAndTheShardCountAreReportedAsConfigured() {
        WindowChoice choice = windows(30, 4).choose(
                NOW, holding("2026-08-28T12:00"), settled(true));

        assertEquals(30, choice.bucketMinutes());
        assertEquals(4, choice.shards());
        assertEquals("2026-08-28T12:30", choice.current());
        assertEquals("2026-08-28T12:00", choice.bucket());
        assertEquals(List.of(Instant.parse("2026-08-28T12:30:00Z")), ends);
    }

    /** An empty window is stepped over rather than reported, and only the one found is asked about. */
    @Test
    void emptyWindowsAreSteppedOver() {
        WindowChoice choice =
                windows(15, 16).choose(NOW, holding("2026-08-28T11:45"), settled(false));

        assertEquals("2026-08-28T11:45", choice.bucket());
        assertTrue(choice.closed());
        assertEquals(List.of(Instant.parse("2026-08-28T12:00:00Z")), ends);
    }

    /**
     * A demo minutes old has no closed window holding anything, and the window still filling is by
     * definition still being written to, so it is named and nothing is claimed of it.
     */
    @Test
    void withNoClosedWindowHoldingEventsTheOneFillingIsNamedAndNotSettled() {
        WindowChoice choice = windows(15, 16).choose(NOW, bucket -> false, settled(true));

        assertEquals("2026-08-28T12:30", choice.current());
        assertEquals("2026-08-28T12:30", choice.bucket());
        assertFalse(choice.closed());
        assertFalse(choice.settled());
        assertEquals("the window is still filling", choice.settledDetail());
        assertEquals(List.of(), ends);
    }

    /** Bounded at eight windows back, which is two hours at the default width. */
    @Test
    void aWindowBeyondTheLookbackIsNotFound() {
        assertEquals(
                "2026-08-28T10:30",
                windows(15, 16).choose(NOW, holding("2026-08-28T10:30"), settled(true)).bucket());
        assertEquals(
                "2026-08-28T12:30",
                windows(15, 16).choose(NOW, holding("2026-08-28T10:15"), settled(true)).bucket());
    }

    /** The sink's verdict is passed through rather than restated. */
    @Test
    void theSinksOwnWordsAreWhatIsReported() {
        WindowChoice choice = windows(15, 16).choose(
                NOW,
                holding("2026-08-28T12:15"),
                end -> new SinkProgress.Verdict(false, "1 of 12 partitions are short"));

        assertFalse(choice.settled());
        assertEquals("1 of 12 partitions are short", choice.settledDetail());
    }

    /** The inverse of the label the sink writes, which is what the window's end is derived from. */
    @Test
    void aWindowsLabelParsesBackToTheInstantItBegan() {
        assertEquals(Instant.parse("2026-08-28T12:15:00Z"), Windows.startOf("2026-08-28T12:15"));
        assertEquals(Instant.parse("2026-01-01T00:00:00Z"), Windows.startOf("2026-01-01T00:00"));
    }

    /**
     * A shard count of zero is refused rather than asked about.
     *
     * <p>Zero shards makes the shard list empty, and {@code shard IN ()} is a SyntaxException the
     * read's own catch would swallow once per candidate window: a misconfigured stack would report
     * eight warnings and no window rather than the one thing wrong with it.
     */
    @Test
    void aShardCountOfZeroIsRefusedRatherThanAskedAbout() {
        IllegalArgumentException refused = assertThrows(
                IllegalArgumentException.class, () -> windows(15, 0).holdsEvents("2026-08-28T12:15"));

        assertEquals("EVENT_SHARDS must be at least 1, got 0", refused.getMessage());
    }

    private static Windows windows(int bucketMinutes, int shards) {
        return new Windows(new EventSettings() {
            @Override
            public int bucketMinutes() {
                return bucketMinutes;
            }

            @Override
            public int shards() {
                return shards;
            }
        }, null, null);
    }

    private static Predicate<String> holding(String... buckets) {
        return Set.of(buckets)::contains;
    }

    /** Records every window end the sink was asked about, which is the claim's subject. */
    private Function<Instant, SinkProgress.Verdict> settled(boolean answer) {
        return end -> {
            ends.add(end);
            return new SinkProgress.Verdict(answer, answer ? "settled" : "not settled");
        };
    }
}
