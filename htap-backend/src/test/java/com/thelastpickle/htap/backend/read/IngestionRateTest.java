package com.thelastpickle.htap.backend.read;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class IngestionRateTest {

    private final AtomicLong clock = new AtomicLong();
    private final IngestionRate rate = new IngestionRate(clock::get);

    private void advance(double seconds) {
        clock.addAndGet((long) (seconds * 1_000_000_000L));
    }

    @Test
    void theFirstObservationHasNothingToDifference() {
        assertEquals(0.0, rate.observe(1_000L));
    }

    @Test
    void twoObservationsGiveEventsPerSecond() {
        rate.observe(1_000L);
        advance(4);

        assertEquals(500.0, rate.observe(3_000L));
    }

    /**
     * Called again too soon it repeats the last figure and keeps the older baseline, so a
     * dashboard polling every second still gets a fresh figure every two seconds rather than
     * none: were the baseline replaced on every call, the interval would never elapse.
     */
    @Test
    void aCallInsideTheIntervalRepeatsTheLastFigureAndKeepsTheBaseline() {
        rate.observe(0L);
        advance(4);
        assertEquals(500.0, rate.observe(2_000L));

        advance(1);
        assertEquals(500.0, rate.observe(3_000L));

        advance(1);
        assertEquals(1_000.0, rate.observe(4_000L));
    }

    /** The counters were truncated; start again rather than reporting a negative rate. */
    @Test
    void aCounterThatWentBackwardsReportsZero() {
        rate.observe(5_000L);
        advance(4);

        assertEquals(0.0, rate.observe(10L));
    }

    @Test
    void theRateIsRoundedToATenth() {
        rate.observe(0L);
        advance(3);

        assertEquals(333.3, rate.observe(1_000L));
    }
}
