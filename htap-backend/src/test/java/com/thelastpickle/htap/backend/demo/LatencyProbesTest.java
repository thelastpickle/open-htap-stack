package com.thelastpickle.htap.backend.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The timing rule the three probes share.
 *
 * <p>Only {@code timed} is exercised here. The probes themselves each need a live engine to choose
 * anything to read, so what they do is asserted by the workflow's dashboard step; what a unit test
 * can hold to is that setup stays outside the measurement and that a tier which cannot answer
 * reports null rather than a figure.
 *
 * <p>Constructed with no engines at all, which is safe because {@code timed} touches none of them:
 * it calls what it is given and reads the clock.
 */
class LatencyProbesTest {

    private final AtomicLong nanos = new AtomicLong();
    private final LatencyProbes probes =
            new LatencyProbes(null, null, null, null, Clock.systemUTC(), nanos::get);

    @Test
    void aFigureIsTheQuerysOwnElapsedTimeInMilliseconds() {
        Double took = probes.timed(() -> () -> nanos.addAndGet(2_500_000L));

        assertEquals(2.5, took);
    }

    /** The point of the two halves: choosing what to read is a scan, and it is not the reading. */
    @Test
    void whatHappensBeforeTheQueryIsNotTimed() {
        Double took = probes.timed(() -> {
            nanos.addAndGet(900_000_000L);
            return () -> nanos.addAndGet(1_000_000L);
        });

        assertEquals(1.0, took);
    }

    @Test
    void aTierThatCannotAnswerReportsNothingRatherThanZero() {
        assertNull(probes.timed(() -> null));
    }

    @Test
    void aFailedSetupIsATierThatCannotAnswer() {
        assertNull(probes.timed(() -> {
            throw new IllegalStateException("Cassandra not connected");
        }));
    }

    @Test
    void aQueryThatRaisesIsATierThatCannotAnswer() {
        Double took = probes.timed(() -> () -> {
            throw new IllegalStateException("no host left to try");
        });

        assertNull(took);
    }

    @Test
    void aFigureIsRoundedToATenthOfAMillisecond() {
        assertEquals(1.2, probes.timed(() -> () -> nanos.addAndGet(1_234_567L)));
    }
}
