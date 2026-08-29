package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The point read sampled beside every path the comparison runs.
 *
 * <p>A read that never came back is the finding the probe exists for, so what has to hold is that a
 * failing read is counted and the sampling carries on. The interval is this test's own, so nothing
 * here waits on a wall clock.
 */
class OltpProbeTest {

    /** A millisecond between readings, so a test waits on its latch rather than on the interval. */
    private static final Duration NO_WAIT = Duration.ofMillis(1);

    /** A millisecond per read, in the units {@code nanoTime} is read in. */
    private final AtomicLong nanos = new AtomicLong();

    @Test
    @Timeout(10)
    void aReadingIsTheReadsOwnElapsedTimeInMilliseconds() throws InterruptedException {
        CountDownLatch read = new CountDownLatch(2);

        try (OltpProbe probe = OltpProbe.start(read::countDown, NO_WAIT, this::tick)) {
            assertTrue(read.await(5, TimeUnit.SECONDS));
            OltpImpact impact = probe.impact();

            assertTrue(impact.samples() >= 2, "took " + impact.samples() + " readings");
            assertEquals(1.0, impact.p50Ms());
            assertEquals(0, impact.failures());
        }
    }

    /** A point read that did not come back is what the comparison is looking for. */
    @Test
    @Timeout(10)
    void aReadThatRaisesIsCountedAndTheSamplingCarriesOn() throws InterruptedException {
        CountDownLatch attempts = new CountDownLatch(4);
        AtomicInteger attempt = new AtomicInteger();

        try (OltpProbe probe = OltpProbe.start(
                () -> {
                    attempts.countDown();
                    if (attempt.incrementAndGet() % 2 == 1) {
                        throw new IllegalStateException("no host left to try");
                    }
                },
                NO_WAIT,
                this::tick)) {
            assertTrue(attempts.await(5, TimeUnit.SECONDS));
            OltpImpact impact = probe.impact();

            assertTrue(impact.failures() >= 1, "counted " + impact.failures() + " failures");
            assertTrue(impact.samples() >= 1, "took " + impact.samples() + " readings");
        }
    }

    /** Read before closing, which is what the comparison does: the probe outlives the reading. */
    @Test
    @Timeout(10)
    void theReadingsCanBeAskedForWhileTheProbeIsStillRunning() throws InterruptedException {
        CountDownLatch read = new CountDownLatch(1);

        try (OltpProbe probe = OltpProbe.start(read::countDown, NO_WAIT, this::tick)) {
            assertTrue(read.await(5, TimeUnit.SECONDS));

            assertTrue(probe.impact().samples() >= 1);
        }
    }

    /** Closing stops the thread, so a comparison's probes do not accumulate over a session. */
    @Test
    @Timeout(10)
    void closingStopsTheReading() throws InterruptedException {
        AtomicInteger reads = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);

        OltpProbe probe = OltpProbe.start(
                () -> {
                    reads.incrementAndGet();
                    started.countDown();
                },
                NO_WAIT,
                this::tick);
        assertTrue(started.await(5, TimeUnit.SECONDS));
        probe.close();

        int afterClose = reads.get();
        Thread.sleep(50);

        assertEquals(afterClose, reads.get());
    }

    /** A probe closed before it read anything reports nothing rather than a zero reading. */
    @Test
    @Timeout(10)
    void aProbeThatNeverReadHasNoReadings() {
        OltpProbe probe = OltpProbe.start(() -> {
            throw new IllegalStateException("down");
        }, Duration.ofHours(1), this::tick);
        probe.close();

        assertEquals(0, probe.impact().samples());
    }

    private long tick() {
        return nanos.getAndAdd(1_000_000);
    }
}
