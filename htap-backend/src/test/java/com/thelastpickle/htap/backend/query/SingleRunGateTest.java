package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * One comparison at a time, and what the refusal tells a viewer.
 *
 * <p>The refusal's own words are part of the contract: a browser that gave up on a long run leaves it
 * going here, so "already running" with no age would read as a stuck dashboard rather than as a run
 * somebody can stop.
 */
class SingleRunGateTest {

    private static final Asked ASKED =
            new Asked("SELECT 1", List.of("cassandra", "cqlite"), RunMode.SEQUENTIAL, 10, false);

    private final AtomicLong nanos = new AtomicLong();

    private final SingleRunGate gate = new SingleRunGate(nanos::get);

    @Test
    void nothingIsRunningUntilARunBegins() {
        assertTrue(gate.running().isEmpty());
        assertTrue(gate.inFlight().isEmpty());
    }

    @Test
    void theRunInFlightIsWhatTheHealthPageShows() {
        nanos.set(1_000_000_000L);
        Run run = gate.begin(ASKED, List.of());
        nanos.addAndGet(41_500_000_000L);

        ComparisonRun state = gate.running().orElseThrow();

        assertSame(run, gate.inFlight().orElseThrow());
        assertEquals(41.5, state.runningForS());
        assertEquals(RunMode.SEQUENTIAL, state.mode());
        assertEquals(List.of("cassandra", "cqlite"), state.engines());
        assertEquals("SELECT 1", state.sql());
        assertEquals(List.of(), state.done());
    }

    /** The paths that have answered, in the run's own order rather than the order they landed. */
    @Test
    void theDonePathsAreTheOnesThatHaveAnswered() {
        Run run = gate.begin(ASKED, List.of());
        run.answered("cqlite", PathResult.unavailable("cqlite", "no files"));

        assertEquals(List.of("cqlite"), gate.running().orElseThrow().done());

        run.answered("cassandra", PathResult.unavailable("cassandra", "not connected"));

        assertEquals(List.of("cassandra", "cqlite"), gate.running().orElseThrow().done());
    }

    /** Refused rather than queued: a caller waiting its turn would be timed while the run ahead ran. */
    @Test
    void aSecondRunIsRefusedWithTheAgeOfTheOneInFlight() {
        gate.begin(ASKED, List.of());
        nanos.addAndGet(41_500_000_000L);

        SingleRunGate.Busy busy =
                assertThrows(SingleRunGate.Busy.class, () -> gate.begin(ASKED, List.of()));

        assertTrue(busy.getMessage().startsWith("A comparison has been running for 41s."),
                busy.getMessage());
        assertTrue(busy.getMessage().endsWith("The Health page shows it, and can stop it."),
                busy.getMessage());
    }

    @Test
    void theGateIsFreeAgainOnceTheRunEnds() {
        Run first = gate.begin(ASKED, List.of());
        gate.end(first);

        assertTrue(gate.running().isEmpty());

        Run second = gate.begin(ASKED, List.of());

        assertSame(second, gate.inFlight().orElseThrow());
    }

    /**
     * A stream route releases the gate in a {@code finally} that the whole-body route may already
     * have reached, and a second release would let two comparisons run at once.
     */
    @Test
    void endingARunThatIsNoLongerTheCurrentOneReleasesNothing() {
        Run first = gate.begin(ASKED, List.of());
        gate.end(first);
        Run second = gate.begin(ASKED, List.of());

        gate.end(first);

        assertSame(second, gate.inFlight().orElseThrow());
        assertThrows(SingleRunGate.Busy.class, () -> gate.begin(ASKED, List.of()));
    }

    /**
     * A run taken on one thread is released on another, which is why this is a semaphore.
     *
     * <p>The stream route writes its body from whichever worker Quarkus gives it, and its {@code
     * finally} runs there. A {@link java.util.concurrent.locks.Lock} would raise {@code
     * IllegalMonitorStateException} at that release and leave the gate held for the life of the
     * process, with every later comparison refused as busy.
     */
    @Test
    @Timeout(10)
    void aRunTakenOnOneThreadCanBeReleasedOnAnother() throws InterruptedException {
        Run first = gate.begin(ASKED, List.of());
        Thread other = Thread.ofPlatform().start(() -> gate.end(first));
        other.join();

        assertTrue(gate.running().isEmpty());

        Run second = gate.begin(ASKED, List.of());

        assertSame(second, gate.inFlight().orElseThrow());
    }

    /** Worked out at the start, because by the time a cancel asks, the path is busy with it. */
    @Test
    void whatTheSparkPathsWillSubmitIsHeldWithTheRun() {
        Run run = gate.begin(ASKED, List.of("SELECT 1 LIMIT 10"));

        assertEquals(List.of("SELECT 1 LIMIT 10"), run.sparkStatements());
    }
}
