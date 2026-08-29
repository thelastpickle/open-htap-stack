package com.thelastpickle.htap.backend.query;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.function.LongSupplier;

/**
 * One comparison at a time, and what the one in flight is.
 *
 * <p>Two overlapping runs would each be timed while the other ran, and both sets of figures would be
 * wrong without saying so. A second caller is refused rather than queued, because a caller waiting
 * its turn would be timed while the run ahead of it finished.
 *
 * <p>A semaphore rather than a lock: the run is released in a {@code finally} that a stream route may
 * reach on a thread other than the one that took it, and a {@link java.util.concurrent.locks.Lock}
 * insists on being released by its owner.
 */
@ApplicationScoped
public class SingleRunGate {

    private final Semaphore turn = new Semaphore(1);
    private final LongSupplier nanoClock;
    private volatile Run current;

    SingleRunGate() {
        this(System::nanoTime);
    }

    SingleRunGate(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
    }

    /**
     * Take the gate and record what is running, or refuse.
     *
     * <p>Every caller must pair this with {@link #end} in a {@code finally}.
     */
    public Run begin(Asked asked, List<String> sparkStatements) {
        if (!turn.tryAcquire()) {
            throw new Busy(refusal());
        }
        Run run = new Run(asked, sparkStatements, nanoClock);
        current = run;
        return run;
    }

    /** Release the gate, and forget the run. */
    public void end(Run run) {
        if (current != run) {
            // Not this caller's run to end, which is the double-release a stream route's
            // `finally` would otherwise cause after the whole-body route had already ended it.
            return;
        }
        current = null;
        turn.release();
    }

    /** The comparison in flight, or empty. */
    public Optional<ComparisonRun> running() {
        return inFlight().map(Run::state);
    }

    /** The run itself, which is what a cancel needs. */
    public Optional<Run> inFlight() {
        return Optional.ofNullable(current);
    }

    private String refusal() {
        int age = (int) inFlight().map(Run::runningForS).orElse(0.0).doubleValue();
        return "A comparison has been running for " + age + "s.  They run one at a time, "
                + "because two at once would each be timed while the other ran; a run whose "
                + "browser gave up carries on here until it finishes.  The Health page shows "
                + "it, and can stop it.";
    }

    /** A second comparison arriving while one runs, which the route answers 409. */
    public static class Busy extends RuntimeException {

        private static final long serialVersionUID = 1L;

        Busy(String detail) {
            super(detail);
        }
    }
}
