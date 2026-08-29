package com.thelastpickle.htap.producer;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

/**
 * The demo controls, as the dashboard last reported them.
 *
 * <p>The Settings page holds the values in the backend's memory and this copies them in, so a
 * backend that is absent, slow or broken leaves the producer on the values it already had. That
 * direction is the point: data generation never depends on the dashboard being up, and stopping
 * either dashboard service does not touch ingest.
 */
final class LiveSettings {

    /** What one caller reads at once, so a batch cannot be sized from two different rates. */
    record Snapshot(int eventsPerSec, int nEntities, double outlierPercent, boolean paused) {}

    private final Object lock = new Object();

    private int eventsPerSec;
    private int nEntities;
    private double outlierPercent;
    private boolean paused;

    LiveSettings(int eventsPerSec, int nEntities, double outlierPercent) {
        this.eventsPerSec = eventsPerSec;
        this.nEntities = nEntities;
        this.outlierPercent = outlierPercent;
    }

    Snapshot snapshot() {
        synchronized (lock) {
            return new Snapshot(eventsPerSec, nEntities, outlierPercent, paused);
        }
    }

    /**
     * Adopts what the backend reported, keeping the current value for anything it did not send.
     *
     * <p>The fleet size arrives as {@code drones_enabled}, which is the dashboard's own name for
     * it; the producer's is {@code n_entities}, and the two are the same number. Both the rate and
     * the size are held at one or more, because a batch of zero would idle the loop without
     * saying so.
     *
     * @return the snapshot in force afterwards, and whether this call changed anything
     */
    Applied apply(Reported reported) {
        Snapshot before;
        Snapshot after;
        synchronized (lock) {
            before = new Snapshot(eventsPerSec, nEntities, outlierPercent, paused);
            eventsPerSec = Math.max(1, reported.eventsPerSec().orElse(eventsPerSec));
            nEntities = Math.max(1, reported.dronesEnabled().orElse(nEntities));
            outlierPercent = reported.outlierPercent().orElse(outlierPercent);
            paused = reported.paused().orElse(paused);
            after = new Snapshot(eventsPerSec, nEntities, outlierPercent, paused);
        }
        return new Applied(after, !after.equals(before));
    }

    record Applied(Snapshot snapshot, boolean changed) {}

    /**
     * What the settings endpoint said, with each field absent where it said nothing.
     *
     * <p>Absent rather than defaulted, so that a backend answering a partial body cannot reset a
     * value it never mentioned. The Python read this out of a dict with the current value as the
     * fallback, which is the same rule written where it could be forgotten.
     */
    record Reported(
            OptionalInt eventsPerSec,
            OptionalInt dronesEnabled,
            OptionalDouble outlierPercent,
            Optional<Boolean> paused) {

        static Reported nothing() {
            return new Reported(
                    OptionalInt.empty(), OptionalInt.empty(), OptionalDouble.empty(), Optional.empty());
        }
    }
}
