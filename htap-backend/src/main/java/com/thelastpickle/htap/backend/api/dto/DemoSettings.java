package com.thelastpickle.htap.backend.api.dto;

import java.util.Optional;

/**
 * The four figures the data producer runs on, with the bounds pydantic declared on them.
 *
 * <p>Sent whole rather than as a patch, which is what the Settings page does: a POST replaces every
 * field, {@code paused} included. A field left out of the body therefore arrives as zero rather
 * than as the value in force, and {@link #outOfRange} refuses it, so a partial body is answered
 * with the field it was missing rather than silently stopping the fleet.
 *
 * @param dronesEnabled how many assets report, held under {@code MAX_ENTITIES} by the route
 * @param outlierPercent the share of readings carrying an anomalous temperature
 */
public record DemoSettings(
        int dronesEnabled, int eventsPerSec, double outlierPercent, boolean paused) {

    private static final int MOST_DRONES = 100_000;
    private static final int MOST_EVENTS_PER_SEC = 1_000_000;

    /**
     * Which field is outside its range, in the wording the page shows, or empty when none is.
     *
     * <p>The bounds are the ones the request carries, not the ones the stack can serve; {@code
     * MAX_ENTITIES} is the second and it caps rather than refuses. These say what the producer's
     * own arithmetic needs: a rate or a fleet of zero divides by zero, and the ceilings are the
     * point past which a slider is reporting a mistake rather than an intention.
     */
    public Optional<String> outOfRange() {
        if (dronesEnabled < 1 || dronesEnabled > MOST_DRONES) {
            return Optional.of(range("drones_enabled", dronesEnabled, 1, MOST_DRONES));
        }
        if (eventsPerSec < 1 || eventsPerSec > MOST_EVENTS_PER_SEC) {
            return Optional.of(range("events_per_sec", eventsPerSec, 1, MOST_EVENTS_PER_SEC));
        }
        if (outlierPercent < 0.0 || outlierPercent > 100.0) {
            return Optional.of(
                    "outlier_percent must be between 0 and 100, got " + outlierPercent);
        }
        return Optional.empty();
    }

    /** The same settings with the fleet size replaced, which is the one field the route clamps. */
    public DemoSettings withDronesEnabled(int enabled) {
        return new DemoSettings(enabled, eventsPerSec, outlierPercent, paused);
    }

    public DemoSettings withPaused(boolean stopped) {
        return new DemoSettings(dronesEnabled, eventsPerSec, outlierPercent, stopped);
    }

    private static String range(String field, int given, int least, int most) {
        return field + " must be between " + least + " and " + most + ", got " + given;
    }
}
