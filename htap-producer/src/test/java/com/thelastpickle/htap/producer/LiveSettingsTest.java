package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

/** The controls the send loop reads, and what an incomplete report leaves alone. */
class LiveSettingsTest {

    private final LiveSettings live = new LiveSettings(2000, 100, 5.0);

    @Test
    void theStartingValuesAreWhatTheProcessWasGiven() {
        LiveSettings.Snapshot now = live.snapshot();

        assertEquals(2000, now.eventsPerSec());
        assertEquals(100, now.nEntities());
        assertEquals(5.0, now.outlierPercent());
        assertFalse(now.paused(), "a fleet starts running");
    }

    @Test
    void aReportOfNothingChangesNothing() {
        LiveSettings.Applied applied = live.apply(LiveSettings.Reported.nothing());

        assertFalse(applied.changed());
        assertEquals(2000, applied.snapshot().eventsPerSec());
    }

    /** Zero is not a rate and not a fleet: a batch of nothing would idle without saying so. */
    @Test
    void theRateAndTheFleetAreHeldAtOneOrMore() {
        LiveSettings.Applied applied = live.apply(reported(0, 0));

        assertEquals(1, applied.snapshot().eventsPerSec());
        assertEquals(1, applied.snapshot().nEntities());
        assertTrue(applied.changed());
    }

    /** The snapshot is one read: a batch cannot be sized from two different rates. */
    @Test
    void aSnapshotIsTakenWhole() {
        live.apply(reported(400, 250));
        LiveSettings.Snapshot now = live.snapshot();
        live.apply(reported(800, 900));

        assertEquals(400, now.eventsPerSec(), "the snapshot changed under its reader");
        assertEquals(250, now.nEntities());
        assertEquals(800, live.snapshot().eventsPerSec());
    }

    private static LiveSettings.Reported reported(int eventsPerSec, int dronesEnabled) {
        return new LiveSettings.Reported(
                OptionalInt.of(eventsPerSec),
                OptionalInt.of(dronesEnabled),
                OptionalDouble.empty(),
                Optional.empty());
    }
}
