package com.thelastpickle.htap.backend.api.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The bounds pydantic declared, and the wording a refused request gets back. */
class DemoSettingsTest {

    @Test
    void whatThePageSendsIsInRange() {
        assertTrue(new DemoSettings(40, 800, 5.0, false).outOfRange().isEmpty());
    }

    /**
     * A field left out of the body arrives as zero, and that is the case the bounds exist for: a
     * fleet of zero and a rate of zero are both divisions by zero in the producer.
     */
    @Test
    void aFieldMissingFromTheBodyIsRefusedByName() {
        assertEquals("drones_enabled must be between 1 and 100000, got 0",
                new DemoSettings(0, 800, 5.0, false).outOfRange().orElseThrow());
        assertEquals("events_per_sec must be between 1 and 1000000, got 0",
                new DemoSettings(40, 0, 5.0, false).outOfRange().orElseThrow());
    }

    @Test
    void aFigureOverItsCeilingIsRefused() {
        assertEquals("drones_enabled must be between 1 and 100000, got 100001",
                new DemoSettings(100_001, 800, 5.0, false).outOfRange().orElseThrow());
        assertEquals("events_per_sec must be between 1 and 1000000, got 1000001",
                new DemoSettings(40, 1_000_001, 5.0, false).outOfRange().orElseThrow());
    }

    /** Both ceilings are legal values, so the refusal must not fire on them. */
    @Test
    void aFigureExactlyAtItsCeilingIsAccepted() {
        assertTrue(new DemoSettings(100_000, 1_000_000, 100.0, false).outOfRange().isEmpty());
    }

    /** Zero outliers is a clean fleet and legal, unlike a zero fleet or a zero rate. */
    @Test
    void anOutlierShareIsAPercentageAndZeroIsOneOfThem() {
        assertTrue(new DemoSettings(40, 800, 0.0, false).outOfRange().isEmpty());
        assertEquals("outlier_percent must be between 0 and 100, got -0.5",
                new DemoSettings(40, 800, -0.5, false).outOfRange().orElseThrow());
        assertEquals("outlier_percent must be between 0 and 100, got 100.5",
                new DemoSettings(40, 800, 100.5, false).outOfRange().orElseThrow());
    }

    @Test
    void theTwoReplacementsChangeOneFieldEach() {
        DemoSettings settings = new DemoSettings(40, 800, 5.0, false);

        assertEquals(new DemoSettings(2000, 800, 5.0, false), settings.withDronesEnabled(2000));
        assertEquals(new DemoSettings(40, 800, 5.0, true), settings.withPaused(true));
    }
}
