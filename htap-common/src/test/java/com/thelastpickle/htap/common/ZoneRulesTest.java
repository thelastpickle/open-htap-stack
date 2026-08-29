package com.thelastpickle.htap.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * The figure itself, pinned.
 *
 * <p>Worth a test for the reason the constant is in this module: the sink's alerting and the
 * dashboard's what-if both read it, and a change made for one of them would silently move the other.
 * A test that names the number is where that change gets noticed.
 */
class ZoneRulesTest {

    @Test
    void theWarningDistanceIsFiveHundredMetres() {
        assertEquals(500.0, ZoneRules.WARNING_DISTANCE_M);
    }
}
