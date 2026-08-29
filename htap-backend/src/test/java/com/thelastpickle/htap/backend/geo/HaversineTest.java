package com.thelastpickle.htap.backend.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class HaversineTest {

    @Test
    void aPositionIsNoDistanceFromItself() {
        assertEquals(0.0, Haversine.metres(50.0, 10.0, 50.0, 10.0));
    }

    @Test
    void aDegreeOfLatitudeIsTheSameEverywhere() {
        assertEquals(111_194.9, Haversine.metres(0.0, 0.0, 1.0, 0.0), 0.1);
        assertEquals(111_194.9, Haversine.metres(50.0, 10.0, 51.0, 10.0), 0.1);
    }

    @Test
    void aDegreeOfLongitudeShortensTowardsThePole() {
        assertEquals(1_111.9, Haversine.metres(0.0, 0.0, 0.0, 0.01), 0.1);
        assertEquals(714.7, Haversine.metres(50.0, 10.0, 50.0, 10.01), 0.1);
    }

    @Test
    void theDistanceIsTheSameEitherWayRound() {
        assertEquals(
                Haversine.metres(51.5, -0.12, 48.85, 2.35),
                Haversine.metres(48.85, 2.35, 51.5, -0.12),
                1e-9);
        assertEquals(343_127.9, Haversine.metres(51.5, -0.12, 48.85, 2.35), 0.1);
    }

    /**
     * Antipodal points are where {@code sqrt(a)} can exceed 1 by a rounding error, and
     * {@code asin} of that is NaN. Half the circumference, and a number rather than a NaN, is
     * what says the clamp is in place.
     */
    @Test
    void antipodalPointsAnswerHalfTheCircumference() {
        assertEquals(20_015_086.8, Haversine.metres(50.0, 10.0, -50.0, -170.0), 0.1);
    }
}
