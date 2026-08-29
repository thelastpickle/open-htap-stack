package com.thelastpickle.htap.backend.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RoundTest {

    /**
     * An exact half goes to the even digit, where {@code Math.round} and
     * {@code String.format} go up. Each value here is representable to the bit, so the tie is
     * real rather than an artefact of the binary form.
     */
    @Test
    void anExactHalfGoesToTheEvenDigit() {
        assertEquals(0.2, Round.tenth(0.25));
        assertEquals(0.8, Round.tenth(0.75));
        assertEquals(0.12, Round.places(0.125, 2));
        assertEquals(0.38, Round.places(0.375, 2));
        assertEquals(2.0, Round.places(2.5, 0));
        assertEquals(4.0, Round.places(3.5, 0));
    }

    /**
     * The values that decide {@code new BigDecimal(double)} over
     * {@code BigDecimal.valueOf(double)}. None of these three is representable, so none is a
     * tie: 0.15 and 0.35 are stored a shade below the half and 0.45 a shade above, and Python
     * rounds the stored value. {@code valueOf} would round the shortest decimal that
     * round-trips instead, and would answer 0.2 and 0.4 where the Python printed 0.1 and 0.3.
     */
    @Test
    void theBinaryValueIsWhatIsRounded() {
        assertEquals(0.1, Round.tenth(0.15));
        assertEquals(0.3, Round.tenth(0.35));
        assertEquals(0.5, Round.tenth(0.45));
    }

    @Test
    void ordinaryFiguresRoundAsExpected() {
        assertEquals(12.3, Round.tenth(12.300000190734863));
        assertEquals(41.7, Round.tenth(41.66666666666667));
        assertEquals(0.0, Round.tenth(0.0));
        assertEquals(-2.5, Round.tenth(-2.45));
    }

    @Test
    void theHealthScoreTakesThreePlaces() {
        assertEquals(0.833, Round.thousandth(5.0 / 6.0));
        assertEquals(0.667, Round.thousandth(2.0 / 3.0));
        assertEquals(1.0, Round.thousandth(1.0));
        assertEquals(0.0, Round.thousandth(0.0));
    }

    /** BigDecimal has no NaN or infinity, so these have to be answered before it is used. */
    @Test
    void aNonFiniteValuePassesThrough() {
        assertTrue(Double.isNaN(Round.tenth(Double.NaN)));
        assertEquals(Double.POSITIVE_INFINITY, Round.tenth(Double.POSITIVE_INFINITY));
        assertEquals(Double.NEGATIVE_INFINITY, Round.thousandth(Double.NEGATIVE_INFINITY));
    }
}
