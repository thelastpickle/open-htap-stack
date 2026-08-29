package com.thelastpickle.htap.backend.support;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Python's {@code round(x, n)}, which is not Java's.
 *
 * <p>Every figure the dashboard shows was rounded by it, so the port has to answer the same
 * or a key performance indicator moves for no reason a reader could account for.
 *
 * <p>Two differences from the obvious Java, and both are measured. Python rounds half to
 * even, where {@code Math.round} and {@code String.format} round half up: at one decimal
 * place 0.25 is 0.2 in Python and 0.3 in Java. And Python rounds the {@code double}'s exact
 * binary value, which is what {@code new BigDecimal(double)} takes and what
 * {@code BigDecimal.valueOf(double)} does not: {@code valueOf} goes through the shortest
 * decimal that round-trips, so it sees 0.15 rather than 0.1499999999999999944488848768742172
 * and answers 0.2 where Python answers 0.1. The same disagreement is at 0.35, and
 * {@code RoundTest} pins both.
 */
public final class Round {

    private Round() {}

    /** One decimal place, as every speed, altitude and distance the dashboard shows. */
    public static double tenth(double value) {
        return places(value, 1);
    }

    /** Three decimal places, as the platform health score. */
    public static double thousandth(double value) {
        return places(value, 3);
    }

    public static double places(double value, int places) {
        if (!Double.isFinite(value)) {
            // BigDecimal has no NaN or infinity, so it would raise here; Python's round
            // returns the value unchanged for both.
            return value;
        }
        return new BigDecimal(value).setScale(places, RoundingMode.HALF_EVEN).doubleValue();
    }
}
