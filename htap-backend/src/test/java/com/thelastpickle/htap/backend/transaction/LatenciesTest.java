package com.thelastpickle.htap.backend.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

/** The two figures a repeat loop reports, and the two ways they are easy to get wrong. */
class LatenciesTest {

    /** Nothing measured must not report a zero, which would read as instant rather than as absent. */
    @Test
    void nothingMeasuredReportsNothing() {
        assertNull(Latencies.p50(List.of()));
        assertNull(Latencies.max(List.of()));
    }

    /**
     * The index is Python's, and the rounding is Python's too.
     *
     * <p>At six readings the p50 index is 2.5, which Python's {@code round} takes to 2 and {@code
     * Math.round} would take to 3. Those are two different readings, so the port has to keep the
     * banker's rounding rather than the arithmetic one.
     */
    @Test
    void theP50TakesTheLowerOfTwoMiddleReadings() {
        List<Double> six = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);

        assertEquals(3.0, Latencies.p50(six));
    }

    /** An odd count has one middle reading, and it is the one taken. */
    @Test
    void theP50OfFiveReadingsIsTheMiddleOne() {
        assertEquals(3.0, Latencies.p50(List.of(5.0, 1.0, 3.0, 2.0, 4.0)));
    }

    /** Sorted first, so the order the loop measured in does not decide the answer. */
    @Test
    void theReadingsAreOrderedBeforeTheIndexIsTaken() {
        assertEquals(Latencies.p50(List.of(9.0, 1.0, 4.0)), Latencies.p50(List.of(1.0, 4.0, 9.0)));
    }

    /** Two decimal places, because a plain insert's median is under a millisecond. */
    @Test
    void bothFiguresKeepTwoDecimalPlaces() {
        List<Double> samples = List.of(0.4361, 0.4362, 1.23456);

        assertEquals(0.44, Latencies.p50(samples));
        assertEquals(1.23, Latencies.max(samples));
    }

    @Test
    void oneReadingIsBothTheMedianAndTheMaximum() {
        assertEquals(2.5, Latencies.p50(List.of(2.5)));
        assertEquals(2.5, Latencies.max(List.of(2.5)));
    }
}
