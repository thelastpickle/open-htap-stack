package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The percentiles of one window's point reads.
 *
 * <p>The index arithmetic is Python's, because the figures are quoted in the docs and a port that
 * takes a different reading moves a number nobody changed.
 */
class OltpImpactTest {

    /** Six readings put the p50 index at 2.5, which is the one case where half-to-even shows. */
    @Test
    void aHalfIndexTakesTheEvenReadingAsPythonDoes() {
        OltpImpact impact = OltpImpact.of(List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 0);

        assertEquals(3.0, impact.p50Ms());
        assertEquals(6.0, impact.p95Ms());
        assertEquals(6.0, impact.maxMs());
        assertEquals(6, impact.samples());
    }

    /** Unsorted on the way in, because the readings arrive in the order the probe took them. */
    @Test
    void theReadingsAreSortedBeforeTheyArePicked() {
        OltpImpact impact = OltpImpact.of(List.of(6.0, 1.0, 4.0, 2.0, 5.0, 3.0), 0);

        assertEquals(3.0, impact.p50Ms());
        assertEquals(6.0, impact.maxMs());
    }

    @Test
    void oneReadingIsEveryPercentileOfItself() {
        OltpImpact impact = OltpImpact.of(List.of(1.9), 0);

        assertEquals(1.9, impact.p50Ms());
        assertEquals(1.9, impact.p95Ms());
        assertEquals(1.9, impact.maxMs());
        assertEquals(1, impact.samples());
    }

    /** Rounded as Python rounds: half to even, over the double's own binary value. */
    @Test
    void everyFigureIsRoundedToATenthAsTheDashboardShowsIt() {
        OltpImpact impact = OltpImpact.of(List.of(1.24, 2.25, 3.55), 0);

        assertEquals(2.2, impact.p50Ms());
        assertEquals(3.5, impact.p95Ms());
        assertEquals(3.5, impact.maxMs());
    }

    /**
     * A window in which no read came back is the most interesting outcome of the five, so the
     * failures survive with nothing else to report beside them.
     */
    @Test
    void aWindowWithNoReadingsStillCarriesItsFailures() {
        OltpImpact impact = OltpImpact.of(List.of(), 7);

        assertEquals(0.0, impact.p50Ms());
        assertEquals(0, impact.samples());
        assertEquals(7, impact.failures());
        assertEquals(OltpImpact.none(7), impact);
    }

    @Test
    void failuresAreCountedBesideTheReadingsThatCameBack() {
        assertEquals(2, OltpImpact.of(List.of(1.0, 2.0), 2).failures());
    }
}
