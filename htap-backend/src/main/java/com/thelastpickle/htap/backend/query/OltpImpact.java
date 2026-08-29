package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.support.Round;
import java.util.List;

/**
 * What a single-partition read cost while something else was running.
 *
 * <p>Sampled beside every path the comparison runs, so what an analytical query costs the
 * transactional path is shown rather than asserted. The bulk reader and the cqlite reader are the
 * two claiming to cost it nothing, and this is what tests the claim.
 *
 * @param failures point reads that did not come back at all during the window, which is the most
 *     interesting outcome of the five and so is counted rather than dropped
 */
public record OltpImpact(double p50Ms, double p95Ms, double maxMs, int samples, int failures) {

    /** A window in which no read came back, so there is nothing to report but the failures. */
    public static OltpImpact none(int failures) {
        return new OltpImpact(0.0, 0.0, 0.0, 0, failures);
    }

    /**
     * The percentiles of one window's readings, rounded as the dashboard shows them.
     *
     * <p>The index arithmetic is Python's, half to even through {@link Round}, because
     * {@code Math.round} would take a different sample: at six readings the p50 index is 2.5,
     * which rounds to 2 there and to 3 here, and the two answers are different readings.
     */
    public static OltpImpact of(List<Double> latenciesMs, int failures) {
        if (latenciesMs.isEmpty()) {
            return none(failures);
        }
        List<Double> sorted = latenciesMs.stream().sorted().toList();
        return new OltpImpact(
                at(sorted, 0.5),
                at(sorted, 0.95),
                Round.tenth(sorted.getLast()),
                sorted.size(),
                failures);
    }

    private static double at(List<Double> sorted, double fraction) {
        int index = Math.min(sorted.size() - 1, (int) Round.places(fraction * (sorted.size() - 1), 0));
        return Round.tenth(sorted.get(index));
    }
}
