package com.thelastpickle.htap.backend.transaction;

import com.thelastpickle.htap.backend.support.Round;
import java.util.List;

/**
 * A p50 and a maximum over the samples a repeat loop kept.
 *
 * <p>Two decimal places, not the dashboard's one: these are millisecond figures for a single
 * statement, and a plain insert's median is under a millisecond, so a tenth would round the
 * reference the transaction is compared against to one significant figure.
 */
final class Latencies {

    private Latencies() {}

    /** Nothing measured reports nothing, rather than a zero that would read as instant. */
    static Double p50(List<Double> samples) {
        return samples.isEmpty() ? null : percentile(samples, 0.5);
    }

    static Double max(List<Double> samples) {
        if (samples.isEmpty()) {
            return null;
        }
        return Round.places(
                samples.stream().mapToDouble(Double::doubleValue).max().orElseThrow(), 2);
    }

    /**
     * The sample at a fraction of the ordered readings.
     *
     * <p>The index arithmetic is Python's, and rounded the way Python rounds: at six readings the
     * p50 index is 2.5, which is 2 there and would be 3 through {@code Math.round}, and those are
     * two different readings.
     */
    static double percentile(List<Double> samples, double fraction) {
        List<Double> ordered = samples.stream().sorted().toList();
        int index = Math.min(
                ordered.size() - 1, (int) Round.places(fraction * (ordered.size() - 1), 0));
        return Round.places(ordered.get(index), 2);
    }
}
