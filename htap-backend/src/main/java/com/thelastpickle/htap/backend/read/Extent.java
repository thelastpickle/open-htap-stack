package com.thelastpickle.htap.backend.read;

import com.thelastpickle.htap.backend.support.Round;

/**
 * The minimum, maximum and mean of whatever readings were present, each rounded as the
 * dashboard shows it and each 0.0 when nothing was.
 *
 * <p>A null reading is skipped rather than counted as zero. That is what makes the mean
 * honest: an asset whose speed the sink has not written is an asset with no speed, and
 * averaging a zero in would pull the fleet's figure down by as many assets as are missing.
 *
 * <p>A plain running sum rather than {@link java.util.DoubleSummaryStatistics}, which carries
 * a compensation term. Kahan summation is the more accurate answer and therefore the wrong one
 * here: it disagrees with Python's plain {@code sum()} in the last digit, and the port is
 * compared with the Python figure for figure.
 */
final class Extent {

    private int count;
    private double sum;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;

    void add(Double value) {
        if (value == null) {
            return;
        }
        count++;
        sum += value;
        min = Math.min(min, value);
        max = Math.max(max, value);
    }

    double min() {
        return count == 0 ? 0.0 : Round.tenth(min);
    }

    double max() {
        return count == 0 ? 0.0 : Round.tenth(max);
    }

    double mean() {
        return count == 0 ? 0.0 : Round.tenth(sum / count);
    }
}
