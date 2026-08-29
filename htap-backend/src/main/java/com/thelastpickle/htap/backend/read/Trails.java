package com.thelastpickle.htap.backend.read;

import java.util.ArrayList;
import java.util.List;

/** Thinning a dense read down to a path a map can draw. */
public final class Trails {

    private Trails() {}

    /**
     * Every {@code size / points}-th row, at most {@code points} of them.
     *
     * <p>The scan behind a flight path is far denser than the path needs: at demo rates each
     * asset emits tens of readings a second, so drawing every row is a smudge rather than a
     * track. Keeping every nth row of a fixed window gives a path spanning real time for one
     * bounded read.
     *
     * <p>Python's {@code rows[::stride][:points]}, and the stride is what makes it a port
     * rather than a rewrite: an integer division, so a read of 2,000 rows asked for 60 points
     * strides by 33 and returns 61 rows before the truncation, not 60.
     */
    public static <T> List<T> thin(List<T> rows, int points) {
        int stride = Math.max(1, rows.size() / points);
        List<T> kept = new ArrayList<>(Math.min(points, rows.size()));
        for (int i = 0; i < rows.size() && kept.size() < points; i += stride) {
            kept.add(rows.get(i));
        }
        return kept;
    }
}
