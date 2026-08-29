package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.engine.ReadFigures;
import java.util.List;

/**
 * What one path answered: the rows, the time, and the figures it measured.
 *
 * <p>A failure is a field rather than an exception, because the comparison runs five paths and one
 * refusing is a result the page shows beside the four that answered. {@code available} is the
 * narrower case: the path could not be reached at all, so there is nothing to time.
 *
 * @param available whether the path was reachable
 * @param sql the statement in this path's own spelling, which is what a viewer checks
 * @param error the engine's own words, or null when it answered
 * @param queryTimeMs the wall clock, kept for a failure too: a path refused in a millisecond
 *     because CQL cannot express the question and a path that gave up after a quarter of an hour
 *     are different findings, and without the clock they read alike
 */
public record PathResult(
        String path,
        boolean available,
        String sql,
        List<String> columns,
        List<List<Object>> rows,
        int rowCount,
        Double queryTimeMs,
        String error,
        ReadFigures figures) {

    /** A path that could not be reached, which is not the same as one that refused. */
    public static PathResult unavailable(String path, String detail) {
        return new PathResult(
                path, false, null, List.of(), List.of(), 0, null, detail, ReadFigures.NONE);
    }

    /** Whether the path answered rows rather than a refusal. */
    public boolean answered() {
        return available && error == null;
    }
}
