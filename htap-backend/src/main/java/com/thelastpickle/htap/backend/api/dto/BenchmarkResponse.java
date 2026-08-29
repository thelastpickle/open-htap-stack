package com.thelastpickle.htap.backend.api.dto;

import com.thelastpickle.htap.backend.query.OltpImpact;
import com.thelastpickle.htap.backend.query.PathResult;
import com.thelastpickle.htap.backend.query.Run;
import com.thelastpickle.htap.backend.query.RunMode;
import java.util.Map;

/**
 * What the five paths answered, one field each.
 *
 * <p>A path the request did not ask for is null rather than empty, so a partial comparison cannot be
 * mistaken for five paths of which some failed.
 *
 * @param sparkBulk the Analytics bulk reader: the same rows, read from a snapshot's SSTable files
 * @param cqlite the cqlite reader: the same rows again, read from the live files in place, in this
 *     process, with no snapshot and no second service
 * @param oltpBaseline the same point read measured before any path ran, so each path's figure has
 *     something to be read against
 * @param oltpCombined set only for a parallel run: one sample over the whole window, because while
 *     the paths overlap the cost cannot be attributed to any one of them
 * @param cancelled true when the run was stopped from the Health page; the paths that had not started
 *     are absent rather than reported as failures, since they never ran
 */
public record BenchmarkResponse(
        EngineResult cassandra,
        EngineResult presto,
        EngineResult spark,
        EngineResult sparkBulk,
        EngineResult cqlite,
        RunMode mode,
        OltpImpact oltpBaseline,
        OltpImpact oltpCombined,
        boolean cancelled) {

    public static BenchmarkResponse of(Run run, OltpImpact baseline, OltpImpact combined) {
        Map<String, PathResult> results = run.results();
        Map<String, OltpImpact> impacts = run.impacts();
        return new BenchmarkResponse(
                answered(results, impacts, "cassandra"),
                answered(results, impacts, "presto"),
                answered(results, impacts, "spark"),
                answered(results, impacts, "spark_bulk"),
                answered(results, impacts, "cqlite"),
                run.asked().mode(),
                baseline,
                combined,
                run.cancelled());
    }

    private static EngineResult answered(
            Map<String, PathResult> results, Map<String, OltpImpact> impacts, String engine) {
        PathResult result = results.get(engine);
        return result == null ? null : EngineResult.of(result, impacts.get(engine));
    }
}
