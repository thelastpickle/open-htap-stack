package com.thelastpickle.htap.backend.api.dto;

import com.thelastpickle.htap.backend.query.RunMode;
import java.util.List;

/**
 * One comparison as the compare page asks for it.
 *
 * @param engines which access paths to compare; null means all of them, and naming a subset is how a
 *     viewer asks a narrower question
 * @param limit clamped rather than refused, as the console's own request is
 * @param reuseSnapshot honoured by the one path that has a snapshot to reuse
 */
public record BenchmarkRequest(
        String sql, int limit, List<String> engines, RunMode mode, boolean reuseSnapshot) {

    public BenchmarkRequest {
        limit = limit < 1
                ? SqlQueryRequest.DEFAULT_LIMIT
                : Math.min(limit, SqlQueryRequest.MAX_LIMIT);
        mode = mode == null ? RunMode.SEQUENTIAL : mode;
        // A named-but-empty list is kept rather than read as "all of them": a caller who sent
        // `engines: []` asked for a comparison of nothing, and is told so.
        engines = engines == null ? null : List.copyOf(engines);
    }

    /** A request that arrived with no body at all, which the route refuses as an empty statement. */
    public static BenchmarkRequest empty() {
        return new BenchmarkRequest(null, 0, null, null, false);
    }
}
