package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * What one path answered the console.
 *
 * @param sql the statement as actually issued, after this path's own rewriting, which is what a
 *     viewer checks the answer against
 */
public record SqlQueryResult(
        List<String> columns,
        List<List<Object>> rows,
        int rowCount,
        double queryTimeMs,
        String sql) {}
