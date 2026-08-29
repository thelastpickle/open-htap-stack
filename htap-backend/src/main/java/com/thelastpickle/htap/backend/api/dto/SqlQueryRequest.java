package com.thelastpickle.htap.backend.api.dto;

/**
 * One statement for one access path, as the console sends it.
 *
 * <p>Every field but the statement is optional, and an absent one is normalised here rather than
 * in the route: the Python's model carried the same three defaults, and a browser that posts only
 * {@code sql} is the console's own commonest request.
 *
 * @param limit clamped rather than refused, as the read routes already clamp their windows; the
 *     bound exists so that a console statement cannot ask for the whole history, and a caller
 *     asking for more than the maximum means the maximum
 * @param reuseSnapshot offered to every path and honoured by the one that has a snapshot to reuse
 */
public record SqlQueryRequest(String sql, int limit, String engine, boolean reuseSnapshot) {

    public static final int DEFAULT_LIMIT = 10;
    public static final int MAX_LIMIT = 1000;
    public static final String DEFAULT_ENGINE = "cassandra";

    public SqlQueryRequest {
        limit = limit < 1 ? DEFAULT_LIMIT : Math.min(limit, MAX_LIMIT);
        engine = engine == null || engine.isBlank() ? DEFAULT_ENGINE : engine;
    }
}
