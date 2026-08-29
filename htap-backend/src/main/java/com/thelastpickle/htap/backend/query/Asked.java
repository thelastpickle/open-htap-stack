package com.thelastpickle.htap.backend.query;

import java.util.List;

/**
 * One comparison as a caller asked for it, validated.
 *
 * @param engines the paths to compare, in the order they will run
 * @param reuseSnapshot honoured by the one path that has a snapshot to reuse
 */
public record Asked(
        String sql, List<String> engines, RunMode mode, int limit, boolean reuseSnapshot) {}
