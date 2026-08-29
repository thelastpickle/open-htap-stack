package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * One statement's outcome, whether it answered or was refused.
 *
 * <p>A refusal is a result and not a server error, because showing what cassandra-sql declines is
 * half of what the console is for.
 *
 * @param rowCount the rows the statement produced, which may exceed the rows carried: a large
 *     result is truncated for the page and this says by how much
 * @param error the service's own words, or null
 */
public record SqlStatementResult(
        String sql,
        List<String> columns,
        List<List<String>> rows,
        int rowCount,
        double durationMs,
        String error) {

    public static SqlStatementResult failed(String sql, String error) {
        return new SqlStatementResult(sql, List.of(), List.of(), 0, 0.0, error);
    }
}
