package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * A batch of statements and what each answered.
 *
 * @param errorCount how many statements were refused, which is not the batch's verdict: a reset's
 *     two {@code DROP TYPE} statements are refused on a stack whose ENUM types are absent, and that
 *     is the state a reset wants anyway. Judge the {@code CREATE} and {@code INSERT} statements
 */
public record SqlConsoleResult(
        String engine, List<SqlStatementResult> statements, double durationMs, int errorCount) {

    public static final String ENGINE = "cassandra-sql";
}
