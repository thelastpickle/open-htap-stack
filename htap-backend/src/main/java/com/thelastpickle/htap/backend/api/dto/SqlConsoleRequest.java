package com.thelastpickle.htap.backend.api.dto;

import java.util.Optional;

/**
 * One SQL string for cassandra-sql, which may hold a whole transaction.
 *
 * <p>Only the statement, and no parameters: a bound parameter of an integer type returns no rows
 * here and raises nothing, so offering one would be offering a silent wrong answer.
 */
public record SqlConsoleRequest(String sql) {

    /** Why this body cannot be run, if it cannot. */
    public Optional<String> outOfRange() {
        return sql == null || sql.isBlank() ? Optional.of("sql must not be empty") : Optional.empty();
    }
}
