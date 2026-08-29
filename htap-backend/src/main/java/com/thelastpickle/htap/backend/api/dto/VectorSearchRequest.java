package com.thelastpickle.htap.backend.api.dto;

import java.util.Optional;

/**
 * What to search the embedding index for, and how many rows to return.
 *
 * <p>{@code limit} is boxed so that a body omitting it takes the default, where a body asking for
 * zero is refused.
 */
public record VectorSearchRequest(String query, Integer limit) {

    public static final int DEFAULT_HITS = 5;
    public static final int MOST_HITS = 50;

    /** The row count asked for, or the default when the body left it out. */
    public int hits() {
        return limit == null ? DEFAULT_HITS : limit;
    }

    /** The reason to refuse the body, if there is one. */
    public Optional<String> outOfRange() {
        if (query == null) {
            return Optional.of("Expected a body carrying the query to search for");
        }
        if (limit != null && (limit < 1 || limit > MOST_HITS)) {
            return Optional.of("limit must be between 1 and " + MOST_HITS + ", got " + limit);
        }
        return Optional.empty();
    }
}
