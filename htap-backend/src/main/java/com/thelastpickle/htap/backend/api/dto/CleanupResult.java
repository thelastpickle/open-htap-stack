package com.thelastpickle.htap.backend.api.dto;

/**
 * Whether the fleet table was cleared, and what to read if it was not.
 *
 * <p>A body with {@code success: false} rather than a status, which is what the Python answered and
 * what the page reads: a truncate refused because Cassandra is down is something the operator can
 * act on, and an error banner with no words is not.
 */
public record CleanupResult(boolean success, String message) {

    public static CleanupResult failed(String message) {
        return new CleanupResult(false, message);
    }
}
