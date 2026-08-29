package com.thelastpickle.htap.backend.api.dto;

/**
 * Which path to rebuild this backend's client for, or {@code all} for every one of them.
 *
 * <p>The names are not listed here: they are the paths table's own keys, so the route asks it and the
 * two cannot come to disagree about what the paths are.
 */
public record ReconnectRequest(String target) {

    /** Every path, which is what the Health page's one button sends. */
    public static final String ALL = "all";
}
