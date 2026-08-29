package com.thelastpickle.htap.backend.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;

/**
 * Whether the paths were timed one at a time or made to contend.
 *
 * <p>Both are legitimate and they answer different questions, so the response carries the mode: a
 * sequential figure is one path's own cost, and a parallel one is what four paths on one node cost
 * each other. Without the field the two are indistinguishable and not comparable.
 */
public enum RunMode {
    SEQUENTIAL,
    PARALLEL;

    /** The lower-case spelling the dashboard sends and reads. */
    @JsonValue
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    @JsonCreator
    public static RunMode of(String name) {
        if (name == null || name.isBlank()) {
            return SEQUENTIAL;
        }
        return valueOf(name.strip().toUpperCase(Locale.ROOT));
    }
}
