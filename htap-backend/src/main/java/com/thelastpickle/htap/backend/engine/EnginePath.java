package com.thelastpickle.htap.backend.engine;

/**
 * One of the five ways the demo reaches the same rows.
 *
 * <p>Every path connects lazily and reports whether it is connected, because an endpoint's
 * answer when a path is down is part of what the demo shows: the dashboard says which path
 * answered and which declined, rather than failing the page.
 */
public interface EnginePath {

    /** The name the dashboard and the logs use for this path. */
    String name();

    /** Connect if not already connected, swallowing a failure. */
    default void connect() {
        connect(false);
    }

    /**
     * Connect, ignoring both the connected state and the retry throttle when {@code force}.
     *
     * @throws EngineUnavailable when {@code force} and the attempt failed; never otherwise
     */
    void connect(boolean force);

    boolean connected();
}
