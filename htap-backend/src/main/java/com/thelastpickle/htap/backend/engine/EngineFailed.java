package com.thelastpickle.htap.backend.engine;

/**
 * A statement an engine refused, or one whose read did not finish.
 *
 * <p>Separate from {@link EngineUnavailable} because the two are different answers rather than
 * different severities. A path that cannot be reached is reported unavailable and the console
 * answers 503; a path that answered "I cannot express that" is the finding the demo exists to
 * show, and the console answers 400 with the engine's own words. Cassandra declining a {@code
 * GROUP BY} on a non-key column is the second, and it is a correct result.
 */
public class EngineFailed extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * What the read had measured before it failed.
     *
     * <p>Worth carrying: a read that failed or was cancelled had still opened its files or taken
     * its snapshot, and how much it opened is part of why it failed.
     */
    private final transient ReadFigures figures;

    public EngineFailed(String message, Throwable cause) {
        this(message, cause, ReadFigures.NONE);
    }

    public EngineFailed(String message, Throwable cause, ReadFigures figures) {
        super(message, cause);
        this.figures = figures;
    }

    public EngineFailed(String message) {
        this(message, null, ReadFigures.NONE);
    }

    /** {@link ReadFigures#NONE} when the path measures nothing, never null. */
    public ReadFigures figures() {
        return figures == null ? ReadFigures.NONE : figures;
    }
}
