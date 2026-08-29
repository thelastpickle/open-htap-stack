package com.thelastpickle.htap.backend.engine;

/** An access path that was asked for a connection it could not make. */
public class EngineUnavailable extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public EngineUnavailable(String message, Throwable cause) {
        super(message, cause);
    }

    public EngineUnavailable(String message) {
        super(message);
    }
}
