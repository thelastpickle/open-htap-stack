package com.thelastpickle.htap.backend.api;

/**
 * A refusal with the status and the wording the frontend already reads.
 *
 * <p>FastAPI's {@code HTTPException} renders {@code {"detail": …}}, and the dashboard's fetch
 * layer reads that field, so {@link ApiExceptionMapper} keeps the shape.
 */
public class ApiException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int status;

    public ApiException(int status, String detail) {
        super(detail);
        this.status = status;
    }

    public int status() {
        return status;
    }
}
