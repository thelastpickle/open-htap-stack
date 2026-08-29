package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * What a control did, in words.
 *
 * <p>Words rather than a status, because these controls do several things at once and partly succeed:
 * a reconnect over five paths may rebuild three, find one busy and fail on the last, and one status
 * cannot say that. The page prints the lines.
 *
 * @param ok false when nothing the control was asked to do was done
 */
public record OperationResult(boolean ok, List<String> actions) {

    /** Done, with the lines saying what. */
    public static OperationResult done(List<String> actions) {
        return new OperationResult(true, actions);
    }

    /** Nothing to do, with the one line saying so. */
    public static OperationResult nothing(String reason) {
        return new OperationResult(false, List.of(reason));
    }
}
