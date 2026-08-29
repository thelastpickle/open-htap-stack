package com.thelastpickle.htap.backend.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.thelastpickle.htap.backend.read.Kpis;

/**
 * @param kpis omitted when the re-probe failed, which is how the Python's error branch answered
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ResyncResult(boolean success, String message, Kpis kpis) {

    public static ResyncResult failed(String message) {
        return new ResyncResult(false, message, null);
    }
}
