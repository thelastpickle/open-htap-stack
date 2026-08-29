package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * @param totalCount alerts found in the window before any severity filter, so the page's
 *     per-severity counts add up to it
 */
public record AlertsResponse(List<AlertRecord> alerts, int totalCount) {

    public static AlertsResponse empty() {
        return new AlertsResponse(List.of(), 0);
    }
}
