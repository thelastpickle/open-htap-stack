package com.thelastpickle.htap.backend.api.dto;

/**
 * A hypothetical zone to score against the live fleet.
 *
 * <p>The constructor supplies the two labels a caller may omit, so a body carrying only
 * {@code polygon_wkt} answers as the Python's field defaults did.
 */
public record WhatIfZoneRequest(String polygonWkt, String zoneName, String severity) {

    public WhatIfZoneRequest {
        zoneName = zoneName == null ? "What-if zone" : zoneName;
        severity = severity == null ? "warning" : severity;
    }
}
