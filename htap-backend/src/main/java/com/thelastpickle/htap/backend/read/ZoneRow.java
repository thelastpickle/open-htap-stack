package com.thelastpickle.htap.backend.read;

/**
 * One row of {@code restricted_zones}, the demo's reference data.
 *
 * <p>{@code enabled} defaults to true for a null, as the Python's {@code z.get("enabled",
 * True)} did: a zone the sink seeded before the column existed is in force rather than
 * ignored.
 */
public record ZoneRow(
        String zoneId, String zoneName, String polygonWkt, String severity, boolean enabled) {}
