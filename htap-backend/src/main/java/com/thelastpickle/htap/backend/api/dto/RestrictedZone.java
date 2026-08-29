package com.thelastpickle.htap.backend.api.dto;

public record RestrictedZone(
        String zoneId, String zoneName, String polygonWkt, String severity, boolean enabled) {}
