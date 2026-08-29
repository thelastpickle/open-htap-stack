package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

public record ZonesResponse(List<RestrictedZone> zones) {

    public static ZonesResponse empty() {
        return new ZonesResponse(List.of());
    }
}
