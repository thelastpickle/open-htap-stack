package com.thelastpickle.htap.backend.api.dto;

public record NearbyDroneResult(
        String entityId,
        String eventTime,
        double latitude,
        double longitude,
        double altitudeM,
        double distanceM) {}
