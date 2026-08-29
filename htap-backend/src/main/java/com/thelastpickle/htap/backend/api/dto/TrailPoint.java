package com.thelastpickle.htap.backend.api.dto;

public record TrailPoint(
        String eventTime, double latitude, double longitude, double altitudeM, double speedMps) {}
