package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * @param drones nearest first
 */
public record NearbyResponse(List<NearbyDroneResult> drones) {

    public static NearbyResponse empty() {
        return new NearbyResponse(List.of());
    }
}
