package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * @param timestamp when this answer was assembled, not when the rows were written
 */
public record MapLiveResponse(
        List<DronePosition> drones, List<RestrictedZone> zones, String timestamp) {

    /** What the map shows while Cassandra is unreachable: the shape, with a live clock. */
    public static MapLiveResponse empty(String timestamp) {
        return new MapLiveResponse(List.of(), List.of(), timestamp);
    }
}
