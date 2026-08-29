package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * @param points oldest first, so the frontend draws the line in flight order
 */
public record DroneTrail(String entityId, List<TrailPoint> points) {}
