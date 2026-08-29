package com.thelastpickle.htap.backend.api.dto;

/**
 * One search result: the indexed prose, its score, and where the asset is now.
 *
 * <p>The score comes from the index and the position from a point read of {@code
 * drone_latest_status}, so one hit is both paths answering about the same row.
 */
public record VectorHit(
        String entityId,
        String textPayload,
        Double similarity,
        String observerId,
        Double latitude,
        Double longitude,
        Double altitudeM,
        Boolean isFlying) {}
