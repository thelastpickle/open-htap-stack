package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/** The hits, and how long the index and the point reads took together. */
public record VectorSearchResponse(List<VectorHit> results, double queryTimeMs) {}
