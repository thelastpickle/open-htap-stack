package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * @param hours the window actually read, after clamping, so the page can say what it got
 *     rather than what it asked for
 */
public record IngestionHistory(int hours, List<IngestionBucket> buckets) {}
