package com.thelastpickle.htap.backend.api.dto;

/**
 * @param time display label, {@code "14:30"}
 * @param timestamp bucket key, {@code "2026-08-16T14:30"}
 */
public record IngestionBucket(String time, String timestamp, long count) {}
