package com.thelastpickle.htap.backend.read;

/** One 30-minute bucket of {@code ingestion_counts}, by its key. */
public record BucketCount(String bucket, long count) {}
