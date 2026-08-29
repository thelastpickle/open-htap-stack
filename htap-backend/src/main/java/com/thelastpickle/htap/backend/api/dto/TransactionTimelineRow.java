package com.thelastpickle.htap.backend.api.dto;

/** One row of the projection the transactions built, as the page lists it. */
public record TransactionTimelineRow(
        long seq, String eventId, String eventTime, String eventType, String payload) {}
