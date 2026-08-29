package com.thelastpickle.htap.backend.api.dto;

/** An alert as the Overview page's latest-alerts list shows it. */
public record AlertSummary(
        String alertId,
        String alertTime,
        String entityId,
        String alertType,
        String severity,
        String message,
        double riskScore) {}
