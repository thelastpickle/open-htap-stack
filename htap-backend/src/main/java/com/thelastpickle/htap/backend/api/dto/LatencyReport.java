package com.thelastpickle.htap.backend.api.dto;

/**
 * One representative query per tier, timed as this backend observed it.
 *
 * <p>Each figure includes the network hop, because that is what a caller of this stack pays. A
 * tier that cannot answer reports null, which the page shows as an em dash rather than as a zero;
 * a zero would read as a fast tier.
 */
public record LatencyReport(
        Double cassandraPointReadMs, Double prestoScanMs, Double vectorSearchMs, String timestamp) {}
