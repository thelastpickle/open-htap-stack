package com.thelastpickle.htap.backend.api.dto;

/**
 * What the live embedder is doing, in the terms the Explore page shows.
 *
 * <p>Each figure names what it measures. {@code pending} is what the last pass had to defer and
 * {@code behindS} is how long ago that pass ran, so the two together say whether the index is
 * following the writes or falling behind them.
 *
 * <p>{@code embedded}, {@code failed} and {@code passes} are totals since this backend started and
 * not since the loop was last enabled: the loop keeps what it has embedded across a disable, so
 * resetting them would misreport work already done.
 */
public record LiveEmbeddingStatus(
        boolean enabled,
        String embedder,
        double intervalS,
        int embedded,
        int failed,
        int passes,
        int lastEmbedded,
        double lastPassMs,
        int pending,
        Double behindS,
        int tracked,
        String error) {}
