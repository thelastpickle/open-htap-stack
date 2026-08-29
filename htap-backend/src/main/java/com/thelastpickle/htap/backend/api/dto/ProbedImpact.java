package com.thelastpickle.htap.backend.api.dto;

import com.thelastpickle.htap.backend.query.OltpImpact;

/**
 * What the point read cost while the transactions ran, and which asset it read.
 *
 * @param entityId the asset sampled, which the idle window does not carry: the baseline is the same
 *     read on the same asset, and naming it twice in one response would invite a reader to look for a
 *     difference between the two
 */
public record ProbedImpact(
        double p50Ms, double p95Ms, double maxMs, int samples, int failures, String entityId) {

    public static ProbedImpact of(OltpImpact impact, String entityId) {
        return new ProbedImpact(
                impact.p50Ms(),
                impact.p95Ms(),
                impact.maxMs(),
                impact.samples(),
                impact.failures(),
                entityId);
    }
}
