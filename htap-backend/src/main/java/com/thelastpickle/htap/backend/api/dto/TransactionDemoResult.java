package com.thelastpickle.htap.backend.api.dto;

import java.util.List;
import java.util.Map;

/**
 * The whole scripted session sequence, and the projection it left behind.
 *
 * @param referenceMs the two references, on the same row shape in a non-transactional twin table: a
 *     plain {@code INSERT} and an {@code IF NOT EXISTS} lightweight transaction. A transaction
 *     latency means nothing without them
 * @param oltpProbe what the point read was doing while the transactions ran, and {@code oltpBaseline}
 *     the same over an idle window just before, so the claim that Accord stayed off the request path
 *     is a difference the reader can see rather than one asserted
 */
public record TransactionDemoResult(
        String userId,
        String sessionId,
        List<TransactionStep> steps,
        List<TransactionTimelineRow> timeline,
        Map<String, Double> referenceMs,
        int repeats,
        Double appliedP50Ms,
        Double appliedMaxMs,
        ProbedImpact oltpProbe,
        ProbedImpact oltpBaseline) {}
