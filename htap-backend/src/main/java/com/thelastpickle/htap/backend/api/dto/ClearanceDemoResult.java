package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * The scripted clearance sequence, and the ledger it left behind.
 *
 * <p>A grant and a release are timed separately, because they are two different transactions: a grant
 * reads two partitions and writes three, a release reads one and writes three.
 */
public record ClearanceDemoResult(
        String zoneId,
        List<String> entityIds,
        List<TransactionStep> steps,
        ClearanceState state,
        int repeats,
        Double grantP50Ms,
        Double grantMaxMs,
        Double releaseP50Ms,
        Double releaseMaxMs) {}
