package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * What happened when many drones asked for the same zone at once.
 *
 * <p>This is the claim the whole schema exists to make, so it is measured rather than described:
 * {@code granted} must equal the zone's capacity however many asked, and the ledger must still add up
 * afterwards. A count read and written back outside consensus would oversubscribe here, and the
 * number would say so.
 *
 * @param winners who won, which differs between runs: that is what shows the asks genuinely contended
 *     rather than being serialised by the client
 * @param errors transactions that raised rather than being refused. A refusal is the expected outcome
 *     for a loser; an error is not, and the two must not be conflated
 * @param durationMs wall clock for the whole overlapping set, not per ask
 */
public record ClearanceContentionResult(
        String zoneId,
        long capacity,
        int askers,
        int granted,
        int refused,
        List<String> winners,
        List<String> errors,
        double durationMs,
        ClearanceZone zone) {}
