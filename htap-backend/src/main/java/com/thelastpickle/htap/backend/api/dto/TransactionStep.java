package com.thelastpickle.htap.backend.api.dto;

import com.thelastpickle.htap.backend.support.Round;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One step of a transaction demonstration, and what the server made of it.
 *
 * @param action what the step was asked to do, for instance "apply seq=1" or "replay seq=0"
 * @param cql the statement that ran, so the page shows the reader the real CQL rather than a
 *     description of it
 * @param applied whether the transaction's {@code IF} fired. Derived from the projection, not read
 *     from an {@code [applied]} column: an Accord transaction has none
 * @param reason why it did not fire, in the words of the guard that stopped it, or empty when it did
 *     fire. This is the field the demo exists to show
 * @param projection the guard values the transaction projected, verbatim
 * @param durationMs latency of the one statement, timed at the backend
 * @param timelineRows rows in {@code session_timeline} for this session after the step, so a refused
 *     step is visibly a step that changed nothing
 * @param state the same idea for a demonstration whose "changed nothing" is not a row count: the
 *     clearance half puts the zone's slots and holders here
 */
public record TransactionStep(
        String action,
        String cql,
        boolean applied,
        String reason,
        Map<String, String> projection,
        double durationMs,
        int timelineRows,
        ClearanceZone state,
        String error) {

    /** A session step that ran, applied or refused according to what its guards projected. */
    public static TransactionStep session(
            String action,
            String cql,
            String reason,
            Map<String, Object> projection,
            double durationMs,
            int timelineRows) {
        return new TransactionStep(
                action, cql, reason.isEmpty(), reason, shown(projection),
                Round.places(durationMs, 2), timelineRows, null, null);
    }

    /** A clearance step, whose state is the zone as it stood after the step rather than a count. */
    public static TransactionStep clearance(
            String action,
            String cql,
            String reason,
            Map<String, Object> projection,
            double durationMs,
            ClearanceZone state) {
        return new TransactionStep(
                action, cql, reason.isEmpty(), reason, shown(projection),
                Round.places(durationMs, 2), 0, state, null);
    }

    /**
     * The one write the session demo makes outside a transaction, which Accord still carries.
     *
     * <p>A plain {@code INSERT}, and it goes through consensus all the same, because the table is
     * {@code transactional_mode='full'}. Timed so the reader can see what that costs beside the
     * plain-insert reference, which writes the same shape into a table that is not transactional.
     */
    public static TransactionStep opened(String cql, double durationMs) {
        return new TransactionStep(
                "open the session", cql, true, "", Map.of(), Round.places(durationMs, 2), 0, null,
                null);
    }

    /**
     * A step whose statement raised.
     *
     * <p>A third outcome and not a refusal: a refused step is the demo working, and reporting the two
     * alike would misstate its result.
     */
    public static TransactionStep failed(
            String action, String cql, double durationMs, String error, int timelineRows) {
        return new TransactionStep(
                action, cql, false, "", Map.of(), Round.places(durationMs, 2), timelineRows, null,
                error);
    }

    /**
     * The projection as the page shows it: every value a string, and a null left null.
     *
     * <p>Stringified here rather than where the reason is derived, because {@code occ.remaining} is
     * compared as a number and the string "0" is not falsy in any language this travels through.
     */
    static Map<String, String> shown(Map<String, Object> projection) {
        Map<String, String> shown = new LinkedHashMap<>(projection.size());
        projection.forEach((name, value) -> shown.put(name, value == null ? null : value.toString()));
        return shown;
    }
}
