package com.thelastpickle.htap.backend.api.dto;

/**
 * One defect, run beside the control that isolates it.
 *
 * <p>Run against the live service rather than quoted from a comment, so the page cannot claim a
 * defect the engine has stopped having.
 *
 * @param expected what a correct engine would answer the probe, in words, since the probe's own
 *     answer is the wrong one
 * @param control a nearby statement that is exact, which is what makes the probe's answer a defect
 *     rather than a limit
 */
public record SqlQuirk(
        String id,
        String title,
        String summary,
        String expected,
        SqlStatementResult probe,
        SqlStatementResult control) {}
