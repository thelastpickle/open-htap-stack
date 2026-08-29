package com.thelastpickle.htap.backend.query;

/**
 * Which window the compare page should ask about, and what may be claimed of it.
 *
 * @param closed the clock: the window's minutes are over
 * @param settled the writer: the sink can no longer add a row to this window, which is the flag
 *     that licenses claiming the five paths agree exactly
 * @param settledDetail the evidence either way, so a false says which partition is short
 */
public record WindowChoice(
        int bucketMinutes,
        int shards,
        String current,
        String bucket,
        boolean closed,
        boolean settled,
        String settledDetail) {}
