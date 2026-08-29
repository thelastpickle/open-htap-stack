package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * One restricted zone's clearance ledger, read from both sides.
 *
 * @param remaining slots left. Held as a count-down rather than a count of grants because that is
 *     what Accord can decrement in one statement: {@code SET remaining -= 1} needs no capacity to
 *     compare against, where a count-up would need the transaction to compare two {@code LET}
 *     references, which Accord refuses
 * @param holders drones cleared into the zone, from the zone's own clearance partition
 * @param consistent {@code capacity == remaining + holders.size()}: the semaphore's whole invariant,
 *     and the thing the demo exists to keep true. Reported rather than asserted, because a broken one
 *     is the interesting result and hiding it would defeat the point
 */
public record ClearanceZone(
        String zoneId,
        String zoneName,
        String severity,
        long capacity,
        long remaining,
        List<String> holders,
        boolean consistent) {

    public static ClearanceZone of(
            String zoneId,
            String zoneName,
            String severity,
            long capacity,
            long remaining,
            List<String> holders) {
        return new ClearanceZone(
                zoneId,
                zoneName,
                severity,
                capacity,
                remaining,
                holders,
                capacity == remaining + holders.size());
    }
}
