package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * Every zone's ledger, and the holders whose two sides disagree.
 *
 * @param mismatched holders whose own {@code drone_clearance} row names a different zone, or none at
 *     all. One transaction writes both tables, so this must stay empty; it is the cross-partition
 *     half of the invariant {@link ClearanceZone#consistent} reports
 */
public record ClearanceState(List<ClearanceZone> zones, List<String> mismatched) {

    /** The zone by that identifier, or null when the seed holds no occupancy row for it. */
    public ClearanceZone zone(String zoneId) {
        return zones.stream().filter(zone -> zone.zoneId().equals(zoneId)).findFirst().orElse(null);
    }
}
