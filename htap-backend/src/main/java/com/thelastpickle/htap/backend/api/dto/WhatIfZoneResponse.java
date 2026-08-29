package com.thelastpickle.htap.backend.api.dto;

import java.util.List;

/**
 * @param affectedDroneIds those inside the polygon, then those within the warning distance
 */
public record WhatIfZoneResponse(
        RestrictedZone zone,
        int dronesInside,
        int dronesNearby,
        List<String> affectedDroneIds) {

    /** The zone alone, when the fleet cannot be read. */
    public static WhatIfZoneResponse unscored(RestrictedZone zone) {
        return new WhatIfZoneResponse(zone, 0, 0, List.of());
    }
}
