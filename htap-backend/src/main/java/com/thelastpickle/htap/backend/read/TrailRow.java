package com.thelastpickle.htap.backend.read;

import java.time.Instant;

/** One reading of {@code drone_events_by_entity}, on the flight path of one asset. */
public record TrailRow(
        Instant eventTime,
        Double latitude,
        Double longitude,
        Double altitudeM,
        Double speedMps,
        Double headingDeg) {

    public boolean located() {
        return latitude != null && longitude != null;
    }
}
