package com.thelastpickle.htap.producer;

/**
 * One asset's state at one instant, which is what one event carries.
 *
 * @param altitudeM held between {@link FleetConfig#ALT_MIN_M} and {@link FleetConfig#ALT_MAX_M}
 * @param tempExternalC the modelled outside air temperature at this latitude and altitude
 * @param tempInternalC the asset's own, which follows the outside temperature with operational
 *     heat and, during an anomaly, a spike; it is what the demo's alerting reads
 * @param text a snippet of the corpus, refreshed on its own cadence rather than per event
 */
record Telemetry(
        int index,
        double lat,
        double lon,
        double altitudeM,
        double tempExternalC,
        double tempInternalC,
        String text) {}
