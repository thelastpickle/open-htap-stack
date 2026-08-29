package com.thelastpickle.htap.producer;

/**
 * The shape of the fleet's motion, its altitude, its temperatures and its anomalies.
 *
 * <p>Every figure is the Python's, and the ones that matter to the demo rather than to the
 * plausibility of the numbers are the area and the anomaly durations: the area is greater Oslo
 * because that is where the demo's restricted zones are, and the durations decide the arrival
 * rate that holds a requested share of the fleet running hot.
 *
 * @param nEntities the number of assets this process holds state for, which is the ceiling the
 *     Settings page may raise the live fleet to rather than the live fleet size
 * @param latSpreadDeg about 6.7 km north to south
 * @param lonSpreadDeg about 8 km east to west at this latitude
 * @param driftFracMax the share of an asset's reference speed spent on linear drift, which is
 *     what takes it away from its origin rather than around it
 * @param isaLapseCPerKm the International Standard Atmosphere lapse rate, which is what makes a
 *     climbing asset read colder outside
 */
record FleetConfig(
        int nEntities,
        long seed,
        double centerLat,
        double centerLon,
        double latSpreadDeg,
        double lonSpreadDeg,
        double scaleMetresMin,
        double scaleMetresMax,
        double speedMpsMin,
        double speedMpsMax,
        double driftFracMax,
        double posNoiseTauS,
        double posNoiseSigmaM,
        double altBaseMin,
        double altBaseMax,
        double altAmpMin,
        double altAmpMax,
        double altPeriodMinS,
        double altPeriodMaxS,
        double altNoiseTauS,
        double altNoiseSigmaM,
        double isaLapseCPerKm,
        double internalTauS,
        double internalBaseDeltaC,
        double internalLoadDeltaC,
        double internalNoiseSigmaC,
        double anomalyDurMinS,
        double anomalyDurMaxS,
        double anomalyDeltaMinC,
        double anomalyDeltaMaxC) {

    /** One observer per this many assets, in contiguous blocks. */
    static final int ENTITIES_PER_OBSERVER = 100;

    /** The floor and ceiling an altitude is held between, in metres. */
    static final double ALT_MIN_M = 5.0;

    static final double ALT_MAX_M = 600.0;

    /** The Python's dataclass defaults, with the fleet size and the area a caller's own. */
    static FleetConfig of(int nEntities, long seed, double centerLat, double centerLon,
            double latSpreadDeg, double lonSpreadDeg) {
        return new FleetConfig(
                nEntities,
                seed,
                centerLat,
                centerLon,
                latSpreadDeg,
                lonSpreadDeg,
                500.0,
                50_000.0,
                6.0,
                24.0,
                0.30,
                10.0,
                1.5,
                30.0,
                200.0,
                0.0,
                80.0,
                40.0,
                180.0,
                25.0,
                0.6,
                6.5,
                45.0,
                8.0,
                12.0,
                0.05,
                10.0,
                60.0,
                15.0,
                40.0);
    }

    /** The defaults entire, for a test that cares about the model rather than the area. */
    static FleetConfig of(int nEntities) {
        return of(nEntities, 42L, 59.91, 10.75, 0.06, 0.12);
    }

    /**
     * The arrival rate that holds {@code outlierFraction} of the fleet in an anomaly.
     *
     * <p>An asset is hot for a mean duration d, so with arrivals at rate r the long-run share
     * hot is rd/(1+rd); solving for r gives the rate that lands on the requested share. That is
     * what makes the Settings page's outlier percentage mean what it says rather than being a
     * dial with no units.
     */
    double anomalyRatePerSecond(double outlierFraction) {
        double target = Math.clamp(outlierFraction, 0.0, 0.99);
        if (target <= 0.0) {
            return 0.0;
        }
        double meanDuration = (anomalyDurMinS + anomalyDurMaxS) / 2.0;
        return target / (meanDuration * (1.0 - target));
    }
}
