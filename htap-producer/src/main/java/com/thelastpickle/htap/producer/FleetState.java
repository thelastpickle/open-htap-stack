package com.thelastpickle.htap.producer;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Random;

/**
 * The fleet's whole state, advanced per asset on each send.
 *
 * <p>One asset holds an origin, a smooth parametric path, an altitude oscillation, an
 * Ornstein-Uhlenbeck deviation from each, an internal temperature and possibly an open anomaly
 * window. That is enough state to make the motion look like flight rather than a random walk,
 * and little enough to hold thousands of assets in a laptop process.
 *
 * <p>Scalar where the Python was vectorised, and that is a port of the model rather than of the
 * plumbing: numpy's arrays were how Python reached the rate, and the same loop in Java is faster
 * than the array traffic would be. The arrays here are {@code double} where the Python held
 * {@code float32}, which was a numpy memory choice; 2,000 assets of state is under a megabyte
 * either way.
 *
 * <p><b>The random stream is not the Python's.</b> numpy's PCG64 and {@link Random}'s linear
 * congruential generator draw different numbers from the same seed, so a given seed does not
 * reproduce a given fleet across the two implementations. What is preserved is every
 * distribution and every bound, which is what the demo's numbers rest on; a stack whose motion
 * had to be byte-identical would need PCG64 written out here, and nothing asks for that.
 */
final class FleetState {

    /** Metres per degree of latitude, which is close enough to constant. */
    private static final double METRES_PER_DEG_LAT = 111_320.0;

    /**
     * The largest head start an asset is given along its own path, in seconds.
     *
     * <p>So the fleet is not synchronised. It has a visible consequence worth knowing: the drift
     * term is metres a second times this, so an asset may be up to 72 km from its origin on its
     * very first reading, and the fleet therefore covers about a degree and a half around the
     * configured centre rather than the spread the centre is given. {@code FleetStateTest} derives
     * that envelope from these figures rather than asserting a box.
     */
    static final double MAX_TIME_OFFSET_S = 10_000.0;

    /** How far a step may advance the model, in seconds, however long the process was paused. */
    private static final double DT_MIN_S = 1e-3;

    private static final double DT_MAX_S = 2.0;

    private final FleetConfig cfg;
    private final Random rng;
    private final double t0Seconds;

    private final String[] observerIds;

    private final double[] lat0;
    private final double[] lon0;
    private final double[] invMetresPerDegLon;

    private final double[] scale;
    private final double[] speedRef;
    private final double[] w1;
    private final double[] w2;
    private final double[] phi1;
    private final double[] phi2;
    private final double[] phi3;
    private final double[] phi4;
    private final double[] driftX;
    private final double[] driftY;
    private final double[] tOffset;

    private final double[] altBase;
    private final double[] altAmp;
    private final double[] wAlt;
    private final double[] phiAlt;

    private final double[] noiseX;
    private final double[] noiseY;
    private final double[] noiseAlt;

    private final double[] lastT;
    private final double[] xPrev;
    private final double[] yPrev;
    private final double[] altPrev;

    private final double[] tempIn;

    private final double[] anomalyEnd;
    private final double[] anomalyDelta;

    private final String[] textCache;
    private final double[] nextTextT;
    private final long[] textRevision;

    FleetState(FleetConfig cfg, double startedAtSeconds) {
        this.cfg = cfg;
        this.rng = new Random(cfg.seed());
        this.t0Seconds = startedAtSeconds;
        int n = cfg.nEntities();

        this.observerIds = new String[n];
        this.lat0 = new double[n];
        this.lon0 = new double[n];
        this.invMetresPerDegLon = new double[n];
        this.scale = new double[n];
        this.speedRef = new double[n];
        this.w1 = new double[n];
        this.w2 = new double[n];
        this.phi1 = new double[n];
        this.phi2 = new double[n];
        this.phi3 = new double[n];
        this.phi4 = new double[n];
        this.driftX = new double[n];
        this.driftY = new double[n];
        this.tOffset = new double[n];
        this.altBase = new double[n];
        this.altAmp = new double[n];
        this.wAlt = new double[n];
        this.phiAlt = new double[n];
        this.noiseX = new double[n];
        this.noiseY = new double[n];
        this.noiseAlt = new double[n];
        this.lastT = new double[n];
        this.xPrev = new double[n];
        this.yPrev = new double[n];
        this.altPrev = new double[n];
        this.tempIn = new double[n];
        this.anomalyEnd = new double[n];
        this.anomalyDelta = new double[n];
        this.textCache = new String[n];
        this.nextTextT = new double[n];
        this.textRevision = new long[n];

        int observers = Math.max(1, n / FleetConfig.ENTITIES_PER_OBSERVER);
        for (int i = 0; i < n; i++) {
            // One observer per hundred assets, in contiguous blocks. The origins are already
            // clustered in one area, so the observer is a second grouping dimension beside the
            // asset rather than a place; contiguous blocks make that grouping predictable.
            // Locale.ROOT, for the reason EventPartitions gives beside its own formatter: under
            // fa-IR the default locale prints Persian digits, and an observer named in them
            // matches no row any engine wrote.
            observerIds[i] = String.format(
                    Locale.ROOT,
                    "observer-%04d",
                    Math.min(i / FleetConfig.ENTITIES_PER_OBSERVER, observers - 1));

            lat0[i] = uniform(cfg.centerLat() - cfg.latSpreadDeg(), cfg.centerLat() + cfg.latSpreadDeg());
            lon0[i] = uniform(cfg.centerLon() - cfg.lonSpreadDeg(), cfg.centerLon() + cfg.lonSpreadDeg());
            // Per asset, so it stays right as the fleet spreads in latitude. The clamp keeps the
            // conversion finite near a pole, which this fleet never reaches and a moved one might.
            invMetresPerDegLon[i] =
                    1.0 / (METRES_PER_DEG_LAT * Math.max(0.2, Math.cos(Math.toRadians(lat0[i]))));

            scale[i] = uniform(cfg.scaleMetresMin(), cfg.scaleMetresMax());
            speedRef[i] = uniform(cfg.speedMpsMin(), cfg.speedMpsMax());
            // Angular frequencies chosen so that the typical speed lands near speedRef.
            w1[i] = (speedRef[i] / scale[i]) * uniform(0.6, 1.4);
            w2[i] = w1[i] * uniform(1.4, 2.3);
            phi1[i] = uniform(0, 2 * Math.PI);
            phi2[i] = uniform(0, 2 * Math.PI);
            phi3[i] = uniform(0, 2 * Math.PI);
            phi4[i] = uniform(0, 2 * Math.PI);

            double driftMagnitude = speedRef[i] * uniform(0.0, cfg.driftFracMax());
            double driftAngle = uniform(0, 2 * Math.PI);
            driftX[i] = driftMagnitude * Math.cos(driftAngle);
            driftY[i] = driftMagnitude * Math.sin(driftAngle);
            tOffset[i] = uniform(0, MAX_TIME_OFFSET_S);

            altBase[i] = uniform(cfg.altBaseMin(), cfg.altBaseMax());
            altAmp[i] = uniform(cfg.altAmpMin(), cfg.altAmpMax());
            wAlt[i] = 2 * Math.PI / uniform(cfg.altPeriodMinS(), cfg.altPeriodMaxS());
            phiAlt[i] = uniform(0, 2 * Math.PI);

            lastT[i] = startedAtSeconds;
            altPrev[i] = altBase[i];
            // Set properly on the first step; this is ambient plus an operational delta.
            tempIn[i] = 25.0;
            textCache[i] = "";
        }
    }

    String observerId(int index) {
        return observerIds[index];
    }

    /**
     * Advances the assets named and answers their telemetry.
     *
     * <p>{@code ids} may name an asset more than once, and does whenever a batch is larger than
     * the live fleet: the second visit sees a {@code dt} of nothing and the model holds still,
     * which is why {@code dt} has a floor rather than being allowed to reach zero.
     */
    Telemetry[] step(
            int[] ids,
            double nowSeconds,
            TextSource text,
            double refreshMinS,
            double refreshMaxS,
            double outlierFraction) {
        double anomalyRate = cfg.anomalyRatePerSecond(outlierFraction);
        Weather weather = Weather.at(nowSeconds);
        Telemetry[] batch = new Telemetry[ids.length];

        for (int k = 0; k < ids.length; k++) {
            int i = ids[k];
            double dt = Math.clamp(nowSeconds - lastT[i], DT_MIN_S, DT_MAX_S);
            double t = (nowSeconds - t0Seconds) + tOffset[i];

            noiseX[i] = ouStep(noiseX[i], dt, cfg.posNoiseTauS(), cfg.posNoiseSigmaM());
            noiseY[i] = ouStep(noiseY[i], dt, cfg.posNoiseTauS(), cfg.posNoiseSigmaM());
            noiseAlt[i] = ouStep(noiseAlt[i], dt, cfg.altNoiseTauS(), cfg.altNoiseSigmaM());

            // Two harmonics and a drift: the harmonics give a closed path of the asset's own
            // size, and the drift is what carries it across the map.
            double x = driftX[i] * t
                    + scale[i] * (Math.sin(w1[i] * t + phi1[i]) + 0.5 * Math.sin(w2[i] * t + phi2[i]))
                    + noiseX[i];
            double y = driftY[i] * t
                    + scale[i] * (Math.cos(w1[i] * t + phi3[i]) + 0.5 * Math.cos(w2[i] * t + phi4[i]))
                    + noiseY[i];
            double altitude = Math.clamp(
                    altBase[i] + altAmp[i] * Math.sin(wAlt[i] * t + phiAlt[i]) + noiseAlt[i],
                    FleetConfig.ALT_MIN_M,
                    FleetConfig.ALT_MAX_M);

            double lat = Math.clamp(lat0[i] + y / METRES_PER_DEG_LAT, -85.0, 85.0);
            double lon = normaliseLongitude(lon0[i] + x * invMetresPerDegLon[i]);

            // Speed and climb from the last position, which stand in for how hard the asset is
            // working; the internal temperature follows that load.
            double speed = Math.hypot(x - xPrev[i], y - yPrev[i]) / dt;
            double climb = (altitude - altPrev[i]) / dt;
            xPrev[i] = x;
            yPrev[i] = y;
            altPrev[i] = altitude;
            lastT[i] = nowSeconds;

            double load = Math.clamp(
                    0.4 * (speed / Math.max(speedRef[i], 1e-3)) + 0.6 * (Math.abs(climb) / 3.0),
                    0.0,
                    1.0);

            double tempExternal = weather.externalTempC(lat, altitude, cfg.isaLapseCPerKm(), rng);

            // An anomaly opens with probability 1 - exp(-rate*dt), and only if the last one has
            // closed: overlapping windows would make the share hot exceed what was asked for.
            if (anomalyEnd[i] <= nowSeconds && anomalyRate > 0.0) {
                if (rng.nextDouble() < 1.0 - Math.exp(-anomalyRate * dt)) {
                    anomalyEnd[i] = nowSeconds + uniform(cfg.anomalyDurMinS(), cfg.anomalyDurMaxS());
                    anomalyDelta[i] = uniform(cfg.anomalyDeltaMinC(), cfg.anomalyDeltaMaxC());
                }
            }
            double anomalyExtra = anomalyEnd[i] > nowSeconds ? anomalyDelta[i] : 0.0;

            // First-order lag towards the target, so a spike takes tens of seconds to show and
            // tens more to clear rather than appearing in one reading.
            double alpha = 1.0 - Math.exp(-dt / cfg.internalTauS());
            double target = tempExternal
                    + cfg.internalBaseDeltaC()
                    + cfg.internalLoadDeltaC() * load
                    + anomalyExtra;
            tempIn[i] += alpha * (target - tempIn[i])
                    + rng.nextGaussian() * cfg.internalNoiseSigmaC();

            batch[k] = new Telemetry(
                    i,
                    lat,
                    lon,
                    altitude,
                    tempExternal,
                    tempIn[i],
                    textFor(i, nowSeconds, text, refreshMinS, refreshMaxS));
        }
        return batch;
    }

    /** Whether this asset is inside an anomaly window, which is what a share is counted over. */
    boolean inAnomaly(int index, double nowSeconds) {
        return anomalyEnd[index] > nowSeconds;
    }

    /**
     * The asset's snippet, refreshed on its own cadence and not per event.
     *
     * <p>Per event would put a corpus read in front of every send; the cache is what keeps the
     * text payload off the hot path, and the refresh interval is what makes the snippets change
     * often enough for the vector page to have something to follow.
     */
    private String textFor(
            int index, double nowSeconds, TextSource text, double refreshMinS, double refreshMaxS) {
        if (nowSeconds >= nextTextT[index]) {
            textRevision[index]++;
            textCache[index] = text.sample(index * 1_000_003L + textRevision[index]);
            nextTextT[index] = nowSeconds + uniform(refreshMinS, refreshMaxS);
        }
        return textCache[index];
    }

    /**
     * One Ornstein-Uhlenbeck step, discretised exactly rather than by an Euler approximation.
     *
     * <p>The process pulls back towards zero with time constant {@code tau} and has stationary
     * standard deviation {@code sigma}, so the deviation stays small whatever the step size; an
     * Euler step would let a long {@code dt} overshoot.
     */
    private double ouStep(double value, double dt, double tau, double sigma) {
        double safeTau = Math.max(1e-6, tau);
        double decay = Math.exp(-dt / safeTau);
        double innovation = sigma * Math.sqrt(1.0 - Math.exp(-2.0 * dt / safeTau));
        return value * decay + innovation * rng.nextGaussian();
    }

    private double uniform(double low, double high) {
        return low + rng.nextDouble() * (high - low);
    }

    /**
     * To {@code [-180, 180)}, so a drifting asset crossing the antimeridian stays on the map.
     *
     * <p>Written out rather than as one remainder, because Java's {@code %} keeps the sign of its
     * left operand where Python's takes the sign of the divisor: {@code -181 % 360} is -181 here
     * and 179 there, and a longitude of -181 would put an asset off the map.
     */
    static double normaliseLongitude(double lon) {
        double shifted = (lon + 180.0) % 360.0;
        return (shifted < 0.0 ? shifted + 360.0 : shifted) - 180.0;
    }

    /**
     * The weather at one instant, which every asset in a batch shares apart from its own
     * latitude and altitude.
     *
     * <p>Cheap and plausible rather than accurate: a latitude baseline, a seasonal term from the
     * day of the year, a diurnal term from the hour, and the lapse rate with altitude. Computed
     * once per batch because the two terms that need a calendar do not vary within one.
     */
    record Weather(double season, double diurnal) {

        static Weather at(double nowSeconds) {
            OffsetDateTime utc = OffsetDateTime.ofInstant(
                    Instant.ofEpochMilli(Math.round(nowSeconds * 1000.0)), ZoneOffset.UTC);
            double dayOfYear = utc.getDayOfYear();
            double hour = utc.getHour() + utc.getMinute() / 60.0;
            // Warmest around the 173rd day in the northern hemisphere, and mid-afternoon.
            return new Weather(
                    Math.cos(2 * Math.PI * (dayOfYear - 173) / 365.0),
                    5.0 * Math.sin(2 * Math.PI * (hour - 14.0) / 24.0));
        }

        double externalTempC(double lat, double altitudeM, double lapseCPerKm, Random rng) {
            double byLatitude = Math.clamp(30.0 - 0.7 * Math.abs(lat), -35.0, 38.0);
            double bySeason = 10.0 * season * Math.signum(lat);
            double atSeaLevel = byLatitude + bySeason + diurnal + rng.nextGaussian() * 0.5;
            return atSeaLevel - lapseCPerKm * (altitudeM / 1000.0) + rng.nextGaussian() * 0.3;
        }
    }
}
