package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The model: where the assets are, how they move, and how hot they run.
 *
 * <p>Every assertion here is about a bound or a share rather than about a value, because the random
 * stream is Java's and not numpy's: the same seed gives a different fleet across the two
 * implementations, and what the demo rests on is that the distributions and the bounds are the
 * same.
 */
class FleetStateTest {

    /** An arbitrary instant with a known day and hour, so the weather terms are settled. */
    private static final double NOON = 1787846133.0;

    private final FleetConfig cfg = FleetConfig.of(300);
    private final FleetState state = new FleetState(cfg, NOON);

    /**
     * Every reading is near the configured area, which is what keeps the fleet on one map view.
     *
     * <p>Near rather than inside, and the difference is the model rather than a tolerance. The
     * spread bounds where an asset's path is centred; the path is up to 1.5 times the largest
     * scale across, 75 km; and the drift carries it further still, up to the largest speed times
     * the largest head start, 7.2 m/s over 10,000 s, which is another 72 km. So the fleet covers
     * about a degree and a half around the centre from its very first reading, and the bound here
     * is those three terms rather than a figure that happened to pass.
     */
    @Test
    void everyReadingIsNearTheConfiguredArea() {
        double reachMetres = 1.5 * cfg.scaleMetresMax()
                + cfg.driftFracMax() * cfg.speedMpsMax() * FleetState.MAX_TIME_OFFSET_S;
        double reachDegLat = reachMetres / 111_320.0;
        double reachDegLon = reachDegLat / Math.cos(Math.toRadians(cfg.centerLat()));
        Telemetry[] batch = step(ids(0, 300), NOON);

        for (Telemetry telemetry : batch) {
            assertTrue(
                    Math.abs(telemetry.lat() - cfg.centerLat()) <= cfg.latSpreadDeg() + reachDegLat,
                    "latitude too far from the area: " + telemetry.lat());
            assertTrue(
                    Math.abs(telemetry.lon() - cfg.centerLon()) <= cfg.lonSpreadDeg() + reachDegLon,
                    "longitude too far from the area: " + telemetry.lon());
        }
    }

    /**
     * One observer per hundred assets, in contiguous blocks.
     *
     * <p>A second grouping dimension beside the asset, which is what the compare page's
     * {@code GROUP BY} needs: contiguous blocks make the grouping predictable, and the last block
     * absorbs the remainder rather than a stray observer holding one asset.
     */
    @Test
    void observersComeInBlocksOfAHundred() {
        assertEquals("observer-0000", state.observerId(0));
        assertEquals("observer-0000", state.observerId(99));
        assertEquals("observer-0001", state.observerId(100));
        assertEquals("observer-0002", state.observerId(299));

        FleetState ragged = new FleetState(FleetConfig.of(250), NOON);
        assertEquals("observer-0001", ragged.observerId(200), "the last block takes the remainder");
        assertEquals(
                2,
                new HashSet<>(List.of(ragged.observerId(0), ragged.observerId(100), ragged.observerId(249)))
                        .size());
    }

    /** A fleet of fewer than a hundred still has one observer rather than none. */
    @Test
    void aSmallFleetStillHasAnObserver() {
        assertEquals("observer-0000", new FleetState(FleetConfig.of(10), NOON).observerId(9));
    }

    /** Altitude is held inside the band the demo's map draws, whatever the oscillation does. */
    @Test
    void altitudeStaysInsideItsBand() {
        for (int turn = 0; turn < 40; turn++) {
            for (Telemetry telemetry : step(ids(0, 300), NOON + turn * 0.5)) {
                assertTrue(
                        telemetry.altitudeM() >= FleetConfig.ALT_MIN_M
                                && telemetry.altitudeM() <= FleetConfig.ALT_MAX_M,
                        "altitude outside the band: " + telemetry.altitudeM());
            }
        }
    }

    /** The assets move, and by metres rather than by degrees, over a second of model time. */
    @Test
    void theAssetsMoveSmoothly() {
        Telemetry[] first = step(ids(0, 50), NOON);
        Telemetry[] second = step(ids(0, 50), NOON + 1.0);

        for (int i = 0; i < first.length; i++) {
            double metres = 111_320.0 * Math.hypot(
                    second[i].lat() - first[i].lat(), second[i].lon() - first[i].lon());
            assertNotEquals(first[i].lat(), second[i].lat(), "the asset did not move at all");
            assertTrue(metres < 2_000.0, "moved " + metres + " m in one second");
        }
    }

    /**
     * A longitude past the antimeridian comes back inside the map.
     *
     * <p>Java's remainder keeps the sign of its left operand where Python's takes the sign of the
     * divisor, so this is the one place the port could have differed silently.
     */
    @Test
    void aLongitudePastTheAntimeridianWraps() {
        assertEquals(-179.0, FleetState.normaliseLongitude(181.0), 1e-9);
        assertEquals(179.0, FleetState.normaliseLongitude(-181.0), 1e-9);
        assertEquals(-180.0, FleetState.normaliseLongitude(180.0), 1e-9);
        assertEquals(0.0, FleetState.normaliseLongitude(720.0), 1e-9);
        assertEquals(10.75, FleetState.normaliseLongitude(10.75), 1e-9);
    }

    /**
     * The anomaly arrival rate is the one that holds the requested share of the fleet hot.
     *
     * <p>rd/(1+rd) is the long-run share for a mean duration d, so r = target/(d(1-target)); with
     * the configured 10 to 60 second durations, d is 35.
     */
    @Test
    void theAnomalyRateIsTheOneThatHoldsTheRequestedShare() {
        assertEquals(0.0, cfg.anomalyRatePerSecond(0.0));
        assertEquals(0.05 / (35.0 * 0.95), cfg.anomalyRatePerSecond(0.05), 1e-12);
        assertEquals(0.5 / (35.0 * 0.5), cfg.anomalyRatePerSecond(0.5), 1e-12);
        // A share of one would ask for an infinite rate, so the target is held below it.
        assertEquals(0.99 / (35.0 * 0.01), cfg.anomalyRatePerSecond(1.0), 1e-9);
        assertEquals(0.0, cfg.anomalyRatePerSecond(-1.0), "a negative share is no anomalies");
    }

    /**
     * The share of the fleet running hot tracks the outlier setting.
     *
     * <p>The claim the Settings page makes, and the reason the rate is derived rather than tuned.
     * Simulated over ten minutes of model time on a fleet of 1,000; the tolerance is wide because
     * this is one sample of a random process, and the point is that 20% is not 5%.
     */
    @Test
    void theShareRunningHotTracksTheSetting() {
        double low = shareInAnomaly(0.05);
        double high = shareInAnomaly(0.20);

        assertTrue(low < 0.12, "5% asked for, got " + low);
        assertTrue(high > 0.10 && high < 0.32, "20% asked for, got " + high);
        assertTrue(high > low, "20% gave " + high + " where 5% gave " + low);
        assertEquals(0.0, shareInAnomaly(0.0), "no anomalies were asked for");
    }

    /**
     * An asset runs warmer inside than out: the base delta plus whatever the load adds.
     *
     * <p>With no anomalies asked for, so the band is the model's own 8 degrees plus up to 12 of
     * load. An anomaly adds 15 to 40 on top, which is the whole point of it and is what the
     * alerting reads; asserting the quiet band is what says the base and the load are right.
     */
    @Test
    void theInsideIsWarmerThanTheOutside() {
        // Long enough for the first-order lag to have caught up with its target.
        Telemetry[] batch = null;
        for (int turn = 0; turn < 200; turn++) {
            batch = state.step(ids(0, 20), NOON + turn, TextSource.NONE, 5.0, 30.0, 0.0);
        }

        for (Telemetry telemetry : batch) {
            double delta = telemetry.tempInternalC() - telemetry.tempExternalC();
            assertTrue(delta > 5.0 && delta < 22.0, "internal minus external was " + delta);
        }
    }

    /** A text payload is refreshed on its own cadence, not once and not per event. */
    @Test
    void theTextRefreshesOnItsOwnCadence() {
        TextSource corpus = seed -> "snippet " + seed;
        String first = state.step(ids(0, 1), NOON, corpus, 5.0, 5.0, 0.0)[0].text();
        String again = state.step(ids(0, 1), NOON + 1.0, corpus, 5.0, 5.0, 0.0)[0].text();
        String later = state.step(ids(0, 1), NOON + 6.0, corpus, 5.0, 5.0, 0.0)[0].text();

        assertEquals(first, again, "a refresh inside the interval would put a read on the hot path");
        assertNotEquals(first, later, "the snippet never changed, so the vector page sees one text");
    }

    /**
     * An asset named twice in one batch is advanced once and then holds still.
     *
     * <p>Which happens whenever a batch is larger than the live fleet, at 2,000 events a second
     * over a hundred assets. The floor on the step is what keeps the speed finite there: a second
     * visit with a true zero would divide by it.
     */
    @Test
    void anAssetVisitedTwiceInOneBatchIsFinite() {
        Telemetry[] batch = step(new int[] {0, 0, 0}, NOON + 1.0);

        for (Telemetry telemetry : batch) {
            assertTrue(Double.isFinite(telemetry.lat()) && Double.isFinite(telemetry.lon()));
            assertTrue(Double.isFinite(telemetry.tempInternalC()));
        }
    }

    /**
     * The share hot after ten minutes of model time at one reading a second per asset.
     *
     * <p>Ten minutes is some seventeen mean anomaly durations, which is long enough for the share
     * to have settled and short enough to keep this suite in the milliseconds it belongs in.
     */
    private double shareInAnomaly(double fraction) {
        FleetState fleet = new FleetState(FleetConfig.of(1000), NOON);
        int[] all = ids(0, 1000);
        double at = NOON;
        for (int second = 0; second < 600; second++) {
            at += 1.0;
            fleet.step(all, at, TextSource.NONE, 5.0, 30.0, fraction);
        }
        Set<Integer> hot = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            if (fleet.inAnomaly(i, at)) {
                hot.add(i);
            }
        }
        return hot.size() / 1000.0;
    }

    private Telemetry[] step(int[] ids, double atSeconds) {
        return state.step(ids, atSeconds, TextSource.NONE, 5.0, 30.0, 0.05);
    }

    private static int[] ids(int from, int count) {
        List<Integer> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(from + i);
        }
        return ids.stream().mapToInt(Integer::intValue).toArray();
    }
}
