package com.thelastpickle.htap.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.common.Geometry;
import com.thelastpickle.htap.sink.DroneTracker.Derived;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Speed, heading and flight state, which exist only because the previous reading is held. */
class DroneTrackerTest {

    private static final Instant AT = Instant.parse("2026-08-29T12:00:00Z");

    private final DroneTracker tracker = new DroneTracker();

    /** The first reading for an asset has nothing to derive from, and says so with zeros. */
    @Test
    void theFirstReadingDerivesNothing() {
        Derived derived = tracker.update("asset-1", 59.91, 10.75, 120.0, AT);

        assertEquals(0.0, derived.speedMps());
        assertEquals(0.0, derived.headingDeg());
        assertTrue(derived.flying(), "altitude alone decides this, and 120 m is flying");
    }

    /** Distance over time, and the heading from the previous position to this one. */
    @Test
    void theSecondReadingGivesSpeedAndHeading() {
        tracker.update("asset-1", 59.91, 10.75, 120.0, AT);
        Derived derived = tracker.update("asset-1", 59.92, 10.75, 120.0, AT.plusSeconds(100));

        double metres = Geometry.haversineDistanceMetres(59.91, 10.75, 59.92, 10.75);
        assertEquals(metres / 100.0, derived.speedMps(), 1e-9);
        assertEquals(0.0, derived.headingDeg(), 1e-6, "due north");
    }

    /**
     * A speed above the plausible ceiling is a position glitch, so the previous speed stands.
     *
     * <p>Reported rather than dropped: the live map draws it, and a hundred-metre jump between two
     * readings a second apart would otherwise show as 100 m/s.
     */
    @Test
    void anImplausibleSpeedKeepsThePreviousOne() {
        tracker.update("asset-1", 59.91, 10.75, 120.0, AT);
        Derived steady = tracker.update("asset-1", 59.9105, 10.75, 120.0, AT.plusSeconds(10));
        Derived glitched = tracker.update("asset-1", 60.91, 10.75, 120.0, AT.plusSeconds(11));

        assertTrue(steady.speedMps() < DroneTracker.MAX_PLAUSIBLE_SPEED_MPS);
        assertEquals(steady.speedMps(), glitched.speedMps(),
                "the glitch is 111 km in a second, so the last plausible speed stands");
    }

    /**
     * Two readings at the same instant would divide by zero; a millisecond is substituted.
     *
     * <p>Which makes all but the smallest movement implausible at that gap, so the two rules meet
     * here: a metre in the substituted millisecond is a thousand metres a second and the previous
     * speed stands, where a centimetre is reported.
     */
    @Test
    void twoReadingsAtOneInstantDoNotDivideByZero() {
        tracker.update("asset-1", 59.91, 10.75, 120.0, AT);
        Derived derived = tracker.update("asset-1", 59.9100001, 10.75, 120.0, AT);

        double metres = Geometry.haversineDistanceMetres(59.91, 10.75, 59.9100001, 10.75);
        assertEquals(metres / 0.001, derived.speedMps(), 1e-6);
        assertTrue(derived.speedMps() < DroneTracker.MAX_PLAUSIBLE_SPEED_MPS, "11 m/s, so reported");
    }

    /** An out-of-order reading takes the same substitution, and never gives a negative speed. */
    @Test
    void aReadingOlderThanTheLastOneIsNotNegative() {
        tracker.update("asset-1", 59.91, 10.75, 120.0, AT);
        Derived derived = tracker.update("asset-1", 59.9100001, 10.75, 120.0, AT.minusSeconds(5));

        assertTrue(derived.speedMps() > 0.0);
        assertTrue(Double.isFinite(derived.speedMps()));
    }

    /** The threshold is altitude and only altitude, whatever the asset is doing. */
    @Test
    void flyingIsDecidedByAltitudeAlone() {
        assertFalse(tracker.update("asset-1", 59.91, 10.75, 10.0, AT).flying(), "10 m is the boundary");
        assertTrue(tracker.update("asset-2", 59.91, 10.75, 10.01, AT).flying());
        assertFalse(tracker.update("asset-3", 59.91, 10.75, 0.0, AT).flying());
    }

    /** One asset's readings say nothing about another's. */
    @Test
    void eachAssetIsTrackedApart() {
        tracker.update("asset-1", 59.91, 10.75, 120.0, AT);
        tracker.update("asset-2", 10.0, 20.0, 120.0, AT);
        Derived second = tracker.update("asset-2", 10.0, 20.0, 120.0, AT.plusSeconds(10));

        assertEquals(0.0, second.speedMps(), "asset-2 has not moved");
        assertEquals(2, tracker.tracked());
    }
}
