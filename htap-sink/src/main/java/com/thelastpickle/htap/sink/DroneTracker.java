package com.thelastpickle.htap.sink;

import com.thelastpickle.htap.common.Geometry;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Speed, heading and flight state, derived from the previous reading for that asset.
 *
 * <p>In memory and per asset, which is what makes the sink stateful and why a restart reports zero
 * speed for one reading per asset. Held here rather than read back from Cassandra: the previous row
 * is one point read per event, 2,000 a second, to derive a figure the next event supersedes.
 */
final class DroneTracker {

    /**
     * Altitude alone decides whether an asset counts as flying.
     *
     * <p>With a variable gap between an asset's readings a derived speed is too noisy to threshold,
     * so it is not used for this.
     */
    static final double FLYING_ALTITUDE_THRESHOLD_M = 10.0;

    /** Above this, a derived speed is a position glitch rather than movement. */
    static final double MAX_PLAUSIBLE_SPEED_MPS = 100.0;

    /** What one reading gives, once the previous one for that asset is known. */
    record Derived(double speedMps, double headingDeg, boolean flying) {}

    private final Map<String, Seen> state = new HashMap<>();

    /** Derives this reading's figures and becomes the asset's new previous reading. */
    Derived update(String entityId, double lat, double lon, double altitudeM, Instant eventTime) {
        Seen previous = state.get(entityId);
        boolean flying = altitudeM > FLYING_ALTITUDE_THRESHOLD_M;
        if (previous == null) {
            state.put(entityId, new Seen(lat, lon, eventTime, 0.0, 0.0));
            return new Derived(0.0, 0.0, flying);
        }

        // A reading that arrives at or before its predecessor would divide by zero or answer a
        // negative speed; a millisecond is what the Python substituted.
        double seconds = Duration.between(previous.at(), eventTime).toNanos() / 1e9;
        if (seconds <= 0) {
            seconds = 0.001;
        }
        double metres = Geometry.haversineDistanceMetres(previous.lat(), previous.lon(), lat, lon);
        double speedMps = metres / seconds;
        if (speedMps > MAX_PLAUSIBLE_SPEED_MPS) {
            speedMps = previous.speedMps();
        }
        double headingDeg =
                Geometry.initialHeadingDegrees(previous.lat(), previous.lon(), lat, lon);

        state.put(entityId, new Seen(lat, lon, eventTime, speedMps, headingDeg));
        return new Derived(speedMps, headingDeg, flying);
    }

    /** How many assets are being tracked, which is the whole of this class's memory. */
    int tracked() {
        return state.size();
    }

    private record Seen(double lat, double lon, Instant at, double speedMps, double headingDeg) {}
}
