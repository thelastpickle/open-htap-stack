package com.thelastpickle.htap.sink;

import com.thelastpickle.htap.common.BucketKeys;
import com.thelastpickle.htap.common.Geometry;
import com.thelastpickle.htap.common.ZoneRules;
import com.thelastpickle.htap.common.TimeUuids;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Scores a position against the restricted zones and decides which alerts to write.
 *
 * <p>The warning distance is the dashboard's too, which is why it lives in {@code htap-common}: the
 * what-if simulation counts the assets a hypothetical zone would have inside it and within that
 * distance, so the page and this table measure a position the same way. The risk curve below is this
 * class's alone; the page computes no risk.
 *
 * <p>Scoring and writing are separate: this class answers with the rows an event earned and holds no
 * session, which is what lets the whole of the alerting rule be tested without a cluster.
 */
final class Alerts {

    /** Distance at which proximity is flagged at all, shared with the dashboard. */
    static final double WARNING_DISTANCE_M = ZoneRules.WARNING_DISTANCE_M;

    /** Proximity risk above this counts as a predicted breach. */
    static final double BREACH_RISK_THRESHOLD = 0.7;

    /** Risk below this is not worth an alert row. */
    static final double ALERT_RISK_THRESHOLD = 0.5;

    /** The risk a position inside a zone carries, which is deliberately not 1.0. */
    static final double INSIDE_RISK = 0.95;

    /**
     * One alert per asset per zone per this many seconds.
     *
     * <p>Without it every reading from a loitering asset writes another row, and an asset parked
     * beside a zone would be most of the alert table.
     */
    static final long COOLDOWN_SECONDS = 60;

    /** What one position scored, and the rows it earned. */
    record Proximity(
            boolean nearZone,
            boolean predictedBreach,
            double riskScore,
            String nearestZoneId,
            List<AlertRow> alerts) {

        static final Proximity NOTHING = new Proximity(false, false, 0.0, null, List.of());
    }

    /** One asset and one zone, which is what a cooldown is held per. */
    private record Cooling(String entityId, String zoneId) {}

    private final LongSupplier nanoClock;
    private final Map<Cooling, Long> lastAlertNanos = new HashMap<>();

    private List<Zone> zones = List.of();

    Alerts() {
        this(System::nanoTime);
    }

    Alerts(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
    }

    /** Takes a freshly read zone list, keeping the cooldowns already in force. */
    void reload(List<Zone> loaded) {
        List<String> unusable = loaded.stream().filter(zone -> !zone.usable()).map(Zone::zoneId).toList();
        if (!unusable.isEmpty()) {
            Log.alert("ignoring zones with unusable polygons: %s", String.join(", ", unusable));
        }
        zones = loaded.stream().filter(Zone::usable).toList();
        Log.alert("loaded %d restricted zones", zones.size());
    }

    int loaded() {
        return zones.size();
    }

    /**
     * Scores one position against every zone.
     *
     * <p>Every zone within {@link #WARNING_DISTANCE_M} contributes: the risk reported is the highest
     * of them and the zone named is the one that earned it, so an asset between two zones is
     * described by the nearer.
     */
    Proximity score(String entityId, double lat, double lon, double altitudeM, Instant alertTime) {
        if (zones.isEmpty()) {
            return Proximity.NOTHING;
        }
        boolean nearZone = false;
        boolean predictedBreach = false;
        double riskScore = 0.0;
        String nearestZoneId = null;
        List<AlertRow> alerts = new ArrayList<>();

        for (Zone zone : zones) {
            double distance = Geometry.distanceToPolygonMetres(lat, lon, zone.ring());
            if (distance >= WARNING_DISTANCE_M) {
                continue;
            }
            boolean inside = distance == 0.0;
            double zoneRisk = inside ? INSIDE_RISK : 1.0 - (distance / WARNING_DISTANCE_M);
            nearZone = true;
            if (zoneRisk > riskScore) {
                riskScore = zoneRisk;
                nearestZoneId = zone.zoneId();
            }
            if (inside || zoneRisk > BREACH_RISK_THRESHOLD) {
                predictedBreach = true;
            }
            if (zoneRisk >= ALERT_RISK_THRESHOLD && cooldownElapsed(entityId, zone.zoneId())) {
                alerts.add(row(entityId, lat, lon, altitudeM, alertTime, zone, distance, inside, zoneRisk));
            }
        }
        return new Proximity(nearZone, predictedBreach, riskScore, nearestZoneId, List.copyOf(alerts));
    }

    private AlertRow row(
            String entityId,
            double lat,
            double lon,
            double altitudeM,
            Instant alertTime,
            Zone zone,
            double distance,
            boolean inside,
            double zoneRisk) {
        String message = inside
                ? entityId + " is inside restricted zone " + zone.zoneName()
                // Locale.ROOT, so the distance is ASCII digits whatever locale the JVM took.
                : String.format(
                        Locale.ROOT, "%s is %.0fm from restricted zone %s",
                        entityId, distance, zone.zoneName());
        return new AlertRow(
                BucketKeys.hour(alertTime),
                alertTime,
                entityId,
                TimeUuids.timeUuid(alertTime),
                inside ? "zone_breach" : "zone_proximity",
                inside ? "critical" : (zoneRisk > 0.8 ? "high" : "warning"),
                zone.zoneId(),
                lat,
                lon,
                altitudeM,
                message,
                zoneRisk);
    }

    /** Whether this asset and zone are outside their cooldown, marking them if they are. */
    private boolean cooldownElapsed(String entityId, String zoneId) {
        Cooling key = new Cooling(entityId, zoneId);
        long now = nanoClock.getAsLong();
        Long last = lastAlertNanos.get(key);
        if (last != null && now - last < COOLDOWN_SECONDS * 1_000_000_000L) {
            return false;
        }
        lastAlertNanos.put(key, now);
        return true;
    }
}
