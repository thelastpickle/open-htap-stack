package com.thelastpickle.htap.backend.demo;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.thelastpickle.htap.backend.api.dto.BreachInjected;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.read.FleetRow;
import com.thelastpickle.htap.common.BucketKeys;
import com.thelastpickle.htap.common.TimeUuids;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.random.RandomGenerator;

/**
 * The scripted zone breach: one real asset flagged, and one alert written to match.
 *
 * <p>A real asset and real rows, so nothing here is a fixture the dashboard has to know about. The
 * asset is picked at random from those flying, which is what lets the scenario be triggered
 * repeatedly during a workshop without every trigger landing on the same asset.
 */
@ApplicationScoped
public class BreachScenario {

    /** High enough that the risk filters and the alert feed both pick the asset up. */
    static final double RISK_SCORE = 0.97;

    /** Not a zone in {@code restricted_zones}: the scenario names its own so the map can tell. */
    static final String ZONE_ID = "scenario-zone";

    private final CassandraPath cassandra;
    private final Clock clock;
    private final RandomGenerator random;

    @Inject
    BreachScenario(CassandraPath cassandra) {
        this(cassandra, Clock.systemUTC(), RandomGenerator.getDefault());
    }

    BreachScenario(CassandraPath cassandra, Clock clock, RandomGenerator random) {
        this.cassandra = cassandra;
        this.clock = clock;
        this.random = random;
    }

    /**
     * Flags one of {@code candidates} and writes its alert.
     *
     * <p>The candidates are read by the caller rather than here, so that a fleet the producer has
     * not filled yet is reported as its own refusal instead of as a failed write.
     */
    public BreachInjected inject(List<FleetRow> candidates) {
        FleetRow target = pick(candidates, random);
        double latitude = orZero(target.latitude());
        double longitude = orZero(target.longitude());
        Instant now = clock.instant();
        UUID alertId = TimeUuids.timeUuid(now);

        cassandra.execute(flag(target.entityId()));
        cassandra.execute(alert(target, latitude, longitude, alertId, now));
        return BreachInjected.of(target.entityId(), latitude, longitude, alertId);
    }

    static FleetRow pick(List<FleetRow> candidates, RandomGenerator random) {
        return candidates.get(random.nextInt(candidates.size()));
    }

    /** The asset's own row, which is what the map and the KPI queries read. */
    static SimpleStatement flag(String entityId) {
        return SimpleStatement.newInstance(
                "UPDATE drone_latest_status SET predicted_zone_breach = true, "
                        + "near_restricted_zone = true, risk_score = ? WHERE entity_id = ?",
                RISK_SCORE,
                entityId);
    }

    /**
     * The alert row the feed reads.
     *
     * <p>{@code alert_time} and the timeuuid are bound from one instant, so the alert's clustering
     * order and its identifier agree; two clock readings could put them a millisecond apart and
     * make a sorted feed disagree with itself.
     *
     * <p>{@code altitude_m} is bound as a float and the rest as doubles, because the column is a
     * CQL {@code float} and this driver will not widen for a caller: there is no {@code FLOAT} to
     * {@code Double} codec in the default registry, and asking for one raises {@code
     * CodecNotFoundException}, which {@code BreachScenarioTest} pins. The Python driver coerced it
     * silently, so this is the one binding the port had to change rather than copy.
     */
    static SimpleStatement alert(
            FleetRow target, double latitude, double longitude, UUID alertId, Instant at) {
        return SimpleStatement.newInstance(
                "INSERT INTO alerts_by_bucket (bucket, alert_time, entity_id, alert_id, alert_type, "
                        + "severity, zone_id, latitude, longitude, altitude_m, message, risk_score) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                BucketKeys.hour(at),
                at,
                target.entityId(),
                alertId,
                "zone_breach_predicted",
                BreachInjected.SEVERITY,
                ZONE_ID,
                latitude,
                longitude,
                (float) orZero(target.altitudeM()),
                "Scenario: " + target.entityId()
                        + " is on a predicted course into restricted airspace",
                RISK_SCORE);
    }

    /**
     * A missing coordinate reported as zero.
     *
     * <p>The Python coerced with {@code float(target.get("latitude") or 0.0)}, and the map reads
     * these as numbers, so a null would be a change to the response rather than to this port.
     */
    static double orZero(Double value) {
        return value == null ? 0.0 : value;
    }
}
