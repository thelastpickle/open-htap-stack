package com.thelastpickle.htap.backend.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.thelastpickle.htap.backend.read.FleetRow;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

/**
 * The two statements the scripted breach writes, and how it chooses its asset.
 *
 * <p>The statements are asserted rather than executed: what a test can hold to is the CQL and the
 * values bound to it, and whether the rows arrive is what the workflow's dashboard step checks
 * against a real cluster.
 */
class BreachScenarioTest {

    private static final Instant AT = Instant.parse("2026-08-29T14:07:31.250Z");
    private static final UUID ALERT_ID = UUID.fromString("d4e1f6a2-8b3c-11ee-9c00-0242ac120002");

    @Test
    void theAssetsOwnRowIsFlaggedByItsPartitionKey() {
        SimpleStatement flag = BreachScenario.flag("drone-0042");

        assertEquals(
                "UPDATE drone_latest_status SET predicted_zone_breach = true, "
                        + "near_restricted_zone = true, risk_score = ? WHERE entity_id = ?",
                flag.getQuery());
        assertEquals(List.of(BreachScenario.RISK_SCORE, "drone-0042"), flag.getPositionalValues());
    }

    @Test
    void theAlertCarriesTheAssetTheHourBucketAndTheScenariosOwnZone() {
        SimpleStatement alert = BreachScenario.alert(asset("drone-0042", 51.5, -0.12, 120.5f),
                51.5, -0.12, ALERT_ID, AT);

        assertTrue(alert.getQuery().startsWith("INSERT INTO alerts_by_bucket (bucket, alert_time, "));
        assertEquals(
                List.of(
                        "2026-08-29T14",
                        AT,
                        "drone-0042",
                        ALERT_ID,
                        "zone_breach_predicted",
                        "critical",
                        BreachScenario.ZONE_ID,
                        51.5,
                        -0.12,
                        120.5f,
                        "Scenario: drone-0042 is on a predicted course into restricted airspace",
                        BreachScenario.RISK_SCORE),
                alert.getPositionalValues());
    }

    /**
     * The one binding this driver will not widen for a caller, so the cast in {@code alert} is what
     * keeps the write from failing at execution rather than at compile time.
     */
    @Test
    void theAltitudeIsBoundAsAFloatBecauseTheColumnIsOne() {
        SimpleStatement alert = BreachScenario.alert(
                asset("drone-0042", 51.5, -0.12, 120.5f), 51.5, -0.12, ALERT_ID, AT);

        assertInstanceOf(Float.class, alert.getPositionalValues().get(9));
        assertThrows(
                CodecNotFoundException.class,
                () -> CodecRegistry.DEFAULT.codecFor(DataTypes.FLOAT, Double.class));
    }

    @Test
    void anAssetWithNoPositionIsWrittenAtZeroRatherThanRefused() {
        SimpleStatement alert = BreachScenario.alert(
                asset("drone-0042", null, null, null), 0.0, 0.0, ALERT_ID, AT);

        assertEquals(0.0, alert.getPositionalValues().get(7));
        assertEquals(0.0, alert.getPositionalValues().get(8));
        assertEquals(0.0f, alert.getPositionalValues().get(9));
        assertEquals(0.0, BreachScenario.orZero(null));
        assertEquals(12.5, BreachScenario.orZero(12.5));
    }

    @Test
    void theChosenAssetIsOneOfTheCandidates() {
        List<FleetRow> candidates = fleet(4);

        assertTrue(candidates.contains(BreachScenario.pick(candidates, RandomGenerator.getDefault())));
    }

    /** Repeated triggers landing on one asset would make the scenario look like a fixture. */
    @Test
    void repeatedTriggersDoNotAllLandOnTheSameAsset() {
        List<FleetRow> candidates = fleet(20);
        Set<String> chosen = new HashSet<>();
        RandomGenerator random = RandomGenerator.getDefault();

        for (int i = 0; i < 100; i++) {
            chosen.add(BreachScenario.pick(candidates, random).entityId());
        }

        assertTrue(chosen.size() > 1, "every one of 100 picks chose " + chosen);
    }

    private static FleetRow asset(String entityId, Double latitude, Double longitude, Float alt) {
        return new FleetRow(
                entityId,
                "observer-1",
                AT,
                latitude,
                longitude,
                alt == null ? null : Double.valueOf(alt),
                12.0,
                90.0,
                true,
                20.0,
                18.0,
                false,
                false,
                0.1);
    }

    private static List<FleetRow> fleet(int count) {
        List<FleetRow> candidates = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            candidates.add(asset("drone-" + i, 51.5, -0.12, 120.5f));
        }
        return candidates;
    }
}
