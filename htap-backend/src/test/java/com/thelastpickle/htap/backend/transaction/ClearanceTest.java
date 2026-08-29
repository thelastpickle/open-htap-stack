package com.thelastpickle.htap.backend.transaction;

import static com.thelastpickle.htap.backend.transaction.TransactionFakes.KEYSPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.cql.Row;
import com.thelastpickle.htap.backend.api.ApiException;
import com.thelastpickle.htap.backend.api.dto.ClearanceContentionResult;
import com.thelastpickle.htap.backend.api.dto.ClearanceDemoResult;
import com.thelastpickle.htap.backend.api.dto.ClearanceResetResult;
import com.thelastpickle.htap.backend.api.dto.ClearanceState;
import com.thelastpickle.htap.backend.api.dto.ClearanceZone;
import com.thelastpickle.htap.backend.api.dto.TransactionStep;
import com.thelastpickle.htap.backend.transaction.TransactionFakes.FakeAccord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** The semaphore's two statements, and the invariant it has to keep after every one of them. */
class ClearanceTest {

    private static final Instant AT = Instant.parse("2026-08-29T12:00:00Z");

    private final FakeAccord node = new FakeAccord();
    private final AtomicLong nanos = new AtomicLong();
    private final Clearance clearance =
            new Clearance(node, Clock.fixed(AT, ZoneOffset.UTC), () -> nanos.addAndGet(1_000_000));

    // ──────────────────────── The statements ────────────────────────

    /**
     * Counting down, because Accord will not compare two {@code LET} references.
     *
     * <p>{@code IF occ.granted < occ.capacity} is refused with a {@code SyntaxException}, so the guard
     * tests a reference against a literal and {@code SET remaining -= 1} is the whole
     * read-modify-write, done inside consensus.
     */
    @Test
    void theGrantTestsARemainingCountAgainstZero() {
        String cql = Clearance.grantCql(KEYSPACE);

        assertTrue(cql.contains("IF occ.remaining IS NOT NULL AND occ.remaining > 0 "
                + "AND held.zone_id IS NULL THEN"));
        assertTrue(cql.contains("SET remaining -= 1"));
        assertFalse(cql.contains("occ.capacity <"));
        assertFalse(cql.contains("< occ.capacity"));
    }

    /** Two reads guard three writes: the counter, the zone's side and the drone's. */
    @Test
    void theGrantReadsTwoPartitionsAndWritesThree() {
        String cql = Clearance.grantCql(KEYSPACE);

        assertEquals(2, count(cql, "LET "));
        assertEquals(2, count(cql, "INSERT INTO"));
        assertEquals(1, count(cql, "UPDATE "));
        assertTrue(cql.contains("SELECT occ.remaining, occ.capacity, held.zone_id;"));
    }

    /**
     * The caller names the zone, and that is the guard rather than a convenience.
     *
     * <p>A {@code LET} reference cannot appear in a write's {@code WHERE} clause, so the zone has to be
     * bound. Comparing it against {@code held.zone_id} then refuses a release of a clearance the drone
     * does not hold, and refuses a second release of one already given back, since a null compares
     * equal to nothing.
     */
    @Test
    void theReleaseComparesTheNamedZoneAgainstTheHeldOne() {
        String cql = Clearance.releaseCql(KEYSPACE);

        assertTrue(cql.contains("IF held.zone_id = ? THEN"));
        assertEquals(1, count(cql, "LET "));
        assertEquals(2, count(cql, "DELETE FROM"));
        assertEquals(1, count(cql, "SET remaining += 1"));
    }

    @Test
    void everyPlaceholderOfBothStatementsIsBound() {
        assertEquals(
                count(Clearance.grantCql(KEYSPACE), "?"),
                Clearance.grantParams("z", "e", AT).length);
        assertEquals(
                count(Clearance.releaseCql(KEYSPACE), "?"),
                Clearance.releaseParams("z", "e").length);
    }

    /** One instant for both sides of a clearance, so the zone's row and the drone's agree. */
    @Test
    void bothSidesOfAGrantCarryTheSameTimestamp() {
        Object[] values = Clearance.grantParams("z", "e", AT);

        assertEquals(AT, values[5]);
        assertEquals(AT, values[8]);
    }

    // ──────────────────────── Reading a refusal ────────────────────────

    @Test
    void anEmptyProjectionCannotBeRead() {
        assertEquals(
                "the transaction projected nothing, so its guards cannot be read",
                Clearance.grantRefusal("z", "e", Map.of()));
        assertEquals(
                "the transaction projected nothing, so its guards cannot be read",
                Clearance.releaseRefusal("z", "e", Map.of()));
    }

    @Test
    void aZoneWithNoOccupancyRowHasNoCapacityToDrawOn() {
        assertEquals(
                "zone-x has no occupancy row, so there is no capacity to draw on",
                Clearance.grantRefusal("zone-x", "asset-1", guards(null, null, null)));
    }

    /** A clearance already held is a more useful answer than a full zone, and comes first. */
    @Test
    void aDroneAlreadyClearedIsToldWhereItIsCleared() {
        assertEquals(
                "asset-1 already holds a clearance into zone-y",
                Clearance.grantRefusal("zone-x", "asset-1", guards(1L, 2L, "zone-y")));
    }

    /**
     * A full zone is decided by comparing a number, which is why the projection is read before it is
     * stringified: the text "0" is not falsy, and a full zone would report itself as having room.
     */
    @Test
    void aFullZoneNamesItsCapacity() {
        assertEquals(
                "zone-x is full: all 2 slots are held",
                Clearance.grantRefusal("zone-x", "asset-1", guards(0L, 2L, null)));
    }

    @Test
    void aSlotLeftAndNoClearanceHeldGivesNoReason() {
        assertEquals("", Clearance.grantRefusal("zone-x", "asset-1", guards(1L, 2L, null)));
    }

    @Test
    void nothingHeldMeansNothingToGiveBack() {
        assertEquals(
                "asset-1 holds no clearance, so there is nothing to give back",
                Clearance.releaseRefusal("zone-x", "asset-1", held(null)));
    }

    @Test
    void aClearanceIntoAnotherZoneCannotBeGivenBackHere() {
        assertEquals(
                "asset-1 holds a clearance into zone-y, not into zone-x",
                Clearance.releaseRefusal("zone-x", "asset-1", held("zone-y")));
    }

    @Test
    void theZoneItIsHeldForCanBeGivenBack() {
        assertEquals("", Clearance.releaseRefusal("zone-x", "asset-1", held("zone-x")));
    }

    // ──────────────────────── The ledger ────────────────────────

    /** Slots and holders adding up is the whole invariant, and the state reports it either way. */
    @Test
    void theInvariantIsReportedRatherThanAssumed() {
        Ledger ledger = ledger();
        ledger.grant(Clearance.DEMO_ZONE, "asset-000000");

        ClearanceState state = clearance.state();

        ClearanceZone airport = state.zone(Clearance.DEMO_ZONE);
        assertEquals(2, airport.capacity());
        assertEquals(1, airport.remaining());
        assertEquals(List.of("asset-000000"), airport.holders());
        assertTrue(airport.consistent());
        assertEquals(List.of(), state.mismatched());
    }

    /** A counter that moved without a holder appearing is a broken invariant, and it is shown. */
    @Test
    void aCounterOutOfStepWithItsHoldersIsReportedInconsistent() {
        Ledger ledger = ledger();
        ledger.zones.get(Clearance.DEMO_ZONE)[1] = 0;

        assertFalse(clearance.state().zone(Clearance.DEMO_ZONE).consistent());
    }

    /**
     * A holder whose own row names another zone is named, because one transaction writes both tables.
     *
     * <p>The check is one-directional, and deliberately: the other direction would need a scan of
     * {@code drone_clearance}, which has no partition to bound it.
     */
    @Test
    void aHolderWhoseOwnRowDisagreesIsNamed() {
        Ledger ledger = ledger();
        ledger.grant(Clearance.DEMO_ZONE, "asset-000000");
        ledger.droneSide.put("asset-000000", Clearance.DEMO_OTHER_ZONE);

        assertEquals(List.of("asset-000000"), clearance.state().mismatched());
    }

    /** Zones by identifier and holders within a zone likewise, so two reads compare row for row. */
    @Test
    void theLedgerIsOrdered() {
        Ledger ledger = ledger();
        ledger.grant(Clearance.DEMO_ZONE, "asset-000001");
        ledger.grant(Clearance.DEMO_ZONE, "asset-000000");

        ClearanceState state = clearance.state();

        assertEquals(
                state.zones().stream().map(ClearanceZone::zoneId).sorted().toList(),
                state.zones().stream().map(ClearanceZone::zoneId).toList());
        assertEquals(
                List.of("asset-000000", "asset-000001"),
                state.zone(Clearance.DEMO_ZONE).holders());
    }

    // ──────────────────────── The scripted sequence ────────────────────────

    /**
     * Seven steps, and only three of them may change anything.
     *
     * <p>Grant the first drone; replay that grant; ask for a second zone with one already held; grant
     * the second drone, taking the last slot; ask for a third, into a zone now full; release the first;
     * release it again.
     */
    @Test
    void onlyThreeOfTheSevenStepsChangeAnything() {
        ledger();

        ClearanceDemoResult result = clearance.demo(0);

        assertEquals(
                List.of(true, false, false, true, false, true, false),
                result.steps().stream().map(TransactionStep::applied).toList());
        assertTrue(result.steps().stream().allMatch(step -> step.state().consistent()),
                "the invariant broke part-way through the sequence");
        assertEquals(0, result.repeats());
        assertNull(result.grantP50Ms());
        assertNull(result.releaseP50Ms());
    }

    /** Each refusal names the guard that stopped it, which is what the sequence is for. */
    @Test
    void eachRefusedStepSaysWhyTheZoneDidNotMove() {
        ledger();

        List<TransactionStep> steps = clearance.demo(0).steps();

        assertEquals(
                "asset-000000 already holds a clearance into " + Clearance.DEMO_ZONE,
                steps.get(1).reason());
        assertEquals(
                "asset-000000 already holds a clearance into " + Clearance.DEMO_ZONE,
                steps.get(2).reason());
        assertEquals(Clearance.DEMO_ZONE + " is full: all 2 slots are held", steps.get(4).reason());
        assertEquals(
                "asset-000000 holds no clearance, so there is nothing to give back",
                steps.get(6).reason());
    }

    /** After all seven the zone holds one drone, and the slots still add up. */
    @Test
    void theSequenceLeavesOneHolderAndABalancedLedger() {
        ledger();

        ClearanceDemoResult result = clearance.demo(0);

        ClearanceZone airport = result.state().zone(Clearance.DEMO_ZONE);
        assertEquals(List.of("asset-000001"), airport.holders());
        assertEquals(1, airport.remaining());
        assertTrue(airport.consistent());
        assertEquals(List.of(), result.state().mismatched());
    }

    /**
     * The measurement pairs cycle a drone of their own through a zone the sequence never touches.
     *
     * <p>The last of the seven steps has to be the last thing that touched the airport zone, because
     * the state the demo returns is what the page shows.
     */
    @Test
    void theMeasuredPairsLeaveTheScriptedZoneAlone() {
        Ledger ledger = ledger();

        ClearanceDemoResult result = clearance.demo(3);

        assertEquals(3, result.repeats());
        assertEquals(List.of("asset-000001"), result.state().zone(Clearance.DEMO_ZONE).holders());
        assertEquals(List.of(), result.state().zone(Clearance.DEMO_MEASURE_ZONE).holders());
        assertFalse(ledger.droneSide.containsKey(Clearance.MEASURE_ASSET));
    }

    // ──────────────────────── Contention ────────────────────────

    /**
     * The claim the scripted steps cannot make: however many ask at once, the capacity is what gets in.
     *
     * <p>A count read and written back outside consensus would oversubscribe here, and the number would
     * say so.
     */
    @Test
    void manyAskersGetExactlyTheCapacity() {
        ledger();

        ClearanceContentionResult result = clearance.contend(Clearance.DEMO_ZONE, 8);

        assertEquals(2, result.capacity());
        assertEquals(2, result.granted());
        assertEquals(6, result.refused());
        assertEquals(8, result.askers());
        assertEquals(2, result.winners().size());
        assertEquals(List.of(), result.errors());
        assertTrue(result.zone().consistent());
    }

    /** Every clearance is given back first, so the zone starts full however the last run ended. */
    @Test
    void aContentionRunStartsFromAFullZone() {
        Ledger ledger = ledger();
        ledger.grant(Clearance.DEMO_ZONE, "asset-000042");

        assertEquals(2, clearance.contend(Clearance.DEMO_ZONE, 4).granted());
    }

    /**
     * A transaction that raised is not a loser.
     *
     * <p>A refusal is the expected outcome for a drone that missed the last slot; an error is not, and
     * conflating the two would let a broken node read as a working semaphore.
     */
    @Test
    void anAskThatRaisedIsCountedApartFromARefusal() {
        Ledger ledger = ledger();
        ledger.failEvery("asset-000001");

        ClearanceContentionResult result = clearance.contend(Clearance.DEMO_ZONE, 4);

        assertEquals(1, result.errors().size());
        assertTrue(result.errors().get(0).contains("asset-000001"));
        assertEquals(3, result.granted() + result.refused());
    }

    /** A zone the seed knows nothing about is a 404, not an empty semaphore. */
    @Test
    void anUnknownZoneIsRefusedOutright() {
        ledger();

        ApiException refused =
                assertThrows(ApiException.class, () -> clearance.contend("zone-nowhere", 4));

        assertEquals(404, refused.status());
    }

    // ──────────────────────── Reset ────────────────────────

    /**
     * Reset releases through the same transaction a caller would use.
     *
     * <p>Writing capacity back over remaining would repair a broken invariant by overwriting it, which
     * would hide exactly the failure the state reports.
     */
    @Test
    void resetGivesEveryClearanceBackOneTransactionAtATime() {
        Ledger ledger = ledger();
        ledger.grant(Clearance.DEMO_ZONE, "asset-000000");
        ledger.grant(Clearance.DEMO_OTHER_ZONE, "asset-000001");

        ClearanceResetResult result = clearance.reset();

        assertEquals(
                List.of(
                        "released asset-000000 from " + Clearance.DEMO_ZONE,
                        "released asset-000001 from " + Clearance.DEMO_OTHER_ZONE),
                result.actions());
        assertTrue(result.state().zones().stream()
                .allMatch(zone -> zone.holders().isEmpty() && zone.consistent()));
        assertTrue(node.transacted.stream().allMatch(cql -> cql.contains("BEGIN TRANSACTION")));
    }

    @Test
    void resettingAnEmptyLedgerDoesNothingAndSaysSo() {
        ledger();

        assertEquals(List.of(), clearance.reset().actions());
    }

    // ──────────────────────── The scripted node ────────────────────────

    /**
     * The three tables the semaphore spans, kept in memory.
     *
     * <p>It decides what the node stored and what the guards therefore projected; the class under test
     * decides what to report about it. So this is not a second copy of the refusal ladder.
     */
    private static final class Ledger {
        final Map<String, long[]> zones = new LinkedHashMap<>();
        final Map<String, String> names = new LinkedHashMap<>();
        /** The zone's own partition: who the tower thinks is inside. */
        final Map<String, Set<String>> zoneSide = new LinkedHashMap<>();
        /** The drone's own partition, kept apart so a test can make the two sides disagree. */
        final Map<String, String> droneSide = new LinkedHashMap<>();

        String failFor;

        synchronized Map<String, Object> grant(String zoneId, String entityId) {
            long[] counts = zones.get(zoneId);
            Map<String, Object> guards = new LinkedHashMap<>();
            guards.put("occ.remaining", counts == null ? null : counts[1]);
            guards.put("occ.capacity", counts == null ? null : counts[0]);
            guards.put("held.zone_id", droneSide.get(entityId));
            if (counts != null && counts[1] > 0 && !droneSide.containsKey(entityId)) {
                counts[1]--;
                inside(zoneId).add(entityId);
                droneSide.put(entityId, zoneId);
            }
            return guards;
        }

        synchronized Map<String, Object> release(String zoneId, String entityId) {
            Map<String, Object> guards = new LinkedHashMap<>();
            String held = droneSide.get(entityId);
            guards.put("held.zone_id", held);
            if (zoneId.equals(held)) {
                zones.get(zoneId)[1]++;
                inside(zoneId).remove(entityId);
                droneSide.remove(entityId);
            }
            return guards;
        }

        synchronized List<String> holders(String zoneId) {
            return List.copyOf(inside(zoneId));
        }

        /** Sorted, because {@code zone_clearance} answers a partition in clustering order. */
        private Set<String> inside(String zoneId) {
            return zoneSide.computeIfAbsent(zoneId, zone -> new TreeSet<>());
        }

        void failEvery(String entityId) {
            failFor = entityId;
        }
    }

    /** Three zones as the sink seeds them, and a node that answers from them. */
    private Ledger ledger() {
        Ledger ledger = new Ledger();
        ledger.zones.put(Clearance.DEMO_ZONE, new long[] {2, 2});
        ledger.names.put(Clearance.DEMO_ZONE, "Oslo Airport");
        ledger.zones.put(Clearance.DEMO_OTHER_ZONE, new long[] {1, 1});
        ledger.names.put(Clearance.DEMO_OTHER_ZONE, "Royal Palace");
        ledger.zones.put(Clearance.DEMO_MEASURE_ZONE, new long[] {8, 8});
        ledger.names.put(Clearance.DEMO_MEASURE_ZONE, "Fornebu");

        node.onTransact = (cql, values) -> {
            if (cql.contains("SET remaining -= 1")) {
                String zoneId = (String) values[0];
                String entityId = (String) values[1];
                if (entityId.equals(ledger.failFor)) {
                    throw new IllegalStateException("Cassandra timeout during write query");
                }
                return ledger.grant(zoneId, entityId);
            }
            return ledger.release((String) values[1], (String) values[0]);
        };
        node.onRead = (cql, values) -> {
            if (cql.contains("zone_occupancy")) {
                List<Row> rows = new ArrayList<>();
                ledger.zones.forEach((zoneId, counts) -> rows.add(TransactionFakes.zoneRow(
                        zoneId, ledger.names.get(zoneId), "restricted", counts[0], counts[1])));
                return rows;
            }
            if (cql.contains("zone_clearance")) {
                return ledger.holders((String) values[0]).stream()
                        .map(entityId -> TransactionFakes.row(Map.of("entity_id", entityId)))
                        .toList();
            }
            String held = ledger.droneSide.get((String) values[0]);
            return held == null ? List.of() : List.of(TransactionFakes.row(Map.of("zone_id", held)));
        };
        return ledger;
    }

    private static Map<String, Object> guards(Object remaining, Object capacity, Object held) {
        Map<String, Object> guards = new LinkedHashMap<>();
        guards.put("occ.remaining", remaining);
        guards.put("occ.capacity", capacity);
        guards.put("held.zone_id", held);
        return guards;
    }

    private static Map<String, Object> held(Object zoneId) {
        Map<String, Object> guards = new LinkedHashMap<>();
        guards.put("held.zone_id", zoneId);
        return guards;
    }

    private static int count(String text, String part) {
        int found = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + part.length())) {
            found++;
        }
        return found;
    }
}
