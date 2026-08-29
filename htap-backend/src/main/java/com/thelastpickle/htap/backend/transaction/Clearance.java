package com.thelastpickle.htap.backend.transaction;

import com.datastax.oss.driver.api.core.cql.Row;
import com.thelastpickle.htap.backend.api.ApiException;
import com.thelastpickle.htap.backend.api.dto.ClearanceContentionResult;
import com.thelastpickle.htap.backend.api.dto.ClearanceDemoResult;
import com.thelastpickle.htap.backend.api.dto.ClearanceResetResult;
import com.thelastpickle.htap.backend.api.dto.ClearanceState;
import com.thelastpickle.htap.backend.api.dto.ClearanceZone;
import com.thelastpickle.htap.backend.api.dto.TransactionStep;
import com.thelastpickle.htap.backend.support.Messages;
import com.thelastpickle.htap.backend.support.Round;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;

/**
 * A distributed semaphore: a restricted zone admits a fixed number of drones at once.
 *
 * <p>A clearance is recorded twice, once under the zone so the tower can list who is inside, and once
 * under the drone so a grant can be refused without scanning every zone. With the occupancy counter
 * that is three partition keys, and one grant has to move all three or none of them.
 *
 * <p>What no other path here can express: a lightweight transaction conditions on one partition, and
 * a CQL batch is atomic without being conditional, so neither can say "admit this drone only if the
 * zone has a slot left and the drone holds no clearance already". The invariant that check protects
 * is {@code capacity == remaining + holders}, and {@link #state} reports whether it still holds
 * rather than assuming it.
 *
 * <p>The zone is a real one from the map and the drones are real fleet assets, but the clearance
 * itself is the demo's own: nothing in ingest reads these tables, and no drone's telemetry changes
 * because a clearance was granted or refused.
 */
@ApplicationScoped
public class Clearance {

    /**
     * The zone the scripted steps fill.
     *
     * <p>Capacity 2 in the sink's seed, so two grants exhaust it and the third is refused for the
     * reason the demo exists to show. A larger zone would need more steps to reach the same point.
     */
    public static final String DEMO_ZONE = "zone-oslo-airport";

    /** Three drones the map is already showing: the first two fill the zone, the third finds it full. */
    public static final List<String> DEMO_ASSETS =
            List.of("asset-000000", "asset-000001", "asset-000002");

    /** A second zone, for the step that shows one drone cannot hold two clearances. */
    public static final String DEMO_OTHER_ZONE = "zone-royal-palace";

    /**
     * Where the repeated grant and release pairs are timed.
     *
     * <p>Fornebu, the widest zone in the seed, so a repeat cannot exhaust it even if one release were
     * refused; and neither of the two zones the scripted steps touch, because the state the page shows
     * must be what those steps left rather than what a measurement loop did afterwards.
     */
    public static final String DEMO_MEASURE_ZONE = "zone-fornebu";

    /** A drone of its own, outside the fleet the map draws, so a run leaves no clearance on it. */
    static final String MEASURE_ASSET = "asset-measure";

    private final Accord accord;
    private final Clock clock;
    private final LongSupplier nanoClock;

    @Inject
    Clearance(Accord accord) {
        this(accord, Clock.systemUTC(), System::nanoTime);
    }

    Clearance(Accord accord, Clock clock, LongSupplier nanoClock) {
        this.accord = accord;
        this.clock = clock;
        this.nanoClock = nanoClock;
    }

    // ──────────────────────── The statements ────────────────────────

    /**
     * Grant a clearance: two reads guard a decrement and two inserts.
     *
     * <p>{@code remaining > 0} and not {@code granted < capacity} because Accord will not compare two
     * {@code LET} references to each other: {@code IF occ.granted < occ.capacity} is refused with a
     * {@code SyntaxException}. Counting down needs only a reference and a literal, and {@code SET
     * remaining -= 1} is then the whole read-modify-write, done inside consensus rather than by
     * reading a value and writing it back.
     */
    static String grantCql(String keyspace) {
        return "BEGIN TRANSACTION\n"
                + "  LET occ = (SELECT capacity, remaining FROM " + keyspace + ".zone_occupancy "
                + "WHERE zone_id = ?);\n"
                + "  LET held = (SELECT zone_id FROM " + keyspace + ".drone_clearance "
                + "WHERE entity_id = ?);\n"
                + "  SELECT occ.remaining, occ.capacity, held.zone_id;\n"
                + "  IF occ.remaining IS NOT NULL AND occ.remaining > 0 AND held.zone_id IS NULL "
                + "THEN\n"
                + "    UPDATE " + keyspace + ".zone_occupancy SET remaining -= 1 WHERE zone_id = ?;\n"
                + "    INSERT INTO " + keyspace + ".zone_clearance (zone_id, entity_id, granted_at) "
                + "VALUES (?, ?, ?);\n"
                + "    INSERT INTO " + keyspace + ".drone_clearance (entity_id, zone_id, granted_at) "
                + "VALUES (?, ?, ?);\n"
                + "  END IF\n"
                + "COMMIT TRANSACTION;";
    }

    /**
     * Release a clearance: one read guards an increment and two deletes.
     *
     * <p>The caller names the zone rather than the transaction reading it out of {@code held}, because
     * a {@code LET} reference cannot appear in a write's {@code WHERE} clause. Naming it costs nothing
     * and buys the guard: {@code held.zone_id = ?} refuses a release of a clearance the drone does not
     * hold, and refuses a second release of one already given back, since a null compares equal to
     * nothing.
     */
    static String releaseCql(String keyspace) {
        return "BEGIN TRANSACTION\n"
                + "  LET held = (SELECT zone_id FROM " + keyspace + ".drone_clearance "
                + "WHERE entity_id = ?);\n"
                + "  SELECT held.zone_id;\n"
                + "  IF held.zone_id = ? THEN\n"
                + "    UPDATE " + keyspace + ".zone_occupancy SET remaining += 1 WHERE zone_id = ?;\n"
                + "    DELETE FROM " + keyspace + ".zone_clearance WHERE zone_id = ? AND entity_id = ?;\n"
                + "    DELETE FROM " + keyspace + ".drone_clearance WHERE entity_id = ?;\n"
                + "  END IF\n"
                + "COMMIT TRANSACTION;";
    }

    /** Values for {@link #grantCql}, in the order the statement reads them. */
    static Object[] grantParams(String zoneId, String entityId, Instant grantedAt) {
        return new Object[] {
            zoneId, entityId, zoneId, zoneId, entityId, grantedAt, entityId, zoneId, grantedAt
        };
    }

    /** Values for {@link #releaseCql}, in the order the statement reads them. */
    static Object[] releaseParams(String zoneId, String entityId) {
        return new Object[] {entityId, zoneId, zoneId, zoneId, entityId, entityId};
    }

    /**
     * Why a grant did not fire, read out of the guards it projected.
     *
     * <p>Ordered by what a caller most wants to hear. An unknown zone leaves nothing else to say; a
     * drone that already holds a clearance is a more useful answer than a full zone, because it is
     * true of that drone wherever it asks.
     */
    static String grantRefusal(String zoneId, String entityId, Map<String, Object> projection) {
        if (projection.isEmpty()) {
            return "the transaction projected nothing, so its guards cannot be read";
        }
        Object remaining = projection.get("occ.remaining");
        if (remaining == null) {
            return zoneId + " has no occupancy row, so there is no capacity to draw on";
        }
        Object held = projection.get("held.zone_id");
        if (held != null) {
            return entityId + " already holds a clearance into " + held;
        }
        if (((Number) remaining).longValue() <= 0) {
            return zoneId + " is full: all " + projection.get("occ.capacity") + " slots are held";
        }
        return "";
    }

    static String releaseRefusal(String zoneId, String entityId, Map<String, Object> projection) {
        if (projection.isEmpty()) {
            return "the transaction projected nothing, so its guards cannot be read";
        }
        Object held = projection.get("held.zone_id");
        if (held == null) {
            return entityId + " holds no clearance, so there is nothing to give back";
        }
        if (!held.toString().equals(zoneId)) {
            return entityId + " holds a clearance into " + held + ", not into " + zoneId;
        }
        return "";
    }

    // ──────────────────────── What the routes call ────────────────────────

    /**
     * The ledger, read from both sides, and whether the two agree.
     *
     * <p>One read per zone plus one per holder, all of them bounded: {@code zone_occupancy} holds one
     * row per zone and {@code zone_clearance} is read one partition at a time, capacity rows at most.
     * The mirror check is a point read per holder rather than a scan of {@code drone_clearance}, which
     * has no partition to bound it.
     *
     * <p>It checks one direction only. A {@code drone_clearance} row whose zone-side twin is missing
     * would not be found here, and finding it would cost the scan this read exists to avoid; the state
     * route says so rather than implying a full audit.
     */
    public ClearanceState state() {
        String keyspace = accord.keyspace();
        List<ClearanceZone> zones = new ArrayList<>();
        TreeSet<String> mismatched = new TreeSet<>();
        List<Row> occupancy = accord.read("SELECT zone_id, zone_name, severity, capacity, remaining "
                + "FROM " + keyspace + ".zone_occupancy");
        for (Row row : occupancy) {
            String zoneId = row.getString("zone_id");
            List<String> holders = accord
                    .read("SELECT entity_id FROM " + keyspace + ".zone_clearance WHERE zone_id = ?",
                            zoneId)
                    .stream()
                    .map(held -> held.getString("entity_id"))
                    .sorted()
                    .toList();
            for (String entityId : holders) {
                List<Row> mirror = accord.read(
                        "SELECT zone_id FROM " + keyspace + ".drone_clearance WHERE entity_id = ?",
                        entityId);
                if (mirror.isEmpty() || !zoneId.equals(mirror.get(0).getString("zone_id"))) {
                    mismatched.add(entityId);
                }
            }
            zones.add(ClearanceZone.of(
                    zoneId,
                    text(row.getString("zone_name")),
                    text(row.getString("severity")),
                    row.getLong("capacity"),
                    row.getLong("remaining"),
                    holders));
        }
        zones.sort(Comparator.comparing(ClearanceZone::zoneId));
        return new ClearanceState(zones, List.copyOf(mismatched));
    }

    /** Ask for one clearance, so the page can drive the demo a step at a time. */
    public TransactionStep grant(String zoneId, String entityId) {
        return runGrant("grant " + entityId + " into " + zoneId, zoneId, entityId, true);
    }

    /** Give one clearance back, which is the only way a slot returns. */
    public TransactionStep release(String zoneId, String entityId) {
        return runRelease("release " + entityId + " from " + zoneId, zoneId, entityId, true);
    }

    /**
     * Release every clearance, so a run starts from a full set of slots.
     *
     * <p>An interrupted run would otherwise leave a zone permanently short, and the next run's first
     * grant would fail for a reason that has nothing to do with what it means to show. Each row is
     * released by the same transaction a caller would use, rather than by writing capacity back over
     * remaining: a reset that repaired a broken invariant by overwriting it would hide exactly the
     * failure {@link #state} reports.
     */
    public ClearanceResetResult reset() {
        List<String> actions = releaseEverything();
        return new ClearanceResetResult(actions, state());
    }

    private List<String> releaseEverything() {
        List<String> actions = new ArrayList<>();
        for (ClearanceZone zone : state().zones()) {
            for (String entityId : zone.holders()) {
                TransactionStep step =
                        runRelease("release " + entityId, zone.zoneId(), entityId, true);
                actions.add(step.applied()
                        ? "released " + entityId + " from " + zone.zoneId()
                        : "could not release " + entityId + " from " + zone.zoneId() + ": "
                                + (step.reason() == null || step.reason().isEmpty()
                                        ? step.error() : step.reason()));
            }
        }
        return actions;
    }

    /**
     * Ask for one zone from many drones at once, and count how many got in.
     *
     * <p>The demonstration the seven scripted steps cannot make on their own: those run one after
     * another, so nothing they show rules out a count read and written back outside consensus. This
     * overlaps the asks, and the answer has to be the zone's capacity exactly.
     *
     * <p>Each ask is a real fleet asset, and every clearance is released first so the zone starts
     * full. A virtual thread per asker, because each transaction blocks on consensus and the driver's
     * session is safe to share; {@code invokeAll} submits every ask before any result is read, which
     * is what makes the contention genuine rather than a sequence of asks arriving one at a time.
     */
    public ClearanceContentionResult contend(String zoneId, int askers) {
        releaseEverything();
        ClearanceZone before = state().zone(zoneId);
        if (before == null) {
            throw new ApiException(404, zoneId + " has no occupancy row");
        }
        List<Callable<TransactionStep>> asks = new ArrayList<>(askers);
        for (int index = 0; index < askers; index++) {
            String entityId = "asset-%06d".formatted(index);
            asks.add(() -> runGrant("grant " + entityId, zoneId, entityId, false));
        }
        long began = nanoClock.getAsLong();
        List<TransactionStep> steps = allOf(asks);
        double durationMs = Round.places(elapsedMs(began), 2);

        ClearanceZone after = state().zone(zoneId);
        List<String> errors = steps.stream()
                .filter(step -> step.error() != null)
                .map(step -> step.action() + ": " + step.error())
                .toList();
        return new ClearanceContentionResult(
                zoneId,
                before.capacity(),
                askers,
                (int) steps.stream().filter(TransactionStep::applied).count(),
                (int) steps.stream()
                        .filter(step -> !step.applied() && step.error() == null)
                        .count(),
                after == null ? List.of() : after.holders(),
                errors,
                durationMs,
                after);
    }

    /**
     * The whole scripted sequence, in one call, on the airport zone.
     *
     * <p>Seven steps, and the order is the argument. Grant the first drone; replay that grant; ask for
     * a second zone with one already held; grant the second drone, taking the last slot; ask for a
     * third, into a zone now full; release the first; release it again. Only three of the seven may
     * change anything, and after all seven the zone's slots and its holders must still add up to its
     * capacity.
     */
    public ClearanceDemoResult demo(int repeats) {
        // Start from a full set of slots. A previous run interrupted halfway would otherwise leave
        // the first grant refused for the wrong reason.
        releaseEverything();
        String first = DEMO_ASSETS.get(0);
        String second = DEMO_ASSETS.get(1);
        String third = DEMO_ASSETS.get(2);
        List<TransactionStep> steps = List.of(
                runGrant("grant " + first + " into " + DEMO_ZONE, DEMO_ZONE, first, true),
                runGrant("replay the grant of " + first, DEMO_ZONE, first, true),
                runGrant("grant " + first + " a second clearance, into " + DEMO_OTHER_ZONE,
                        DEMO_OTHER_ZONE, first, true),
                runGrant("grant " + second + " the last slot", DEMO_ZONE, second, true),
                runGrant("grant " + third + " into a full zone", DEMO_ZONE, third, true),
                runRelease("release " + first, DEMO_ZONE, first, true),
                runRelease("release " + first + " again", DEMO_ZONE, first, true));

        List<Double> grantMs = new ArrayList<>();
        List<Double> releaseMs = new ArrayList<>();
        repeatClearance(repeats, grantMs, releaseMs);
        return new ClearanceDemoResult(
                DEMO_ZONE,
                DEMO_ASSETS,
                steps,
                state(),
                grantMs.size(),
                Latencies.p50(grantMs),
                Latencies.max(grantMs),
                Latencies.p50(releaseMs),
                Latencies.max(releaseMs));
    }

    /**
     * Time a grant and a release over repeats, both on their applied path.
     *
     * <p>One drone cycles in and out of the quietest zone, so every grant finds a slot and every
     * release finds a clearance to give back; a repeat that was refused would time a cheaper
     * transaction and flatter the figure.
     *
     * <p>Not the airport zone the seven steps use, because the last of those steps must be the last
     * thing that touched it: the state the demo returns is what the page shows.
     */
    private void repeatClearance(int repeats, List<Double> grantMs, List<Double> releaseMs) {
        for (int index = 0; index < repeats; index++) {
            TransactionStep granted =
                    runGrant("measure grant", DEMO_MEASURE_ZONE, MEASURE_ASSET, false);
            if (granted.applied()) {
                grantMs.add(granted.durationMs());
            }
            TransactionStep released =
                    runRelease("measure release", DEMO_MEASURE_ZONE, MEASURE_ASSET, false);
            if (released.applied()) {
                releaseMs.add(released.durationMs());
            }
        }
    }

    // ──────────────────────── Running one transaction ────────────────────────

    /** What reads a refusal out of a projection, so one step runner serves both statements. */
    private interface Refusal {
        String of(String zoneId, String entityId, Map<String, Object> projection);
    }

    private TransactionStep runGrant(
            String action, String zoneId, String entityId, boolean withState) {
        return clearanceStep(
                action,
                grantCql(accord.keyspace()),
                grantParams(zoneId, entityId, clock.instant()),
                zoneId,
                entityId,
                Clearance::grantRefusal,
                withState);
    }

    private TransactionStep runRelease(
            String action, String zoneId, String entityId, boolean withState) {
        return clearanceStep(
                action,
                releaseCql(accord.keyspace()),
                releaseParams(zoneId, entityId),
                zoneId,
                entityId,
                Clearance::releaseRefusal,
                withState);
    }

    /**
     * Runs one clearance transaction and reports what it did to the zone.
     *
     * <p>The step's state carries the zone's slots and holders afterwards, which is this
     * demonstration's counterpart of the session one's timeline row count: a refused step is only
     * convincing if the reader can see the number it did not move. Reading it costs several bounded
     * reads, so the measurement loop asks for none: it reports latencies and no state, and those reads
     * would slow the loop without being reported.
     */
    private TransactionStep clearanceStep(
            String action,
            String cql,
            Object[] values,
            String zoneId,
            String entityId,
            Refusal refusal,
            boolean withState) {
        long began = nanoClock.getAsLong();
        Map<String, Object> projection;
        try {
            projection = accord.transact(cql, values);
        } catch (RuntimeException e) {
            return TransactionStep.failed(action, cql, elapsedMs(began), Messages.oneLine(e), 0);
        }
        double durationMs = elapsedMs(began);
        // Read from the raw projection, before the values are stringified for the response:
        // remaining is compared as a number, and the string "0" is not falsy.
        String reason = refusal.of(zoneId, entityId, projection);
        ClearanceZone zone = withState ? state().zone(zoneId) : null;
        return TransactionStep.clearance(action, cql, reason, projection, durationMs, zone);
    }

    /** Every ask submitted before any answer is read, so the asks genuinely overlap. */
    private static List<TransactionStep> allOf(List<Callable<TransactionStep>> asks) {
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<TransactionStep> steps = new ArrayList<>(asks.size());
            for (Future<TransactionStep> ask : pool.invokeAll(asks)) {
                steps.add(ask.get());
            }
            return steps;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("the contention run was interrupted", e);
        } catch (ExecutionException e) {
            // Nothing an ask does raises: clearanceStep answers a failure as a step. So this is a
            // fault in this class rather than a transaction the node refused.
            throw new IllegalStateException("an ask failed outside its own transaction", e);
        }
    }

    private double elapsedMs(long began) {
        return (nanoClock.getAsLong() - began) / 1e6;
    }

    private static String text(String value) {
        return value == null ? "" : value;
    }
}
