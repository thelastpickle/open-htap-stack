package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.ClearanceContentionResult;
import com.thelastpickle.htap.backend.api.dto.ClearanceDemoResult;
import com.thelastpickle.htap.backend.api.dto.ClearanceResetResult;
import com.thelastpickle.htap.backend.api.dto.ClearanceState;
import com.thelastpickle.htap.backend.api.dto.SessionOpened;
import com.thelastpickle.htap.backend.api.dto.SessionTimelineView;
import com.thelastpickle.htap.backend.api.dto.TransactionDemoResult;
import com.thelastpickle.htap.backend.api.dto.TransactionSchemaReport;
import com.thelastpickle.htap.backend.api.dto.TransactionStep;
import com.thelastpickle.htap.backend.transaction.Clearance;
import com.thelastpickle.htap.backend.transaction.SessionDemo;
import com.thelastpickle.htap.backend.transaction.TransactionGate;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.UUID;
import java.util.function.Supplier;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * Accord transactions: multi-partition conditional writes, and their references.
 *
 * <p>Two demonstrations, on two schemas, because they make two different claims. The session one is
 * exactly-once in-order delivery, which is the shape of almost every stream-to-projection problem: a
 * replayed event must not duplicate a row, and an event that arrives early must not leave a gap. The
 * airspace clearance one is admission control: a count that must never be oversubscribed, however many
 * callers ask at once.
 *
 * <p>Kept apart from {@link QueryResource}, which rejects every write keyword by design: that check is
 * what keeps the read console honest. A transaction is a write, so it needs its own routes rather than
 * a hole in that check.
 */
@Path("/api/transactions")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "transactions")
public class TransactionsResource {

    /**
     * The most repeats a caller may ask for.
     *
     * <p>High enough to make the point read say something: the probe reads four times a second, so a
     * run of 40 transactions at a few milliseconds each is over before it has taken three samples, and
     * roughly 200 repeats per second of run means about 2,000 for a ten-second window. The default is
     * deliberately not that, because the default run answers "what does one cost".
     */
    static final int MAX_REPEATS = 4000;

    /** The fleet the map draws, which is what bounds a contention run: each asker is a real asset. */
    static final int MAX_ASKERS = 100;

    private final SessionDemo sessions;
    private final Clearance clearance;
    private final TransactionGate gate;

    TransactionsResource(SessionDemo sessions, Clearance clearance, TransactionGate gate) {
        this.sessions = sessions;
        this.clearance = clearance;
        this.gate = gate;
    }

    // ──────────────────────── Session routes ────────────────────────

    /** Whether each table will accept a transaction, asked of the node rather than looked up. */
    @GET
    @Path("/session/schema")
    public TransactionSchemaReport schema() {
        return sessions.schema();
    }

    /** The projection for one session, as the transactions left it. */
    @GET
    @Path("/session/{user_id}/{session_id}")
    public SessionTimelineView timeline(
            @PathParam("user_id") String userId, @PathParam("session_id") String sessionId) {
        return unavailable(() -> sessions.view(userId, uuid(sessionId)));
    }

    /** Open a session, which is the guard every step below reads first. */
    @POST
    @Path("/session/open")
    public SessionOpened open(@QueryParam("user_id") String userId) {
        return unavailable(() -> sessions.open(userId));
    }

    /** Attempt one sequence number, so the page can drive the demo a step at a time. */
    @POST
    @Path("/session/step")
    public TransactionStep step(
            @QueryParam("user_id") String userId,
            @QueryParam("session_id") String sessionId,
            @QueryParam("seq") long seq,
            @QueryParam("event_type") @DefaultValue(SessionDemo.DEFAULT_EVENT_TYPE)
                    String eventType) {
        UUID parsed = uuid(sessionId);
        if (seq < 0) {
            throw new ApiException(422, "seq must not be negative");
        }
        return alone(() -> unavailable(() -> sessions.step(userId, parsed, seq, eventType)));
    }

    /** The whole scripted sequence, in one call, on a session of its own. */
    @POST
    @Path("/session/demo")
    public TransactionDemoResult sessionDemo(
            @QueryParam("repeats") @DefaultValue("" + SessionDemo.DEFAULT_REPEATS) int repeats,
            @QueryParam("probe") @DefaultValue("true") boolean probe) {
        return alone(() -> unavailable(() -> sessions.demo(repeats(repeats), probe)));
    }

    // ──────────────────────── Clearance routes ────────────────────────

    /** The ledger now: slots left, who holds them, and whether the two sides agree. */
    @GET
    @Path("/clearance/state")
    public ClearanceState clearanceState() {
        return unavailable(clearance::state);
    }

    /** Ask for one clearance, so the page can drive the demo a step at a time. */
    @POST
    @Path("/clearance/grant")
    public TransactionStep grant(
            @QueryParam("zone_id") String zoneId, @QueryParam("entity_id") String entityId) {
        return alone(() -> unavailable(() -> clearance.grant(zoneId, entityId)));
    }

    /** Give one clearance back, which is the only way a slot returns. */
    @POST
    @Path("/clearance/release")
    public TransactionStep release(
            @QueryParam("zone_id") String zoneId, @QueryParam("entity_id") String entityId) {
        return alone(() -> unavailable(() -> clearance.release(zoneId, entityId)));
    }

    /** Release every clearance, so a run starts from a full set of slots. */
    @POST
    @Path("/clearance/reset")
    public ClearanceResetResult reset() {
        return alone(() -> unavailable(clearance::reset));
    }

    /** Ask for one zone from many drones at once, and count how many got in. */
    @POST
    @Path("/clearance/contend")
    public ClearanceContentionResult contend(
            @QueryParam("zone_id") @DefaultValue(Clearance.DEMO_ZONE) String zoneId,
            @QueryParam("askers") @DefaultValue("16") int askers) {
        return alone(() -> unavailable(
                // Clamped, where FastAPI declared ge=2, le=100 and answered 422: two is the
                // fewest askers that can contend at all, and the ceiling is what the node was
                // measured against.
                () -> clearance.contend(zoneId, Math.clamp(askers, 2, MAX_ASKERS))));
    }

    /** The whole scripted clearance sequence, in one call, on the airport zone. */
    @POST
    @Path("/clearance/demo")
    public ClearanceDemoResult clearanceDemo(
            @QueryParam("repeats") @DefaultValue("" + SessionDemo.DEFAULT_REPEATS) int repeats) {
        return alone(() -> unavailable(() -> clearance.demo(repeats(repeats))));
    }

    // ──────────────────────── The two things every route does ────────────────────────

    /**
     * One demonstration at a time, refused rather than queued.
     *
     * <p>Here and not inside the two classes, so the refusal is one line in one place and a caller
     * reading the routes can see which of them take the gate.
     */
    private <T> T alone(Supplier<T> work) {
        if (!gate.tryEnter()) {
            throw new ApiException(409, "a transaction demo is already running");
        }
        try {
            return work.get();
        } finally {
            gate.leave();
        }
    }

    /**
     * A node that cannot answer is a 503 rather than a 500.
     *
     * <p>The page polls the state route, so an unreachable node has to be a status the fetch layer
     * treats as temporary. A refusal this class decided on passes through unchanged.
     */
    private static <T> T unavailable(Supplier<T> work) {
        try {
            return work.get();
        } catch (ApiException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new ApiException(503, String.valueOf(e.getMessage()));
        }
    }

    /**
     * Clamped rather than refused, because a repeat count decides how long a measurement runs and
     * nothing else: an out-of-range one has an obvious nearest answer, where an unparseable session
     * identifier does not.
     *
     * <p>A divergence from FastAPI, which declared {@code ge=0, le=4000} and answered 422. The line
     * this backend draws is what the value is for: a knob that sizes a measurement is clamped, and a
     * value that is part of the request's own data is refused. That is why {@code seq} and
     * {@code session_id} are refused a few lines up although a negative {@code seq} has an obvious
     * nearest answer: it names a step of the caller's own sequence, and guessing which step was meant
     * would write a row the caller did not ask for.
     */
    private static int repeats(int asked) {
        return Math.clamp(asked, 0, MAX_REPEATS);
    }

    private static UUID uuid(String sessionId) {
        try {
            return UUID.fromString(sessionId);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new ApiException(400, "session_id is not a UUID");
        }
    }
}
