package com.thelastpickle.htap.backend.transaction;

import com.datastax.oss.driver.api.core.cql.Row;
import com.thelastpickle.htap.backend.api.dto.ProbedImpact;
import com.thelastpickle.htap.backend.api.dto.SessionOpened;
import com.thelastpickle.htap.backend.api.dto.SessionTimelineView;
import com.thelastpickle.htap.backend.api.dto.TransactionDemoResult;
import com.thelastpickle.htap.backend.api.dto.TransactionSchemaReport;
import com.thelastpickle.htap.backend.api.dto.TransactionStep;
import com.thelastpickle.htap.backend.api.dto.TransactionTimelineRow;
import com.thelastpickle.htap.backend.query.Comparison;
import com.thelastpickle.htap.backend.query.OltpProbe;
import com.thelastpickle.htap.backend.query.OltpSampler;
import com.thelastpickle.htap.backend.support.Messages;
import com.thelastpickle.htap.common.TimeUuids;
import com.thelastpickle.htap.common.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.LongSupplier;

/**
 * Exactly-once, in-order delivery, as one conditional write across three partitions.
 *
 * <p>Three reads guard two writes: {@code sessions_open} says the session exists, {@code
 * session_seq_applied} is the record of which sequence numbers have been applied, and {@code
 * session_timeline} is the projection itself. The three have three different partition keys, which is
 * why no other path in this stack can express the write: a CQL batch is atomic without being
 * conditional, and a lightweight transaction conditions on one partition only.
 *
 * <p>The scripted sequence is the argument, so it drives the refusals as well as the successes and
 * shows the projection unchanged after each refusal.
 */
@ApplicationScoped
public class SessionDemo {

    /** The demo writes into a session of its own, so nothing it does collides with another run. */
    public static final String USER_PREFIX = "txn-demo";

    /**
     * How many times an applied transaction is repeated when a p50 is asked for.
     *
     * <p>Small, because each repeat is a real consensus round trip on a real node and the figure
     * wanted here is an order of magnitude rather than a benchmark.
     */
    public static final int DEFAULT_REPEATS = 20;

    /** The event type a step writes when the caller names none. */
    public static final String DEFAULT_EVENT_TYPE = "session.step";

    private static final String EMPTY_PAYLOAD = "{}";

    private final Accord accord;
    private final OltpSampler sampler;
    private final Clock clock;
    private final LongSupplier nanoClock;
    private final Duration baselineWindow;

    @Inject
    SessionDemo(Accord accord, OltpSampler sampler) {
        this(accord, sampler, Clock.systemUTC(), System::nanoTime, Comparison.BASELINE_WINDOW);
    }

    SessionDemo(
            Accord accord,
            OltpSampler sampler,
            Clock clock,
            LongSupplier nanoClock,
            Duration baselineWindow) {
        this.accord = accord;
        this.sampler = sampler;
        this.clock = clock;
        this.nanoClock = nanoClock;
        this.baselineWindow = baselineWindow;
    }

    // ──────────────────────── The statements ────────────────────────
    //
    // Every timeuuid and timestamp below is bound rather than generated in the statement. An
    // Accord transaction must be deterministic, so now() and toTimestamp(now()) are out: each
    // would be evaluated per replica, and the same transaction would then write different values
    // depending on who executed it.

    /**
     * The transaction that appends one event to a session's timeline.
     *
     * <p>Three reads guard two writes. {@code prev_ok} is left out for {@code seq=0}, which has no
     * predecessor; including it would make the first event of every session unappendable.
     */
    static String applyCql(String keyspace, long seq) {
        List<String> guards = new ArrayList<>();
        guards.add("LET session_ok = (SELECT session_id FROM " + keyspace + ".sessions_open "
                + "WHERE user_id = ? AND session_id = ?);");
        guards.add("LET already = (SELECT seq FROM " + keyspace + ".session_seq_applied "
                + "WHERE user_id = ? AND session_id = ? AND seq = ?);");
        String projection = "SELECT session_ok.session_id, already.seq";
        String condition = "session_ok IS NOT NULL AND already IS NULL";
        if (seq > 0) {
            guards.add("LET prev_ok = (SELECT seq FROM " + keyspace + ".session_seq_applied "
                    + "WHERE user_id = ? AND session_id = ? AND seq = ?);");
            projection = "SELECT session_ok.session_id, already.seq, prev_ok.seq";
            condition += " AND prev_ok IS NOT NULL";
        }
        return "BEGIN TRANSACTION\n  " + String.join("\n  ", guards)
                + "\n  " + projection + ";"
                + "\n  IF " + condition + " THEN"
                + "\n    INSERT INTO " + keyspace + ".session_timeline "
                + "(user_id, session_id, seq, event_id, event_time, event_type, payload) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?);"
                + "\n    INSERT INTO " + keyspace + ".session_seq_applied (user_id, session_id, seq) "
                + "VALUES (?, ?, ?);"
                + "\n  END IF\nCOMMIT TRANSACTION;";
    }

    /**
     * Values for {@link #applyCql}, in the order the statement reads them.
     *
     * <p>{@code seq} is bound as a {@code long} and not an {@code int}, because the column is a CQL
     * {@code bigint} and this driver looks a codec up by the column's type and the value's class:
     * there is none for {@code BIGINT} and {@code Integer}, and asking for one raises {@code
     * CodecNotFoundException}, which {@code SessionDemoTest} pins. The Python driver widened
     * silently.
     *
     * <p>One instant serves the timeuuid and the timestamp, so a row's identifier and its event time
     * agree; two clock readings could put them a millisecond apart and make a sorted timeline
     * disagree with itself.
     */
    static Object[] applyParams(
            String userId, UUID sessionId, long seq, String eventType, String payload, Instant at) {
        List<Object> values = new ArrayList<>(List.of(userId, sessionId, userId, sessionId, seq));
        if (seq > 0) {
            values.addAll(List.of(userId, sessionId, seq - 1));
        }
        values.addAll(List.of(
                userId, sessionId, seq, TimeUuids.timeUuid(at), at, eventType, payload));
        values.addAll(List.of(userId, sessionId, seq));
        return values.toArray();
    }

    /**
     * Why the {@code IF} did not fire, read out of the guards the transaction projected.
     *
     * <p>An Accord transaction returns no {@code [applied]} column, so this is the only way to tell
     * one refusal from another. The order matters: a caller wants to hear "already applied" rather
     * than "no predecessor" when both are true.
     */
    static String refusal(long seq, Map<String, Object> projection) {
        if (projection.isEmpty()) {
            return "the transaction projected nothing, so its guards cannot be read";
        }
        if (projection.get("session_ok.session_id") == null) {
            return "the session is not open";
        }
        if (projection.get("already.seq") != null) {
            return "seq=" + seq + " was already applied, so the replay changed nothing";
        }
        if (seq > 0 && projection.get("prev_ok.seq") == null) {
            return "seq=" + (seq - 1) + " has not been applied, so seq=" + seq
                    + " would leave a gap";
        }
        return "";
    }

    /**
     * The statement that opens a session, as the page shows it.
     *
     * <p>A plain {@code INSERT} with {@code ?} rather than the Python's {@code %s}: this driver binds
     * a positional marker on a simple statement too, so the statement the reader is shown is the
     * statement that ran.
     */
    static String openCql(String keyspace) {
        return "INSERT INTO " + keyspace + ".sessions_open (user_id, session_id) VALUES (?, ?)";
    }

    // ──────────────────────── What the routes call ────────────────────────

    /**
     * Whether each table will accept a transaction, asked rather than looked up.
     *
     * <p>There is no schema column to read. {@code transactional_mode} does not appear in {@code
     * system_schema.tables}, and nothing else there distinguishes a transactional table from a plain
     * one: {@code session_timeline} and its non-transactional twin have identical flags, extensions
     * and fast-path settings.
     *
     * <p>So this asks the node directly, with a transaction that reads one row of each table and
     * writes nothing, and reports the node's own answer. That is the better test in any case: what a
     * page needs to know is whether a transaction will run, and a table can be transactional and
     * still refuse one while its migration is incomplete.
     */
    public TransactionSchemaReport schema() {
        String keyspace = accord.keyspace();
        Map<String, String> answers = new LinkedHashMap<>();
        probes(keyspace).forEach((table, cql) -> {
            try {
                accord.transact(cql, "__schema_probe__", new UUID(0L, 0L));
                answers.put(table, TransactionSchemaReport.ACCEPTS);
            } catch (RuntimeException e) {
                answers.put(table, Messages.oneLine(e));
            }
        });
        return TransactionSchemaReport.of(keyspace, answers);
    }

    /**
     * One read-only transaction per table.
     *
     * <p>Each projects a named column rather than the whole {@code LET} reference: "SELECT probe" is
     * refused with "SELECT references must specify a column."
     */
    static Map<String, String> probes(String keyspace) {
        Map<String, String> reads = new LinkedHashMap<>();
        reads.put("sessions_open", probe(
                "(SELECT session_id FROM " + keyspace + ".sessions_open "
                        + "WHERE user_id = ? AND session_id = ?)",
                "session_id"));
        reads.put("session_seq_applied", probe(
                "(SELECT seq FROM " + keyspace + ".session_seq_applied "
                        + "WHERE user_id = ? AND session_id = ? AND seq = 0)",
                "seq"));
        reads.put("session_timeline", probe(
                "(SELECT seq FROM " + keyspace + ".session_timeline "
                        + "WHERE user_id = ? AND session_id = ? AND seq = 0)",
                "seq"));
        return reads;
    }

    private static String probe(String read, String column) {
        return "BEGIN TRANSACTION\n  LET probe = " + read + ";\n  SELECT probe." + column + ";"
                + "\nCOMMIT TRANSACTION;";
    }

    /** The projection for one session, which is one bounded single-partition read. */
    public SessionTimelineView view(String userId, UUID sessionId) {
        return new SessionTimelineView(userId, sessionId.toString(), timeline(userId, sessionId));
    }

    /** Opens a session, which is the guard every step reads first. */
    public SessionOpened open(String named) {
        UUID sessionId = UUID.randomUUID();
        String userId = named == null || named.isBlank() ? demoUser(USER_PREFIX, sessionId) : named;
        openSession(userId, sessionId);
        return new SessionOpened(userId, sessionId.toString());
    }

    /** Attempts one sequence number, so the page can drive the demo a step at a time. */
    public TransactionStep step(String userId, UUID sessionId, long seq, String eventType) {
        return runStep("apply seq=" + seq, userId, sessionId, seq, eventType);
    }

    /**
     * The whole scripted sequence, in one call, on a session of its own.
     *
     * <p>Six steps in this order, because the order is the argument: apply seq=0, replay it, attempt
     * seq=2 before seq=1, apply seq=1, then apply seq=2. Steps two and three must leave the timeline
     * exactly as they found it, and the last must succeed only once its predecessor exists.
     */
    public TransactionDemoResult demo(int repeats, boolean probe) {
        UUID sessionId = UUID.randomUUID();
        String userId = demoUser(USER_PREFIX, sessionId);
        Optional<String> subject = probe ? sampler.subject() : Optional.empty();

        List<TransactionStep> steps;
        List<Double> appliedMs;
        ProbedImpact idle = null;
        ProbedImpact during = null;
        if (subject.isPresent()) {
            // The same idle window the comparison page takes, and for the same reason: a probe
            // figure on its own says nothing, because it is the difference from idle that shows
            // what the work cost the request path.
            try (OltpProbe before = sampler.sample(subject.get())) {
                sleep(baselineWindow);
                idle = ProbedImpact.of(before.impact(), null);
            }
            try (OltpProbe alongside = sampler.sample(subject.get())) {
                steps = sequence(userId, sessionId);
                appliedMs = repeatApplied(repeats);
                during = ProbedImpact.of(alongside.impact(), subject.get());
            }
        } else {
            steps = sequence(userId, sessionId);
            appliedMs = repeatApplied(repeats);
        }

        List<TransactionTimelineRow> timeline = timeline(userId, sessionId);
        return new TransactionDemoResult(
                userId,
                sessionId.toString(),
                steps,
                timeline,
                referenceMs(userId, sessionId, repeats),
                appliedMs.size(),
                Latencies.p50(appliedMs),
                Latencies.max(appliedMs),
                during,
                idle);
    }

    private List<TransactionStep> sequence(String userId, UUID sessionId) {
        List<TransactionStep> steps = new ArrayList<>();
        long began = nanoClock.getAsLong();
        openSession(userId, sessionId);
        steps.add(TransactionStep.opened(openCql(accord.keyspace()), elapsedMs(began)));
        steps.add(runStep("apply seq=0", userId, sessionId, 0, DEFAULT_EVENT_TYPE));
        steps.add(runStep("replay seq=0", userId, sessionId, 0, DEFAULT_EVENT_TYPE));
        steps.add(runStep("attempt seq=2 out of order", userId, sessionId, 2, DEFAULT_EVENT_TYPE));
        steps.add(runStep("apply seq=1", userId, sessionId, 1, DEFAULT_EVENT_TYPE));
        steps.add(runStep("apply seq=2", userId, sessionId, 2, DEFAULT_EVENT_TYPE));
        return steps;
    }

    /** Runs one transaction, and reports what it did without asking twice. */
    private TransactionStep runStep(
            String action, String userId, UUID sessionId, long seq, String eventType) {
        String cql = applyCql(accord.keyspace(), seq);
        Object[] values =
                applyParams(userId, sessionId, seq, eventType, EMPTY_PAYLOAD, clock.instant());
        long began = nanoClock.getAsLong();
        Map<String, Object> projection;
        try {
            projection = accord.transact(cql, values);
        } catch (RuntimeException e) {
            return TransactionStep.failed(action, cql, elapsedMs(began), Messages.oneLine(e),
                    timeline(userId, sessionId).size());
        }
        double durationMs = elapsedMs(began);
        return TransactionStep.session(action, cql, refusal(seq, projection), projection, durationMs,
                timeline(userId, sessionId).size());
    }

    /**
     * Latencies of transactions that all applied, for a p50 and a maximum.
     *
     * <p>In a session of its own, not the one the six steps illustrate: putting forty repeats into
     * that session would bury its three-row story under forty more rows.
     *
     * <p>Every repeat takes the three-guard path with a predecessor to find, because seq=0 has one
     * guard fewer and timing it would flatter the figure.
     */
    private List<Double> repeatApplied(int repeats) {
        if (repeats <= 0) {
            return List.of();
        }
        UUID sessionId = UUID.randomUUID();
        String userId = demoUser(USER_PREFIX + "-measure", sessionId);
        openSession(userId, sessionId);
        runStep("apply seq=0", userId, sessionId, 0, DEFAULT_EVENT_TYPE);
        List<Double> latencies = new ArrayList<>(repeats);
        for (long seq = 1; seq <= repeats; seq++) {
            TransactionStep step =
                    runStep("apply seq=" + seq, userId, sessionId, seq, DEFAULT_EVENT_TYPE);
            if (step.applied()) {
                latencies.add(step.durationMs());
            }
        }
        return latencies;
    }

    /**
     * The same row written two other ways, on a non-transactional twin table.
     *
     * <p>Same columns, same key and the same QUORUM as the transaction, so what is compared is the
     * write path and not two table definitions or two consistency levels. Repeated as often as the
     * transaction and reported as a p50, because a single sample against a twenty-run p50 is not a
     * comparison; nothing is measured at all when the caller asks for no repeats, which the CI step
     * does.
     *
     * <p>A distinct sequence number per repeat, so the lightweight transaction's {@code IF NOT
     * EXISTS} finds nothing and takes its applied path every time. Reusing one key would make every
     * repeat after the first a rejection, which is a different and cheaper operation.
     */
    private Map<String, Double> referenceMs(String userId, UUID sessionId, int repeats) {
        Map<String, Double> figures = new LinkedHashMap<>();
        if (repeats <= 0) {
            return figures;
        }
        reference(figures, "plain_insert", "", 100_000, userId, sessionId, repeats);
        reference(figures, "lwt_if_not_exists", " IF NOT EXISTS", 200_000, userId, sessionId,
                repeats);
        return figures;
    }

    private void reference(
            Map<String, Double> figures,
            String label,
            String suffix,
            long base,
            String userId,
            UUID sessionId,
            int repeats) {
        String cql = "INSERT INTO " + accord.keyspace() + ".session_timeline_plain "
                + "(user_id, session_id, seq, event_id, event_time, event_type, payload) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)" + suffix;
        List<Double> samples = new ArrayList<>(repeats);
        for (int index = 0; index < repeats; index++) {
            Instant at = clock.instant();
            long began = nanoClock.getAsLong();
            try {
                accord.write(cql, userId, sessionId, base + index, TimeUuids.timeUuid(at), at,
                        "reference", EMPTY_PAYLOAD);
                samples.add(elapsedMs(began));
            } catch (RuntimeException e) {
                // A reference that failed is left out rather than reported as zero: a zero here
                // would read as "faster than everything", which is the one thing it cannot mean.
            }
        }
        if (!samples.isEmpty()) {
            figures.put(label + "_p50_ms", Latencies.p50(samples));
            figures.put(label + "_max_ms", Latencies.max(samples));
        }
    }

    /**
     * The projection itself: one bounded single-partition read, at QUORUM.
     *
     * <p>QUORUM because {@code transactional_mode='full'} routes a table's ordinary reads through
     * Accord as well as its writes. Worth knowing before opting any table in: had {@code demo.events}
     * taken the option, every read path on the dashboard would have started failing this way rather
     * than merely slowing down.
     */
    private List<TransactionTimelineRow> timeline(String userId, UUID sessionId) {
        List<Row> rows = accord.read(
                "SELECT seq, event_id, event_time, event_type, payload FROM "
                        + accord.keyspace() + ".session_timeline "
                        + "WHERE user_id = ? AND session_id = ?",
                userId, sessionId);
        List<TransactionTimelineRow> timeline = new ArrayList<>(rows.size());
        for (Row row : rows) {
            Instant at = row.getInstant("event_time");
            timeline.add(new TransactionTimelineRow(
                    row.getLong("seq"),
                    String.valueOf(row.getUuid("event_id")),
                    at == null ? "" : Timestamps.iso(at),
                    row.getString("event_type"),
                    row.getString("payload")));
        }
        return timeline;
    }

    private void openSession(String userId, UUID sessionId) {
        accord.write(openCql(accord.keyspace()), userId, sessionId);
    }

    /** The demo's own user, named after the session so two runs cannot share one. */
    static String demoUser(String prefix, UUID sessionId) {
        return prefix + "-" + sessionId.toString().substring(0, 8);
    }

    private double elapsedMs(long began) {
        return (nanoClock.getAsLong() - began) / 1e6;
    }

    private static void sleep(Duration window) {
        try {
            Thread.sleep(window);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
