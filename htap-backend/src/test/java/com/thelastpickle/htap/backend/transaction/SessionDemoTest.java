package com.thelastpickle.htap.backend.transaction;

import static com.thelastpickle.htap.backend.transaction.TransactionFakes.KEYSPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.thelastpickle.htap.backend.api.dto.TransactionDemoResult;
import com.thelastpickle.htap.backend.api.dto.TransactionSchemaReport;
import com.thelastpickle.htap.backend.api.dto.TransactionStep;
import com.thelastpickle.htap.backend.transaction.TransactionFakes.FakeAccord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** The statements the session demonstration sends, and what it makes of the answers. */
class SessionDemoTest {

    private static final UUID SESSION = UUID.fromString("6bd0c9f4-1c9a-4a2e-8f5c-6d1f2a3b4c5d");
    private static final Instant AT = Instant.parse("2026-08-29T12:00:00Z");

    private final FakeAccord node = new FakeAccord();
    private final AtomicLong nanos = new AtomicLong();
    private final SessionDemo demo = new SessionDemo(
            node,
            TransactionFakes.noSampler(),
            Clock.fixed(AT, ZoneOffset.UTC),
            () -> nanos.addAndGet(1_500_000),
            Duration.ZERO);

    // ──────────────────────── The statement ────────────────────────

    /** seq=0 has no predecessor, so the third guard is left out rather than left unsatisfiable. */
    @Test
    void theFirstSequenceNumberIsGuardedTwice() {
        String cql = SessionDemo.applyCql(KEYSPACE, 0);

        assertFalse(cql.contains("prev_ok"));
        assertTrue(cql.contains("SELECT session_ok.session_id, already.seq;"));
        assertTrue(cql.contains("IF session_ok IS NOT NULL AND already IS NULL THEN"));
    }

    /** Every later one is guarded three times, and the projection names the third guard. */
    @Test
    void aLaterSequenceNumberIsGuardedThreeTimes() {
        String cql = SessionDemo.applyCql(KEYSPACE, 1);

        assertTrue(cql.contains("LET prev_ok = (SELECT seq FROM demo.session_seq_applied"));
        assertTrue(cql.contains("SELECT session_ok.session_id, already.seq, prev_ok.seq;"));
        assertTrue(cql.contains(
                "IF session_ok IS NOT NULL AND already IS NULL AND prev_ok IS NOT NULL THEN"));
    }

    /** Three reads and two writes, whatever the sequence number: one transaction, five statements. */
    @Test
    void theTransactionReadsThreeTablesAndWritesTwo() {
        String cql = SessionDemo.applyCql(KEYSPACE, 3);

        assertEquals(1, count(cql, "BEGIN TRANSACTION"));
        assertEquals(1, count(cql, "COMMIT TRANSACTION;"));
        assertEquals(3, count(cql, "LET "));
        assertEquals(2, count(cql, "INSERT INTO"));
    }

    // ──────────────────────── The bindings ────────────────────────

    /** The two-guard statement takes twelve values and the three-guard one fifteen. */
    @Test
    void everyPlaceholderIsBound() {
        assertEquals(
                count(SessionDemo.applyCql(KEYSPACE, 0), "?"),
                SessionDemo.applyParams("u", SESSION, 0, "t", "{}", AT).length);
        assertEquals(
                count(SessionDemo.applyCql(KEYSPACE, 5), "?"),
                SessionDemo.applyParams("u", SESSION, 5, "t", "{}", AT).length);
    }

    /**
     * The predecessor's own number, which is the whole of what the third guard asks about.
     */
    @Test
    void theThirdGuardIsBoundWithThePrecedingSequenceNumber() {
        Object[] values = SessionDemo.applyParams("u", SESSION, 5, "t", "{}", AT);

        assertEquals(List.of("u", SESSION, "u", SESSION, 5L, "u", SESSION, 4L),
                List.of(values).subList(0, 8));
    }

    /**
     * Every sequence number is a {@code Long}, because the column is a CQL {@code bigint}.
     *
     * <p>This driver looks a codec up by the column's type and the value's class and raises when it
     * finds none, so an {@code Integer} here would fail every transaction the demo sends. The Python
     * driver widened silently, which is why the hazard survives a line-by-line port.
     */
    @Test
    void aSequenceNumberIsBoundAsALong() {
        for (Object value : SessionDemo.applyParams("u", SESSION, 5, "t", "{}", AT)) {
            if (value instanceof Number number) {
                assertEquals(Long.class, number.getClass(), "bound " + number + " as a narrower type");
            }
        }
        assertThrows(
                CodecNotFoundException.class,
                () -> CodecRegistry.DEFAULT.codecFor(DataTypes.BIGINT, Integer.class));
    }

    /**
     * One instant serves the row's identifier and its event time.
     *
     * <p>Two clock readings could put them a millisecond apart, and a page that sorts a timeline by
     * one and shows the other would then disagree with itself. The Python read the clock twice.
     */
    @Test
    void theTimeuuidAndTheTimestampAgree() {
        Object[] values = SessionDemo.applyParams("u", SESSION, 1, "t", "{}", AT);

        UUID eventId = (UUID) values[11];
        assertEquals(AT, values[12]);
        assertEquals(AT.toEpochMilli(), com.thelastpickle.htap.common.TimeUuids.instantOf(eventId)
                .toEpochMilli());
    }

    /** A prepared statement's marker is {@code ?}, so that is what the page is shown. */
    @Test
    void theOpenStatementShowsTheMarkerThatRan() {
        assertEquals(
                "INSERT INTO demo.sessions_open (user_id, session_id) VALUES (?, ?)",
                SessionDemo.openCql(KEYSPACE));
    }

    // ──────────────────────── Reading a refusal ────────────────────────

    @Test
    void anEmptyProjectionCannotBeRead() {
        assertEquals(
                "the transaction projected nothing, so its guards cannot be read",
                SessionDemo.refusal(0, Map.of()));
    }

    @Test
    void aMissingSessionIsTheFirstThingReported() {
        assertEquals("the session is not open", SessionDemo.refusal(1, guards(null, 1L, 0L)));
    }

    /** A replay is a more useful answer than a gap, and the Python reported it first too. */
    @Test
    void aReplayIsReportedBeforeAGap() {
        assertEquals(
                "seq=1 was already applied, so the replay changed nothing",
                SessionDemo.refusal(1, guards("open", 1L, null)));
    }

    @Test
    void aMissingPredecessorNamesBothSequenceNumbers() {
        assertEquals(
                "seq=1 has not been applied, so seq=2 would leave a gap",
                SessionDemo.refusal(2, guards("open", null, null)));
    }

    /** Every guard satisfied is reported as no reason at all, which is what "applied" means here. */
    @Test
    void everyGuardSatisfiedGivesNoReason() {
        assertEquals("", SessionDemo.refusal(2, guards("open", null, 1L)));
        assertEquals("", SessionDemo.refusal(0, guards("open", null, null)));
    }

    // ──────────────────────── The scripted sequence ────────────────────────

    /**
     * The six steps, in the order that is the argument: apply, replay, jump the queue, then catch up.
     *
     * <p>Steps two and three must leave the projection exactly as they found it, which is what the row
     * count beside each step reports.
     */
    @Test
    void theScriptedSequenceAppliesFourStepsAndRefusesTwo() {
        scriptANode();

        TransactionDemoResult result = demo.demo(0, false);

        assertEquals(
                List.of("open the session", "apply seq=0", "replay seq=0",
                        "attempt seq=2 out of order", "apply seq=1", "apply seq=2"),
                result.steps().stream().map(TransactionStep::action).toList());
        assertEquals(
                List.of(true, true, false, false, true, true),
                result.steps().stream().map(TransactionStep::applied).toList());
        assertEquals(
                List.of(0, 1, 1, 1, 2, 3),
                result.steps().stream().map(TransactionStep::timelineRows).toList());
        assertEquals(List.of(0L, 1L, 2L), result.timeline().stream().map(row -> row.seq()).toList());
    }

    /** Each refusal carries the guard's own words, which is what the demonstration is for. */
    @Test
    void eachRefusalSaysWhichGuardStoppedIt() {
        scriptANode();

        List<TransactionStep> steps = demo.demo(0, false).steps();

        assertEquals(
                "seq=0 was already applied, so the replay changed nothing", steps.get(2).reason());
        assertEquals(
                "seq=1 has not been applied, so seq=2 would leave a gap", steps.get(3).reason());
        assertTrue(steps.stream().filter(TransactionStep::applied)
                .allMatch(step -> step.reason().isEmpty()));
    }

    /** No repeats means no figures at all, rather than a p50 over one sample. */
    @Test
    void noRepeatsMeasuresNothing() {
        scriptANode();

        TransactionDemoResult result = demo.demo(0, false);

        assertEquals(0, result.repeats());
        assertNull(result.appliedP50Ms());
        assertNull(result.appliedMaxMs());
        assertEquals(Map.of(), result.referenceMs());
        assertNull(result.oltpProbe());
        assertNull(result.oltpBaseline());
    }

    /**
     * The measured repeats go into a session of their own, and the references into the demo's.
     *
     * <p>Putting the repeats into the illustrated session would bury its three-row story; the
     * reference writes go to the non-transactional twin, so they cannot move the story at all.
     */
    @Test
    void theMeasuredRepeatsAreKeptOutOfTheIllustratedSession() {
        scriptANode();

        TransactionDemoResult result = demo.demo(2, false);

        assertEquals(2, result.repeats());
        assertEquals(
                Set.of("plain_insert_p50_ms", "plain_insert_max_ms", "lwt_if_not_exists_p50_ms",
                        "lwt_if_not_exists_max_ms"),
                result.referenceMs().keySet());
        assertEquals(List.of(0L, 1L, 2L), result.timeline().stream().map(row -> row.seq()).toList());
        assertEquals(
                2, node.written.stream().filter(cql -> cql.contains("sessions_open")).count(),
                "the repeats shared the illustrated session");
    }

    /** Both references write the same row shape into the twin table, one of them conditionally. */
    @Test
    void theTwoReferencesDifferOnlyInTheirCondition() {
        scriptANode();

        demo.demo(1, false);

        List<String> references = node.written.stream()
                .filter(cql -> cql.contains("session_timeline_plain"))
                .distinct()
                .toList();
        assertEquals(2, references.size());
        assertEquals(references.get(0) + " IF NOT EXISTS", references.get(1));
    }

    // ──────────────────────── The schema probe ────────────────────────

    /** Each probe projects a named column, because a whole {@code LET} reference is refused. */
    @Test
    void everyProbeNamesTheColumnItProjects() {
        Map<String, String> probes = SessionDemo.probes(KEYSPACE);

        assertEquals(
                List.of("sessions_open", "session_seq_applied", "session_timeline"),
                List.copyOf(probes.keySet()));
        assertTrue(probes.get("sessions_open").contains("SELECT probe.session_id;"));
        assertTrue(probes.get("session_timeline").contains("SELECT probe.seq;"));
        assertTrue(probes.values().stream().noneMatch(cql -> cql.contains("INSERT")));
    }

    @Test
    void aNodeThatTakesEveryProbeIsReportedReady() {
        node.onTransact = (cql, values) -> Map.of("probe.seq", 0L);

        TransactionSchemaReport report = demo.schema();

        assertTrue(report.ready());
        assertEquals("", report.note());
        assertEquals(3, report.tables().size());
        assertTrue(report.tables().values().stream()
                .allMatch(TransactionSchemaReport.ACCEPTS::equals));
    }

    /** A refusal is reported in the node's own words, and it is what makes the wipe note appear. */
    @Test
    void aTableThatRefusesIsReportedWithTheNodesWords() {
        node.onTransact = (cql, values) -> {
            throw new IllegalStateException("Accord is not enabled");
        };

        TransactionSchemaReport report = demo.schema();

        assertFalse(report.ready());
        assertEquals("Accord is not enabled", report.tables().get("sessions_open"));
        assertEquals(TransactionSchemaReport.WIPE, report.note());
    }

    // ──────────────────────── A step that raised ────────────────────────

    /**
     * A failure is a third outcome and not a refusal.
     *
     * <p>A refused step is the demonstration working; an error is not, and reporting the two alike
     * would misstate the result.
     */
    @Test
    void aStatementThatRaisedIsReportedApartFromARefusal() {
        node.onTransact = (cql, values) -> {
            throw new IllegalStateException("Cassandra timeout during read query");
        };
        node.onRead = (cql, values) -> List.of();

        TransactionStep step = demo.step("u", SESSION, 0, "session.step");

        assertFalse(step.applied());
        assertEquals("", step.reason());
        assertEquals("Cassandra timeout during read query", step.error());
    }

    /** The demo's own user names the session it belongs to, so two runs cannot share one. */
    @Test
    void theDemoUserCarriesTheSessionsFirstEightCharacters() {
        assertEquals("txn-demo-6bd0c9f4", SessionDemo.demoUser(SessionDemo.USER_PREFIX, SESSION));
    }

    // ──────────────────────── The scripted node ────────────────────────

    /**
     * A node that applies a sequence number once its predecessor is there, and reports the guards it
     * read as an Accord transaction would.
     */
    private void scriptANode() {
        // Per user, because the measurement repeats run in a session of their own: one shared set
        // would have them refused as replays and the test would then pass for the wrong reason.
        Map<String, Set<Long>> applied = new LinkedHashMap<>();
        node.onTransact = (cql, values) -> {
            Set<Long> session = applied.computeIfAbsent((String) values[0], user -> new TreeSet<>());
            long seq = (Long) values[4];
            Map<String, Object> guards = new LinkedHashMap<>();
            guards.put("session_ok.session_id", SESSION.toString());
            guards.put("already.seq", session.contains(seq) ? seq : null);
            if (seq > 0) {
                guards.put("prev_ok.seq", session.contains(seq - 1) ? seq - 1 : null);
            }
            if (!session.contains(seq) && (seq == 0 || session.contains(seq - 1))) {
                session.add(seq);
            }
            return guards;
        };
        node.onRead = (cql, values) -> {
            Set<Long> session = applied.getOrDefault((String) values[0], Set.of());
            long[] ordered = session.stream().mapToLong(Long::longValue).toArray();
            return TransactionFakes.timelineRows(ordered);
        };
    }

    private static Map<String, Object> guards(Object session, Object already, Object previous) {
        Map<String, Object> guards = new LinkedHashMap<>();
        guards.put("session_ok.session_id", session);
        guards.put("already.seq", already);
        guards.put("prev_ok.seq", previous);
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
