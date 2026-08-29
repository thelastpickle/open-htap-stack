package com.thelastpickle.htap.sink;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.thelastpickle.htap.sink.Alerts.Proximity;
import com.thelastpickle.htap.sink.DroneTracker.Derived;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * The seams the sink's tests drive: a node, a broker and the writes.
 *
 * <p>Each is scripted rather than modelled. A fake that decided for itself which statement succeeds
 * would only be asserting that two implementations of the same rule agree.
 */
final class SinkFakes {

    private SinkFakes() {}

    /**
     * A node that records every statement and answers reads from a script.
     *
     * <p>Over {@link Proxy} rather than a stub class, because {@code CqlSession} has some thirty
     * methods and this needs four of them; a proxy also fails loudly on a fifth, which is what keeps
     * the fake honest about what the class under test actually calls.
     */
    static final class RecordingSession {

        final List<String> executed = new ArrayList<>();
        final List<Object[]> bound = new ArrayList<>();
        final Map<String, ConsistencyLevel> consistency = new HashMap<>();

        /** What a read answers, by the statement that asked. */
        Function<String, List<Row>> answers = cql -> List.of();

        /** Statements whose text contains one of these fail, as a node refusing would. */
        final List<String> failing = new ArrayList<>();

        private final CqlSession session;

        RecordingSession() {
            this.session = (CqlSession) Proxy.newProxyInstance(
                    SinkFakes.class.getClassLoader(),
                    new Class<?>[] {CqlSession.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "execute" -> execute(args[0]);
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        CqlSession session() {
            return session;
        }

        /** Every statement whose text holds this fragment. */
        List<String> matching(String fragment) {
            return executed.stream().filter(cql -> cql.contains(fragment)).toList();
        }

        private ResultSet execute(Object statement) {
            String cql;
            if (statement instanceof SimpleStatement simple) {
                cql = simple.getQuery();
                bound.add(simple.getPositionalValues().toArray());
                if (simple.getConsistencyLevel() != null) {
                    consistency.put(cql, simple.getConsistencyLevel());
                }
            } else {
                cql = (String) statement;
                bound.add(new Object[0]);
            }
            executed.add(cql);
            for (String fragment : failing) {
                if (cql.contains(fragment)) {
                    throw new IllegalStateException("the node refused: " + fragment);
                }
            }
            return resultSet(answers.apply(cql));
        }
    }

    /** A result set over rows a test supplied. */
    static ResultSet resultSet(List<Row> rows) {
        Deque<Row> remaining = new ArrayDeque<>(rows);
        return (ResultSet) Proxy.newProxyInstance(
                SinkFakes.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "one" -> remaining.poll();
                    case "iterator" -> List.copyOf(rows).iterator();
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    /** A row answering by column name, which is how every read here reads. */
    static Row row(Map<String, Object> values) {
        return (Row) Proxy.newProxyInstance(
                SinkFakes.class.getClassLoader(),
                new Class<?>[] {Row.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getString", "getObject" -> values.get((String) args[0]);
                    case "getBoolean" -> Boolean.TRUE.equals(values.get((String) args[0]));
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    /** Writes that record what they were asked to write, and fail when a test says to. */
    static final class RecordingWrites implements Writes {

        final List<Event> events = new ArrayList<>();
        final List<AlertRow> alerts = new ArrayList<>();
        final List<String> counted = new ArrayList<>();
        final List<Integer> countedRecords = new ArrayList<>();

        /** When set, every write of the next batch is answered with this failure. */
        RuntimeException failure;

        @Override
        public List<CompletionStage<?>> event(
                Event event, Derived derived, Proximity proximity, Instant now) {
            events.add(event);
            return failure == null
                    ? List.of(CompletableFuture.completedFuture(null))
                    : List.of(CompletableFuture.failedFuture(failure));
        }

        @Override
        public void count(int records, String bucket) {
            counted.add(bucket);
            countedRecords.add(records);
        }

        @Override
        public void alert(AlertRow alert) {
            alerts.add(alert);
        }
    }

    /** One record on the topic, which is all the sink reads of it. */
    static byte[] json(String text) {
        return text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** The event the producer writes, as JSON, with an id a caller chose. */
    static byte[] event(String eventId, String entityId, double lat, double lon, double altitude) {
        return json("""
                {"event_id": "%s", "entity_id": "%s", "position": {"lat": %s, "lon": %s},
                 "z_m": %s, "event_type": "telemetry", "observer_id": "observer-0000",
                 "temp_external_c": -3.5, "temp_internal_c": 21.5, "text": "a snippet"}"""
                .formatted(eventId, entityId, lat, lon, altitude));
    }
}
