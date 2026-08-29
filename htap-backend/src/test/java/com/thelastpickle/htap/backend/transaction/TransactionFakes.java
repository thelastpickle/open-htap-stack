package com.thelastpickle.htap.backend.transaction;

import com.datastax.oss.driver.api.core.cql.Row;
import com.thelastpickle.htap.backend.query.OltpProbe;
import com.thelastpickle.htap.backend.query.OltpSampler;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * A node that answers what a test tells it to, and the rows to answer with.
 *
 * <p>Scripted rather than modelled, so a test says what the node projects and the class under test
 * has to read it correctly. A fake that decided the outcomes itself would only be asserting that two
 * implementations of Accord's guard rules agree.
 */
final class TransactionFakes {

    static final String KEYSPACE = "demo";

    private TransactionFakes() {}

    /** What a scripted node does with one statement. */
    interface Answer {
        Object of(String cql, Object[] values);
    }

    /** An {@link Accord} whose three methods are whatever a test set them to. */
    static final class FakeAccord extends Accord {

        final List<String> transacted = new ArrayList<>();
        final List<Object[]> bound = new ArrayList<>();
        final List<String> written = new ArrayList<>();
        final List<String> readStatements = new ArrayList<>();

        Answer onTransact = (cql, values) -> Map.of();
        Answer onRead = (cql, values) -> List.of();

        FakeAccord() {
            // Neither collaborator is reached: every method below is overridden.
            super(null, null);
        }

        @Override
        public String keyspace() {
            return KEYSPACE;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<String, Object> transact(String cql, Object... values) {
            transacted.add(cql);
            bound.add(values);
            return (Map<String, Object>) onTransact.of(cql, values);
        }

        @Override
        public void write(String cql, Object... values) {
            written.add(cql);
            bound.add(values);
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<Row> read(String cql, Object... values) {
            readStatements.add(cql);
            return (List<Row>) onRead.of(cql, values);
        }
    }

    /** A row that answers by column name, which is how every read outside a transaction reads. */
    static Row row(Map<String, Object> values) {
        return (Row) Proxy.newProxyInstance(
                TransactionFakes.class.getClassLoader(),
                new Class<?>[] {Row.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getString", "getUuid", "getInstant", "getObject" ->
                            values.get((String) args[0]);
                    case "getLong" -> ((Number) values.get((String) args[0])).longValue();
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    /** One timeline row per applied sequence number, in sequence order. */
    static List<Row> timelineRows(long... seqs) {
        List<Row> rows = new ArrayList<>(seqs.length);
        for (long seq : seqs) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("seq", seq);
            values.put("event_id", UUID.nameUUIDFromBytes(Long.toString(seq).getBytes()));
            values.put("event_time", Instant.parse("2026-08-29T12:00:00Z").plusSeconds(seq));
            values.put("event_type", "session.step");
            values.put("payload", "{}");
            rows.add(row(values));
        }
        return rows;
    }

    /** A zone occupancy row, as {@code zone_occupancy} answers one. */
    static Row zoneRow(String zoneId, String name, String severity, long capacity, long remaining) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("zone_id", zoneId);
        values.put("zone_name", name);
        values.put("severity", severity);
        values.put("capacity", capacity);
        values.put("remaining", remaining);
        return row(values);
    }

    /** A sampler that fails the test if a demo asked for a probe it was not meant to take. */
    static OltpSampler noSampler() {
        return new OltpSampler() {
            @Override
            public Optional<String> subject() {
                throw new AssertionError("the demo asked for a probe subject");
            }

            @Override
            public OltpProbe sample(String entityId) {
                throw new AssertionError("the demo started a probe");
            }
        };
    }
}
