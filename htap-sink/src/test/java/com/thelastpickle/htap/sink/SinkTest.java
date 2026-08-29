package com.thelastpickle.htap.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.common.Geometry;
import com.thelastpickle.htap.common.TimeUuids;
import com.thelastpickle.htap.sink.Sink.Batch;
import com.thelastpickle.htap.sink.SinkFakes.RecordingWrites;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * The batch: what one poll writes, and when the offsets may move.
 *
 * <p>The commit rule is the whole of the sink's delivery guarantee, and it is decidable here: every
 * write of the batch is awaited before the offsets are committed, so a failed batch is redelivered.
 * Every write is an idempotent upsert, which is what makes the redelivery cost duplicate work and no
 * duplicate data.
 */
class SinkTest {

    private static final Instant NOW = Instant.parse("2026-08-29T12:34:56Z");
    private static final SinkSettings SETTINGS = SinkSettings.from(name -> null);

    private final AtomicLong nanos = new AtomicLong();
    private final RecordingWrites writes = new RecordingWrites();
    private final Alerts alerts = new Alerts(nanos::get);
    private final Sink sink = new Sink(SETTINGS, writes, alerts, () -> NOW, nanos::get);

    /** Every record of the batch is written, and the batch says how many it held. */
    @Test
    void everyRecordOfTheBatchIsWritten() {
        Batch batch = sink.write(List.of(
                SinkFakes.event(id(), "asset-1", 59.91, 10.75, 120.0),
                SinkFakes.event(id(), "asset-2", 59.92, 10.76, 130.0)));

        assertTrue(batch.acknowledged());
        assertEquals(2, batch.buffered());
        assertEquals(List.of("asset-1", "asset-2"), writes.events.stream().map(Event::entityId).toList());
    }

    /**
     * One write that fails takes the whole batch's commit with it.
     *
     * <p>The offsets stay where they are, so the batch is redelivered and replayed. Committing first,
     * as a fire-and-forget loop must, would drop the batch silently: the offsets would already say it
     * had been handled.
     */
    @Test
    void aFailedWriteWithholdsTheCommit() {
        writes.failure = new IllegalStateException("no host available");

        Batch batch = sink.write(List.of(SinkFakes.event(id(), "asset-1", 59.91, 10.75, 120.0)));

        assertFalse(batch.acknowledged());
        assertEquals(1, batch.buffered(), "the readings were derived and sent; none was acknowledged");
    }

    /**
     * A record that is not readable JSON is skipped rather than failing the process.
     *
     * <p>A change from the Python, where the deserializer ran inside the poll: one malformed record
     * failed the process, and since its offset was never committed the next process was handed the
     * same record for ever.
     */
    @Test
    void anUnreadableRecordIsSkipped() {
        Batch batch = sink.write(List.of(
                SinkFakes.json("not json at all"),
                SinkFakes.event(id(), "asset-1", 59.91, 10.75, 120.0)));

        assertTrue(batch.acknowledged());
        assertEquals(1, batch.buffered());
        assertEquals(1, writes.events.size());
    }

    /** An empty poll writes nothing and is acknowledged, so the offsets still move. */
    @Test
    void anEmptyBatchIsAcknowledged() {
        Batch batch = sink.write(List.of());

        assertTrue(batch.acknowledged());
        assertEquals(0, batch.buffered());
    }

    /** The alerts a batch earns are written as they are scored, before the batch is awaited. */
    @Test
    void theAlertsOfABatchAreWritten() {
        alerts.reload(List.of(new Zone(
                "zone-oslo-airport",
                "Oslo Lufthavn Gardermoen",
                "critical",
                Geometry.parseWktPolygon(DemoSchema.ZONES.getFirst().polygonWkt()))));

        sink.write(List.of(SinkFakes.event(id(), "asset-1", 60.20, 11.10, 120.0)));

        assertEquals(1, writes.alerts.size());
        assertEquals("zone_breach", writes.alerts.getFirst().alertType());
    }

    /** The derived speed comes from the previous reading of the same asset in an earlier batch. */
    @Test
    void theTrackerRemembersAcrossBatches() {
        UUID first = TimeUuids.timeUuid(NOW.minusSeconds(10));
        UUID second = TimeUuids.timeUuid(NOW);
        sink.write(List.of(SinkFakes.event(first.toString(), "asset-1", 59.91, 10.75, 120.0)));
        sink.write(List.of(SinkFakes.event(second.toString(), "asset-1", 59.9105, 10.75, 120.0)));

        assertEquals(2, writes.events.size());
        assertEquals(second, writes.events.get(1).eventId());
    }

    /**
     * The loop commits, then counts, then reports.
     *
     * <p>In that order because the counter is an indicator rather than the data: a counter write that
     * failed must not hold up the offsets of a batch the cluster has already acknowledged.
     */
    @Test
    void theLoopCommitsThenCounts() {
        List<String> calls = new ArrayList<>();
        Consumer<byte[], byte[]> broker = scripted(calls, new ArrayDeque<>(List.of(
                records(SinkFakes.event(id(), "asset-1", 59.91, 10.75, 120.0)))));

        assertThrows(StopTheLoop.class, () -> sink.run(broker, zones()));

        assertEquals(List.of("poll", "commitSync", "poll"), calls);
        assertEquals(List.of("2026-08-29T12:30"), writes.counted);
        assertEquals(List.of(1), writes.countedRecords);
    }

    /** A failed batch is not committed, and the loop goes back for the same records. */
    @Test
    void theLoopDoesNotCommitAFailedBatch() {
        writes.failure = new IllegalStateException("no host available");
        List<String> calls = new ArrayList<>();
        Consumer<byte[], byte[]> broker = scripted(calls, new ArrayDeque<>(List.of(
                records(SinkFakes.event(id(), "asset-1", 59.91, 10.75, 120.0)))));

        assertThrows(StopTheLoop.class, () -> sink.run(broker, zones()));

        assertEquals(List.of("poll", "poll"), calls);
        assertEquals(List.of(), writes.counted);
    }

    /** An empty poll costs nothing: no commit, no counter, no report. */
    @Test
    void anEmptyPollIsNotCommitted() {
        List<String> calls = new ArrayList<>();
        Consumer<byte[], byte[]> broker =
                scripted(calls, new ArrayDeque<>(List.of(ConsumerRecords.empty())));

        assertThrows(StopTheLoop.class, () -> sink.run(broker, zones()));

        assertEquals(List.of("poll", "poll"), calls);
        assertEquals(List.of(), writes.counted);
    }

    /** The counter's bucket is the half hour the batch was committed in, not the reading's own. */
    @Test
    void theCounterBucketIsTheHalfHourOfTheCommit() {
        List<String> calls = new ArrayList<>();
        Sink onTheHalfHour = new Sink(
                SETTINGS, writes, alerts, () -> Instant.parse("2026-08-29T12:59:59Z"), nanos::get);
        Consumer<byte[], byte[]> broker = scripted(calls, new ArrayDeque<>(List.of(
                records(SinkFakes.event(id(), "asset-1", 59.91, 10.75, 120.0)))));

        assertThrows(StopTheLoop.class, () -> onTheHalfHour.run(broker, zones()));

        assertEquals(List.of("2026-08-29T12:30"), writes.counted);
    }

    /** The rate is over the window between two reports, and the report is on the sink's own clock. */
    @Test
    void theReportIsEveryFiveSeconds() {
        assertEquals(Duration.ofSeconds(5), SETTINGS.reportEvery());
        assertEquals(Duration.ofSeconds(60), SETTINGS.zoneReload());
    }

    private static String id() {
        return TimeUuids.timeUuid(NOW).toString();
    }

    /** A zone reader that answers nothing, for the loop tests where the zones are not the point. */
    private static Zones zones() {
        SinkFakes.RecordingSession node = new SinkFakes.RecordingSession();
        node.answers = cql -> List.of();
        return new Zones(node.session(), "demo");
    }

    private static ConsumerRecords<byte[], byte[]> records(byte[]... values) {
        TopicPartition partition = new TopicPartition("demo-events", 0);
        List<ConsumerRecord<byte[], byte[]>> polled = new ArrayList<>();
        for (int at = 0; at < values.length; at++) {
            polled.add(new ConsumerRecord<>(partition.topic(), partition.partition(), at, null, values[at]));
        }
        // The two-argument form: the one-argument constructor is deprecated in 4.2, and the second
        // map is the next offsets, which nothing here reads.
        return new ConsumerRecords<>(Map.of(partition, polled), Map.of());
    }

    /**
     * A broker that answers the scripted polls and then stops the loop.
     *
     * <p>The loop is deliberately endless, as the Python's was, so a test ends it by having the poll
     * raise once the script is spent.
     */
    private static Consumer<byte[], byte[]> scripted(
            List<String> calls, Deque<ConsumerRecords<byte[], byte[]>> answers) {
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = (Consumer<byte[], byte[]>) Proxy.newProxyInstance(
                SinkTest.class.getClassLoader(),
                new Class<?>[] {Consumer.class},
                (proxy, method, args) -> {
                    calls.add(method.getName());
                    return switch (method.getName()) {
                        case "poll" -> {
                            ConsumerRecords<byte[], byte[]> next = answers.poll();
                            if (next == null) {
                                throw new StopTheLoop();
                            }
                            yield next;
                        }
                        case "commitSync", "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
        return consumer;
    }

    /** The sentinel that ends an endless loop, and nothing the sink itself can raise. */
    private static final class StopTheLoop extends RuntimeException {

        private static final long serialVersionUID = 1L;
    }
}
