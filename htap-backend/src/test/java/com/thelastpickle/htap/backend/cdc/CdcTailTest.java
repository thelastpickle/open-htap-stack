package com.thelastpickle.htap.backend.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.CdcRecord;
import com.thelastpickle.htap.backend.api.dto.CdcStreamStatus;
import com.thelastpickle.htap.backend.config.CdcSettings;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The buffer, the counters and the states, with the broker scripted.
 *
 * <p>What Kafka answers an assign and a seek is settled by running the tail against the stack; what is
 * decidable here is everything above that line, which is the whole of what the page reads.
 */
class CdcTailTest {

    private static final long WROTE_AT_MICROS = 1787846133_000000L;

    private final CdcSettings settings = CdcFixtures.settings(3);
    private final ScriptedSource source = new ScriptedSource();
    private final AtomicLong nanos = new AtomicLong();
    private final CdcTail tail = tail(settings);

    /** Before the first attach the page is told the topic and the broker, and nothing else. */
    @Test
    void theStatusAnswersBeforeAnythingIsAttached() {
        CdcStreamStatus status = tail.status();

        assertEquals(CdcTail.STARTING, status.state());
        assertEquals("cdc-mutations", status.topic());
        assertEquals("kafka:19092", status.bootstrap());
        // The registry's own trailing slash is dropped, since every path below it adds one.
        assertEquals("http://apicurio:8080/apis/ccompat/v7", status.registry());
        assertEquals(List.of(), status.partitions());
        assertEquals(3, status.bufferSize());
        assertEquals(0, status.buffered());
        assertEquals(0, status.consumed());
        assertNull(status.latencyP50Ms());
    }

    /** One attach, then the partitions the broker gave and the state that says it is reading. */
    @Test
    void attachingReportsThePartitionsItWasGiven() {
        source.attachment = new CdcSource.Attachment(List.of(0, 1), Map.of(0, 10L, 1, 20L));

        assertTrue(tail.tick());
        assertEquals(List.of(0, 1), tail.status().partitions());
        assertEquals(CdcTail.TAILING, tail.status().state());
        assertEquals(1, source.attaches);
    }

    /** A second tick reuses the attachment: the seek is what a re-attach would repeat. */
    @Test
    void aSecondTickDoesNotAttachAgain() {
        tail.tick();
        tail.tick();

        assertEquals(1, source.attaches);
    }

    /** An absent topic is a state and not a failure, and it is retried from scratch. */
    @Test
    void anAbsentTopicIsItsOwnState() {
        source.attachFailure =
                new CdcSource.TopicAbsent("topic cdc-mutations does not exist yet; the Sidecar"
                        + " creates it with its first published mutation");

        assertFalse(tail.tick());
        assertEquals(CdcTail.WAITING_FOR_TOPIC, tail.status().state());
        assertTrue(tail.status().error().startsWith("topic cdc-mutations does not exist yet"));
        assertEquals(1, source.closes, "the consumer is closed before the retry");
    }

    /** Any other failure is reported with its type, and the next tick attaches again. */
    @Test
    void aBrokerFailureIsReportedAndTheAttachRepeated() {
        source.attachFailure = new IllegalStateException("No entry found for connection 2147483646");

        assertFalse(tail.tick());
        assertEquals(CdcTail.ERRORED, tail.status().state());
        assertEquals(
                "IllegalStateException: No entry found for connection 2147483646",
                tail.status().error());

        source.attachFailure = null;
        assertTrue(tail.tick());
        assertEquals(2, source.attaches);
        assertNull(tail.status().error(), "a tick that got through clears the last failure");
    }

    /** The oldest record leaves as the newest arrives, which is the page's memory bound. */
    @Test
    void theBufferHoldsNoMoreThanItsSize() {
        source.batches.add(arrivals(0, 100, 5));
        tail.tick();

        assertEquals(3, tail.status().buffered());
        assertEquals(5, tail.status().consumed(), "the count is of what arrived, not of what is held");
        assertEquals(
                List.of(5L, 4L, 3L),
                tail.records(10, null).stream().map(CdcRecord::seq).toList());
    }

    /** Newest first, and no more than asked for. */
    @Test
    void theRecordsComeBackNewestFirst() {
        source.batches.add(arrivals(0, 100, 3));
        tail.tick();

        List<CdcRecord> records = tail.records(2, null);
        assertEquals(List.of(3L, 2L), records.stream().map(CdcRecord::seq).toList());
        assertEquals(102, records.getFirst().offset());
    }

    /** A page polling with {@code since} is given only what it has not seen. */
    @Test
    void sinceLeavesOutWhatTheCallerHas() {
        source.batches.add(arrivals(0, 100, 3));
        tail.tick();

        assertEquals(List.of(3L), tail.records(10, 2L).stream().map(CdcRecord::seq).toList());
        assertEquals(List.of(), tail.records(10, 3L));
    }

    /**
     * A record below the offset the log ended at when the tail attached is a backfill.
     *
     * <p>It is excluded from the latency samples, because its age measures the backlog the tail read
     * through rather than the publisher's delay.
     */
    @Test
    void aRecordFromBeforeTheAttachIsFlaggedAndNotSampled() {
        source.attachment = new CdcSource.Attachment(List.of(0), Map.of(0, 102L));
        source.batches.add(arrivals(0, 100, 3));
        tail.tick();

        List<CdcRecord> oldestFirst = tail.records(10, null).reversed();
        assertEquals(List.of(true, true, false), oldestFirst.stream().map(CdcRecord::backfill).toList());
        assertNull(oldestFirst.getFirst().ageMs());
        assertEquals(8000.0, oldestFirst.getLast().ageMs().doubleValue());
        assertEquals(8000.0, tail.status().latencyP50Ms().doubleValue(), "one live sample");
    }

    /** The p50 and the maximum come from the live records' ages, sorted. */
    @Test
    void theLatencyFiguresAreOverTheLiveRecords() {
        CdcTail wide = tail(CdcFixtures.settings(10));
        source.batches.add(List.of(
                arrival(0, 100, WROTE_AT_MICROS / 1000 + 1000),
                arrival(0, 101, WROTE_AT_MICROS / 1000 + 9000),
                arrival(0, 102, WROTE_AT_MICROS / 1000 + 5000)));
        wide.tick();

        assertEquals(5000.0, wide.status().latencyP50Ms().doubleValue());
        assertEquals(9000.0, wide.status().latencyMaxMs().doubleValue());
    }

    /** A record the dashboard cannot read is counted and kept, with its reason. */
    @Test
    void anUnreadableRecordIsCountedAndKept() {
        source.batches.add(List.of(new Arrival(0, 100, null, 1787846141_000L, new byte[] {9})));
        tail.tick();

        assertEquals(1, tail.status().decodeFailures());
        assertEquals(1, tail.status().buffered());
        assertTrue(tail.records(1, null).getFirst().decodeError().contains("not Confluent-framed"));
        assertEquals("", tail.records(1, null).getFirst().key(), "no key, so nothing to split");
    }

    /**
     * The rate is measured between two polls that both saw records, and only once a second has passed.
     *
     * <p>Not an average since startup: the write rate is changed from the Settings page, and an average
     * would hide the change.
     */
    @Test
    void theRateIsOverTheIntervalBetweenTwoPolls() {
        CdcTail wide = tail(CdcFixtures.settings(400));
        source.batches.add(arrivals(0, 100, 100));
        wide.tick();
        assertEquals(0.0, wide.status().ratePerSec(), "under a second, so nothing is claimed yet");

        nanos.set(2_000_000_000L);
        source.batches.add(arrivals(0, 200, 100));
        wide.tick();

        assertEquals(100.0, wide.status().ratePerSec());
    }

    /** The schema ids are the registry's, which is where the lookups were held. */
    @Test
    void theSchemaIdsAreTheOnesLookedUp() {
        source.batches.add(arrivals(0, 100, 1));
        tail.tick();

        assertEquals(List.of(CdcFixtures.SCHEMA_ID), tail.status().schemaIds());
    }

    /**
     * Shutdown wakes the consumer and leaves the closing to the loop.
     *
     * <p>A Kafka consumer permits one thread at a time, so closing it from the shutdown thread while
     * a poll was in flight raises from its own guard and leaves it open; {@code wakeup} is the one
     * call that is safe from there.
     */
    @Test
    void shutdownWakesTheConsumerRatherThanClosingIt() {
        tail.tick();

        tail.onStop(null);

        assertEquals(1, source.wakeups);
        assertEquals(0, source.closes, "the shutdown thread must not close the consumer");
    }

    /** The newest record's own arrival time, so the page can say how long ago the last one was. */
    @Test
    void theLastRecordTimeIsTheBrokersAppend() {
        source.batches.add(arrivals(0, 100, 2));
        tail.tick();

        assertEquals(Long.valueOf(1787846141_000L), tail.status().lastRecordAtMs());
    }

    private CdcTail tail(CdcSettings using) {
        // One registry for both, since the ids the status reports are the ones the decoder looked up.
        SchemaRegistry registry = CdcFixtures.registry(using);
        return new CdcTail(source, new CdcDecoder(registry), registry, using, nanos::get);
    }

    /** {@code count} arrivals from one partition, each eight seconds behind its own mutation. */
    private static List<Arrival> arrivals(int partition, long firstOffset, int count) {
        List<Arrival> arrivals = new ArrayList<>(count);
        for (int at = 0; at < count; at++) {
            arrivals.add(arrival(partition, firstOffset + at, 1787846141_000L));
        }
        return arrivals;
    }

    private static Arrival arrival(int partition, long offset, long kafkaAtMs) {
        return CdcFixtures.arrival(
                partition,
                offset,
                kafkaAtMs,
                CdcFixtures.framed(
                        CdcFixtures.SCHEMA_ID,
                        CdcFixtures.mutation("UPDATE", WROTE_AT_MICROS, CdcFixtures.telemetry())));
    }

    /** A broker that answers from a script: one attachment, then a batch per poll. */
    private static final class ScriptedSource implements CdcSource {

        private final Deque<List<Arrival>> batches = new ArrayDeque<>();
        private Attachment attachment = new Attachment(List.of(0), Map.of(0, 0L));
        private RuntimeException attachFailure;
        private int attaches;
        private int closes;
        private int wakeups;

        @Override
        public Attachment attach() {
            attaches++;
            if (attachFailure != null) {
                throw attachFailure;
            }
            return attachment;
        }

        @Override
        public List<Arrival> poll() {
            List<Arrival> batch = batches.poll();
            return batch == null ? List.of() : batch;
        }

        @Override
        public void close() {
            closes++;
        }

        @Override
        public void wakeup() {
            wakeups++;
        }

        @Override
        public String bootstrap() {
            return "kafka:19092";
        }
    }
}
