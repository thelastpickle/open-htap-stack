package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Whether a closed window can still grow, decided from what the broker answered.
 *
 * <p>The decision is a pure function of three maps, so every case here is settled without a
 * broker: what this check gets wrong is which partitions it requires, not how it talks to Kafka.
 *
 * <p>The three maps are, in order, where the sink has committed, the first offset stamped at or
 * after the target, and where each partition ends.
 */
class SinkProgressTest {

    private static final String TOPIC = "demo-events";
    private static final Instant TARGET = Instant.parse("2026-08-28T12:16:00Z");

    /** What {@code OffsetSpec.forTimestamp} answers when nothing on the partition is that late. */
    private static final long NO_RECORD_THAT_LATE = -1L;

    private final List<TopicPartition> twelve = partitions(12);

    @Test
    void aSinkPastEveryPartitionsTargetIsSettledAndSaysSo() {
        SinkProgress.Verdict verdict = SinkProgress.decide(
                TOPIC, TARGET, twelve, everywhere(500), everywhere(400), everywhere(600));

        assertTrue(verdict.settled());
        assertEquals(
                "all 12 partitions of demo-events are consumed past 2026-08-28T12:16:00Z",
                verdict.detail());
    }

    /** At the offset, not past it: the record at that offset belongs to a later window. */
    @Test
    void aSinkExactlyAtTheFirstRecordBeyondTheTargetIsSettled() {
        assertTrue(SinkProgress
                .decide(TOPIC, TARGET, twelve, everywhere(400), everywhere(400), everywhere(600))
                .settled());
    }

    /**
     * One partition short is enough. Requiring every one is the fix for two CI failures: the sink
     * polls across twelve partitions, so under a backlog their positions diverge and one partition
     * reaching the current window says nothing about the other eleven.
     */
    @Test
    void oneShortPartitionOutOfTwelveIsNotSettled() {
        Map<TopicPartition, Long> committed = everywhere(400);
        committed.put(new TopicPartition(TOPIC, 5), 100L);

        SinkProgress.Verdict verdict = SinkProgress.decide(
                TOPIC, TARGET, twelve, committed, everywhere(400), everywhere(600));

        assertFalse(verdict.settled());
        assertEquals(
                "the sink has not consumed past 2026-08-28T12:16:00Z: 1 of 12 partitions are short,"
                        + " 300 records in all, the worst partition 5 by 300",
                verdict.detail());
    }

    /** The lag CI measured on a four-minute-old stack, summarised as the page reports it. */
    @Test
    void severalShortPartitionsAreSummarisedByCountTotalAndWorst() {
        Map<TopicPartition, Long> committed = everywhere(30_000);
        committed.put(new TopicPartition(TOPIC, 0), 30_000L - 560);
        committed.put(new TopicPartition(TOPIC, 5), 30_000L - 29_718);

        SinkProgress.Verdict verdict = SinkProgress.decide(
                TOPIC, TARGET, twelve, committed, everywhere(30_000), everywhere(40_000));

        assertFalse(verdict.settled());
        assertEquals(
                "the sink has not consumed past 2026-08-28T12:16:00Z: 2 of 12 partitions are short,"
                        + " 30,278 records in all, the worst partition 5 by 29,718",
                verdict.detail());
    }

    /**
     * A partition holding nothing that late has to be read to its end: all of it belongs to this
     * window or an earlier one.
     */
    @Test
    void aPartitionWithNoRecordThatLateIsMeasuredAgainstItsEnd() {
        List<TopicPartition> one = partitions(1);
        Map<TopicPartition, Long> noneThatLate = everywhere(NO_RECORD_THAT_LATE);

        assertFalse(SinkProgress
                .decide(TOPIC, TARGET, one, everywhere(599), noneThatLate, everywhere(600))
                .settled());
        assertTrue(SinkProgress
                .decide(TOPIC, TARGET, one, everywhere(600), noneThatLate, everywhere(600))
                .settled());
    }

    /**
     * An answer Kafka did not give is not an answer. Reading a missing end offset as the committed
     * one would pass the partition on nothing at all, which is how this check would fail silently.
     */
    @Test
    void aPartitionKafkaDidNotAnswerForIsRefusedRatherThanPassed() {
        SinkProgress.Verdict verdict = SinkProgress.decide(
                TOPIC, TARGET, partitions(1), everywhere(600),
                everywhere(NO_RECORD_THAT_LATE), Map.of());

        assertFalse(verdict.settled());
        assertEquals(
                "Kafka reported no end offset for partition 0 of demo-events", verdict.detail());
    }

    @Test
    void aGroupThatHasCommittedNothingOnAPartitionIsNotSettled() {
        List<TopicPartition> one = partitions(1);
        String expected = "the sink has committed no offset on partition 0 of demo-events";

        assertEquals(expected, SinkProgress
                .decide(TOPIC, TARGET, one, Map.of(), everywhere(400), everywhere(600))
                .detail());
        assertEquals(expected, SinkProgress
                .decide(TOPIC, TARGET, one, everywhere(-1), everywhere(400), everywhere(600))
                .detail());
    }

    /**
     * A minute of margin, because a Kafka record's timestamp is the producer's clock at send and
     * the event time comes from a timeuuid the same producer stamped in the same loop.
     */
    @Test
    void theMarginIsAMinute() {
        assertEquals(60, SinkProgress.MARGIN.toSeconds());
    }

    private static List<TopicPartition> partitions(int count) {
        return IntStream.range(0, count)
                .mapToObj(partition -> new TopicPartition(TOPIC, partition))
                .toList();
    }

    /** The same offset on every one of the twelve partitions, which each case then varies. */
    private Map<TopicPartition, Long> everywhere(long offset) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (TopicPartition partition : twelve) {
            offsets.put(partition, offset);
        }
        return offsets;
    }
}
