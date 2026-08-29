package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;

/**
 * The topic this process asks for, and the two answers it treats as ordinary.
 *
 * <p>The partition count is the reason the request exists: the broker's own default is three, and
 * the settled-window check is only meaningful across more than that.
 */
class TopicsTest {

    private final List<String> said = new ArrayList<>();

    /** Twelve partitions and one replica, whatever the broker would have created. */
    @Test
    void theTopicAsksForTwelvePartitions() {
        NewTopic wanted = Topics.wanted("demo-events", 12, (short) 1);

        assertEquals("demo-events", wanted.name());
        assertEquals(12, wanted.numPartitions());
        assertEquals((short) 1, wanted.replicationFactor());
    }

    @Test
    void aCreatedTopicIsReportedWithItsShape() {
        Topics.ensure(topic -> {}, Topics.wanted("demo-events", 12, (short) 1), said::add);

        assertEquals(List.of("created topic=demo-events partitions=12 rf=1"), said);
    }

    /**
     * A topic that exists is the restart case, and the cause arrives wrapped.
     *
     * <p>The admin client hands the reason back inside an {@code ExecutionException}, so a check on
     * the outer type alone would report every restart as a failure.
     */
    @Test
    void aTopicThatExistsIsNotAFailure() {
        Topics.ensure(
                topic -> {
                    throw new ExecutionException(new TopicExistsException("Topic 'demo-events' exists"));
                },
                Topics.wanted("demo-events", 12, (short) 1),
                said::add);

        assertEquals(List.of("topic already exists: demo-events"), said);
    }

    /** A broker that cannot be reached is reported and left to the send loop. */
    @Test
    void anyOtherFailureIsReportedAndNotRaised() {
        Topics.ensure(
                topic -> {
                    throw new ExecutionException(new IllegalStateException("No cluster yet"));
                },
                Topics.wanted("demo-events", 12, (short) 1),
                said::add);

        assertEquals(1, said.size());
        assertTrue(said.getFirst().startsWith("topic create skipped/failed (ok for demo):"), said.toString());
    }

    /** The client is lossy by design, and named so the broker's metrics can tell it apart. */
    @Test
    void theProducerIsConfiguredForRateRatherThanDurability() {
        Map<String, Object> config = Topics.producerConfig(
                ProducerSettings.from(Map.of("KAFKA_COMPRESSION", "lz4")::get));

        assertEquals("0", config.get("acks"));
        assertEquals(0, config.get("retries"));
        assertEquals("demo-producer", config.get("client.id"));
        assertEquals("lz4", config.get("compression.type"));
        assertEquals(20, config.get("linger.ms"));
        assertEquals(32768, config.get("batch.size"));
        assertEquals(
                "org.apache.kafka.common.serialization.ByteArraySerializer",
                config.get("value.serializer"));
    }

    /** No compression key at all when none was named, rather than a null the client rejects. */
    @Test
    void compressionIsAbsentRatherThanNull() {
        assertTrue(!Topics.producerConfig(ProducerSettings.from(Map.<String, String>of()::get))
                .containsKey("compression.type"));
    }
}
