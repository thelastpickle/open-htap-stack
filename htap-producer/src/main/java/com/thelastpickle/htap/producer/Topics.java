package com.thelastpickle.htap.producer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * Creates the events topic with the partition count the demo needs.
 *
 * <p>Best effort, and never fatal: the broker also auto-creates topics, and a topic that exists
 * already is the normal case on every restart.
 *
 * <p>The partition count is why this exists at all. The broker's {@code KAFKA_NUM_PARTITIONS} is
 * 3, and the demo wants twelve: the sink polls across every partition, and the settled-window
 * check requires each one to have passed the window's end, so more partitions is what makes that
 * check meaningful. Left to auto-creation the topic would have three.
 */
final class Topics {

    /** One {@code createTopics} call, so the arguments can be read in a test. */
    interface Creator {
        void create(NewTopic topic) throws InterruptedException, ExecutionException;
    }

    private Topics() {}

    static NewTopic wanted(String topic, int partitions, short replication) {
        return new NewTopic(topic, partitions, replication);
    }

    static void ensure(String bootstrap, NewTopic topic, Log log) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "topic-bootstrap");
        try (Admin admin = Admin.create(properties)) {
            ensure(wanted -> admin.createTopics(List.of(wanted)).all().get(), topic, log);
        }
    }

    /**
     * Creates the topic, reporting the two ordinary failures rather than raising them.
     *
     * <p>A topic that exists is the restart case. Anything else is a broker this process cannot
     * reach yet, and the send loop below will report that on its own terms; stopping here would
     * make a producer that starts before the broker fail rather than wait.
     */
    static void ensure(Creator creator, NewTopic topic, Log log) {
        try {
            creator.create(topic);
            log.say(String.format(
                    Locale.ROOT,
                    "created topic=%s partitions=%d rf=%d",
                    topic.name(),
                    topic.numPartitions(),
                    topic.replicationFactor()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.say("topic create interrupted: " + topic.name());
        } catch (ExecutionException | RuntimeException e) {
            if (causedByTopicExists(e)) {
                log.say("topic already exists: " + topic.name());
            } else {
                log.say("topic create skipped/failed (ok for demo): " + e.getMessage());
            }
        }
    }

    /**
     * Whether the whole cause chain names a topic that already exists.
     *
     * <p>The chain, and not the exception: the admin client hands the reason back inside an
     * {@code ExecutionException}, and a {@code catch} on the outer type alone would report every
     * restart as a failure.
     */
    private static boolean causedByTopicExists(Throwable failure) {
        for (Throwable at = failure; at != null; at = at.getCause()) {
            if (at instanceof TopicExistsException) {
                return true;
            }
            if (at.getCause() == at) {
                break;
            }
        }
        return false;
    }

    /** The producer's own client configuration, in one place so the send loop reads plainly. */
    static Map<String, Object> producerConfig(ProducerSettings settings) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("bootstrap.servers", settings.bootstrap());
        config.put("client.id", settings.clientId());
        config.put("linger.ms", settings.lingerMs());
        config.put("batch.size", settings.kafkaBatchSize());
        // acks=0 is lossy by design: this is generated telemetry and the demo values a sustained
        // rate over durability. The sink's own writes are where the demo makes a durability claim.
        config.put("acks", settings.acks());
        config.put("retries", 0);
        config.put("max.in.flight.requests.per.connection", 5);
        config.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        if (!settings.compression().isEmpty()) {
            config.put("compression.type", settings.compression());
        }
        return config;
    }
}
