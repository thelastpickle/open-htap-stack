package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;

/**
 * The broker, the topic the sink reads, and the group whose offsets say how far it has got.
 *
 * <p>19092 is the in-network listener rather than the 9092 the host reaches, so a probe
 * that answers is a broker this backend can actually use.
 *
 * <p>The topic and the group are named {@code KAFKA_EVENTS_TOPIC} and
 * {@code KAFKA_SINK_GROUP_ID} here where the Python read {@code EVENTS_TOPIC} and
 * {@code SINK_GROUP_ID}: the prefix says which broker the topic is on, and every Java
 * service reads the one spelling. Compose sets both forms while the two implementations run
 * beside each other.
 */
@ConfigMapping(prefix = "kafka")
public interface KafkaSettings {

    @WithDefault("kafka")
    String host();

    @WithDefault("19092")
    int port();

    /** What the producer writes and the sink consumes. */
    @WithDefault("demo-events")
    String eventsTopic();

    /**
     * The sink's consumer group.
     *
     * <p>Read rather than joined: the settled-window check asks the broker for this group's
     * committed offsets, and a client that joined the group would be given partitions the
     * sink is reading.
     */
    @WithDefault("demo-cassandra-sink")
    String sinkGroupId();

    /**
     * How long the admin and offset lookups wait before giving up.
     *
     * <p>Short on purpose: the settled-window check runs inside a request the compare page is
     * waiting on, and a broker that is not answering in five seconds is better reported as
     * unknown than waited for. The clients are opened per check and closed after it, which is
     * what costs {@code /api/query/window} about 520 ms of its 550.
     */
    @WithName("offsets-timeout-s")
    @WithDefault("5")
    double offsetsTimeoutSeconds();

    default Duration offsetsTimeout() {
        return Duration.ofMillis(Math.round(offsetsTimeoutSeconds() * 1000));
    }
}
