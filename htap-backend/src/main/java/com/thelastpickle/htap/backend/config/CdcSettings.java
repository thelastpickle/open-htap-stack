package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;

/**
 * The topic the Sidecar's Change Data Capture (CDC) publisher writes to, and the registry holding
 * the Avro schema those records name.
 *
 * <p>Apicurio rather than Confluent's registry, which is under the Confluent Community License; its
 * Confluent-compatible endpoint is {@code /apis/ccompat/v7}, and the one subject
 * {@code cdc-mutations-value} holds one schema.
 *
 * <p>The broker is not here: the tail reads it from {@link KafkaSettings}, which is the same broker
 * the sink consumes and the settled-window check asks about.
 */
@ConfigMapping(prefix = "cdc")
public interface CdcSettings {

    @WithDefault("cdc-mutations")
    String topic();

    @WithName("schema-registry-url")
    @WithDefault("http://apicurio:8080/apis/ccompat/v7")
    String schemaRegistryUrl();

    /**
     * How many records the tail keeps.
     *
     * <p>The whole of the page's memory bound: the oldest record leaves as the newest arrives, so a
     * page left open overnight holds no more than one just opened. It also bounds one poll, which
     * is asked for no more records than the buffer can hold.
     */
    @WithDefault("200")
    int bufferSize();

    @WithName("poll-timeout-s")
    @WithDefault("1.0")
    double pollTimeoutSeconds();

    default Duration pollTimeout() {
        return Duration.ofMillis(Math.round(pollTimeoutSeconds() * 1000));
    }

    /** The registry's base address with no trailing slash, since every path here adds one. */
    default String registry() {
        String url = schemaRegistryUrl();
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    /** {@code {topic}-value}, which is the subject name a Confluent serializer registers under. */
    default String subject() {
        return topic() + "-value";
    }
}
