package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * The broker, for the platform probe here and for the settled-window check and the CDC
 * tail in later commits.
 *
 * <p>19092 is the in-network listener rather than the 9092 the host reaches, so a probe
 * that answers is a broker this backend can actually use.
 */
@ConfigMapping(prefix = "kafka")
public interface KafkaSettings {

    @WithDefault("kafka")
    String host();

    @WithDefault("19092")
    int port();
}
