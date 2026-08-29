package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Where cassandra-sql listens on the Postgres wire protocol.
 *
 * <p>5432 is not a setting on the other side: it is a {@code private static final} in that
 * service's {@code PostgresProtocolServer}, so it is named here only to be addressed.
 */
@ConfigMapping(prefix = "accord-sql")
public interface AccordSqlSettings {

    @WithDefault("accord-sql")
    String host();

    @WithDefault("5432")
    int port();
}
