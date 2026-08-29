package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;

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

    /** One of the three keyspaces cassandra-sql keeps its own rows in, and the one it answers as. */
    @WithDefault("cassandra_sql")
    String database();

    /** A label rather than a credential: the service authenticates nobody. */
    @WithDefault("htap-mission-control")
    String user();

    @WithDefault("5s")
    Duration connectTimeout();
}
