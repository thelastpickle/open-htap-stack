package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Where the Presto coordinator is, and which catalog and schema a statement resolves in. */
@ConfigMapping(prefix = "presto")
public interface PrestoSettings {

    @WithDefault("presto")
    String host();

    @WithDefault("8080")
    int port();

    /**
     * The user a query runs as. Presto requires one and this cluster authenticates nobody, so
     * the value is a label in the coordinator's query list rather than a credential.
     */
    @WithDefault("htap-mission-control")
    String user();

    /** The Cassandra connector's catalog, configured in the coordinator's own properties. */
    @WithDefault("cassandra")
    String catalog();

    @WithDefault("demo")
    String schema();
}
