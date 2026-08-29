package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Where the Presto coordinator is. The query path itself arrives in the next commit. */
@ConfigMapping(prefix = "presto")
public interface PrestoSettings {

    @WithDefault("presto")
    String host();

    @WithDefault("8080")
    int port();
}
