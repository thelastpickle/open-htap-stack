package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * The Spark master's web UI, which the platform probe reads and nothing else does.
 *
 * <p>The Thrift Server the two Spark access paths talk to is a different port, and the
 * application UI the Health page reads jobs from is a third; both arrive with the paths
 * that need them.
 */
@ConfigMapping(prefix = "spark")
public interface SparkSettings {

    @WithDefault("spark")
    String uiHost();

    @WithDefault("8080")
    int uiPort();
}
