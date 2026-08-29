package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;

/**
 * The three Spark ports this backend uses, which are three different servers.
 *
 * <p>The master's web UI at 8080 is what the platform probe reads. The Thrift Server at 10000
 * is what both Spark access paths run their SQL against. The application UI at 4040 is the third,
 * and it is where a running job is seen and killed.
 */
@ConfigMapping(prefix = "spark")
public interface SparkSettings {

    @WithDefault("spark")
    String uiHost();

    @WithDefault("8080")
    int uiPort();

    @WithDefault("spark")
    String thriftHost();

    @WithDefault("10000")
    int thriftPort();

    /**
     * The Thrift Server's own application UI, on the same host as the master's.
     *
     * <p>4040 is where the first application in a JVM binds, and the Thrift Server is the only
     * application in that container, so the default is the whole answer here as it was in the
     * Python. {@code SPARK_APP_UI_PORT} overrides it; compose declares no such variable, and a
     * container that needs to is the one whose Spark binds elsewhere.
     */
    @WithName("app-ui-port")
    @WithDefault("4040")
    int appUiPort();

    /**
     * How long the client waits on a socket that is saying nothing before it gives up.
     *
     * <p>A Spark job has no other deadline, so this is what stops a stuck query hanging the
     * dashboard. It bounds how long the server may go without answering, which is not a budget
     * for how long the query may take. Set for the contended case and not the typical one: a
     * scan of the whole history that answers in 113 s alone was still working after 180 s with
     * the three other paths beside it. The bulk reader derives its snapshot lifetime from this
     * value, and nginx allows 3600 s in front of the backend, so a longer setting than that
     * is the browser's deadline rather than this one.
     *
     * <p>Seconds and an {@code int}, so the property name is the {@code SPARK_QUERY_TIMEOUT_S}
     * compose already declares and the value is the bare integer it already carries. A {@code
     * Duration} here would rest on the converter reading {@code 900} as seconds rather than
     * refusing it, and the CI step that asserts a snapshot outlives its read takes the figure
     * from that variable.
     */
    @WithName("query-timeout-s")
    @WithDefault("900")
    int queryTimeoutSeconds();

    default Duration queryTimeout() {
        return Duration.ofSeconds(queryTimeoutSeconds());
    }
}
