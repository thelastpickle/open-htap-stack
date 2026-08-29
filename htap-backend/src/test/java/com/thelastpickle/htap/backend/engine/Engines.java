package com.thelastpickle.htap.backend.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.config.PrestoSettings;
import com.thelastpickle.htap.backend.config.SparkSettings;
import java.time.Clock;

/**
 * The two engine clients a test builds, and the settings they read.
 *
 * <p>Public and in this package because the clients' constructors are package-private: the cancel and
 * the Health page live in other packages and both need a client pointed at a server of the test's own,
 * so the seam belongs here rather than being widened on each client.
 */
public final class Engines {

    /** The user the paths connect as, which is what a cancel filters this backend's own queries by. */
    public static final String PRESTO_USER = "htap-mission-control";

    private Engines() {}

    /** A coordinator on the loopback address, at a port a test's own server is listening on. */
    public static PrestoSettings prestoAt(int port) {
        return new PrestoSettings() {
            @Override
            public String host() {
                return "127.0.0.1";
            }

            @Override
            public int port() {
                return port;
            }

            @Override
            public String user() {
                return PRESTO_USER;
            }

            @Override
            public String catalog() {
                return "cassandra";
            }

            @Override
            public String schema() {
                return "demo";
            }
        };
    }

    /**
     * A Spark on the loopback address, with its application UI at a port a test is listening on.
     *
     * <p>The application UI is the one server anything built here dials; the master UI's port and the
     * Thrift Server's keep their compose values, because no client here reads either.
     */
    public static SparkSettings sparkAt(int appUiPort) {
        return spark(appUiPort, 900);
    }

    /** The same, with a query timeout a test chose: the snapshot lifetime is derived from it. */
    public static SparkSettings spark(int appUiPort, int queryTimeoutSeconds) {
        return new SparkSettings() {
            @Override
            public String uiHost() {
                return "127.0.0.1";
            }

            @Override
            public int uiPort() {
                return 8080;
            }

            @Override
            public String thriftHost() {
                return "127.0.0.1";
            }

            @Override
            public int thriftPort() {
                return 10_000;
            }

            @Override
            public int appUiPort() {
                return appUiPort;
            }

            @Override
            public int queryTimeoutSeconds() {
                return queryTimeoutSeconds;
            }
        };
    }

    public static PrestoQueries prestoQueries(int port, Clock clock) {
        return new PrestoQueries(prestoAt(port), new ObjectMapper(), clock);
    }

    public static SparkUi sparkUi(int appUiPort, Clock clock) {
        return new SparkUi(sparkAt(appUiPort), new ObjectMapper(), clock);
    }
}
