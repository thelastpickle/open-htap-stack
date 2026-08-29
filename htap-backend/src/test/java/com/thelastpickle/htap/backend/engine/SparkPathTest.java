package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.config.SparkSettings;
import java.lang.reflect.Proxy;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * What a failed Spark statement does to the session and to the views.
 *
 * <p>Three outcomes, chosen from the failure alone: a dead connection takes the path down, a stale
 * view is re-registered and the statement retried once, and any other failure is the statement's
 * own business. The connection is a proxy rather than a stand-in for {@link ThriftConnection}, so
 * the branch is exercised through that class's own locking and failure mapping.
 */
class SparkPathTest {

    private static final String VIEW_DDL = "CREATE OR REPLACE TEMP VIEW";

    /** Every statement the server was asked to run, view registrations included, in order. */
    private final List<String> asked = new ArrayList<>();

    /** What the next {@code executeQuery} raises; an empty queue answers rows. */
    private final Deque<SQLException> failures = new ArrayDeque<>();

    /**
     * A missing view is the one failure worth registering again for, and the retry is once: the
     * second attempt's own failure is the caller's, or there would be no bound on the loop.
     */
    @Test
    void aStaleViewIsReRegisteredAndTheStatementRetried() {
        failures.add(new SQLException("[TABLE_OR_VIEW_NOT_FOUND] The table or view `events`"));
        SparkPath path = path();

        QueryRows rows = path.query("SELECT event_type FROM events");

        assertEquals(List.of("event_type"), rows.columns());
        assertEquals(6, registrations(), "the three views are registered twice");
        assertEquals(2, queries(), "the statement is run again after the registration");
        assertTrue(path.connected());
    }

    /**
     * A transport failure means the session, so the connection is dropped and the path reports
     * itself down; the next read reconnects rather than running on a session that is gone.
     */
    @Test
    void aDeadConnectionTakesThePathDown() {
        failures.add(new SQLException("transport", new SocketException("connection reset")));
        SparkPath path = path();

        assertThrows(EngineFailed.class, () -> path.query("SELECT 1"));

        assertFalse(path.connected());
        assertEquals(3, registrations(), "a dead session is no reason to register views again");
    }

    /**
     * Anything else is the statement's own refusal. Retrying registration there would re-enter the
     * code path that resolves a table definition, which is where the failures come from.
     */
    @Test
    void anyOtherFailureIsRaisedWithTheViewsLeftAlone() {
        failures.add(new SQLException("Job aborted due to stage failure"));
        SparkPath path = path();

        assertEquals(
                "Job aborted due to stage failure",
                assertThrows(EngineFailed.class, () -> path.query("SELECT 1")).getMessage());

        assertEquals(3, registrations());
        assertEquals(1, queries());
        assertTrue(path.connected());
    }

    /** One view per table, under the table's own name, with the keyspace named in its options. */
    @Test
    void theThreeConnectorViewsAreRegisteredOnConnecting() {
        path().connect();

        assertEquals(3, registrations());
        assertTrue(
                asked.contains(
                        VIEW_DDL + " events USING org.apache.spark.sql.cassandra "
                                + "OPTIONS (keyspace 'demo', table 'events')"),
                asked.toString());
    }

    /** The connector's views carry the table's own name, so the statement is left unprefixed. */
    @Test
    void theDialectNamesTheTablesAndBoundsTheStatement() {
        assertEquals(
                "SELECT * FROM events LIMIT 10",
                path().dialect("SELECT * FROM events ALLOW FILTERING", 10));
    }

    /** Nothing in flight is nothing to stop, which the Health page's cancel asks before reporting. */
    @Test
    void withNothingRunningThereIsNothingToStop() {
        assertFalse(path().abort());
    }

    private long registrations() {
        return asked.stream().filter(sql -> sql.startsWith(VIEW_DDL)).count();
    }

    private long queries() {
        return asked.stream().filter(sql -> !sql.startsWith(VIEW_DDL)).count();
    }

    private SparkPath path() {
        return new SparkPath(cassandra(), new ThriftConnection("test", spark(), this::connection));
    }

    /**
     * A connection whose statements record what they were asked and answer from {@link #failures}.
     *
     * <p>One column and no rows for a statement that succeeds: what the reader does with rows is
     * {@link JdbcRowsTest}'s question, and this one is about which failures reach which branch.
     */
    private Connection connection(String url, Properties properties) {
        Statement statement = (Statement) Proxy.newProxyInstance(
                SparkPathTest.class.getClassLoader(),
                new Class<?>[] {Statement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setQueryTimeout", "close" -> null;
                    case "execute" -> {
                        asked.add((String) args[0]);
                        yield false;
                    }
                    case "executeQuery" -> {
                        asked.add((String) args[0]);
                        SQLException failure = failures.poll();
                        if (failure != null) {
                            throw failure;
                        }
                        yield Jdbc.resultSet(List.of("event_type"), List.of());
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (Connection) Proxy.newProxyInstance(
                SparkPathTest.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "createStatement" -> statement;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static CassandraSettings cassandra() {
        return (CassandraSettings) Proxy.newProxyInstance(
                SparkPathTest.class.getClassLoader(),
                new Class<?>[] {CassandraSettings.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "keyspace" -> "demo";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static SparkSettings spark() {
        return (SparkSettings) Proxy.newProxyInstance(
                SparkPathTest.class.getClassLoader(),
                new Class<?>[] {SparkSettings.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "thriftHost" -> "127.0.0.1";
                    case "thriftPort" -> 1;
                    case "queryTimeoutSeconds" -> 900;
                    case "queryTimeout" -> Duration.ofSeconds(900);
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
