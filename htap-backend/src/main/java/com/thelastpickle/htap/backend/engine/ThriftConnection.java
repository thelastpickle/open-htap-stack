package com.thelastpickle.htap.backend.engine;

import com.thelastpickle.htap.backend.config.SparkSettings;
import com.thelastpickle.htap.backend.support.Messages;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.logging.Logger;

/**
 * One HiveServer2 connection to the Spark Thrift Server, serialised, and the cancel that
 * reaches a statement running on it.
 *
 * <p>The two Spark paths hold one of these each, so each has its own HiveServer2 session and
 * they can run at the same time; the comparison offers that on purpose, so the contention
 * between the two can be seen rather than described. Each session carries only the views its
 * own path reads.
 *
 * <p>{@code label} names the connection in the log, which is how the two are told apart.
 */
final class ThriftConnection {

    private static final Logger LOG = Logger.getLogger(ThriftConnection.class);

    /**
     * What a cancelled statement reports.
     *
     * <p>The Python said the connection had been taken away and would be rebuilt, because its
     * only way to stop a statement was to shut the socket under the thread waiting on it. This
     * one sends the protocol's own cancel, so the session survives and the sentence changed
     * with the mechanism.
     */
    static final String CANCELLED_MESSAGE =
            "Cancelled: Spark was asked to stop this statement. The session is still open, so "
                    + "the next query needs no reconnect.";

    /** A listener either answers or does not, so connecting is not given a long budget. */
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    /** How much longer the socket waits than the server-side timeout it is guarding. */
    private static final Duration SOCKET_MARGIN = Duration.ofSeconds(30);

    /**
     * The session user, which the Thrift Server shows against the session and the jobs it runs.
     *
     * <p>A label rather than a credential: the server runs with NOSASL and asks for no password.
     * Not a setting, because nothing would set it.
     */
    private static final String SESSION_USER = "htap-mission-control";

    /**
     * Errors that mean this session's views want rebuilding, because the session was recycled
     * or the table behind a view was replaced since it was registered.
     */
    private static final String[] STALE_VIEW_MARKERS = {
        "TABLE_OR_VIEW_NOT_FOUND", "cannot be found", "does not exist", "UNRESOLVED_"
    };

    /** What one statement does with the connection, once this class has serialised it. */
    @FunctionalInterface
    private interface Work<T> {
        T on(Statement statement) throws SQLException;
    }

    /**
     * Where a connection comes from: the JDBC driver, or a stand-in a test supplies.
     *
     * <p>The seam is here rather than in the paths above, so a test of one of them exercises this
     * class's own locking and failure mapping instead of a substitute for it.
     */
    @FunctionalInterface
    interface ConnectionSource {
        Connection open(String url, Properties properties) throws SQLException;
    }

    private final String label;
    private final SparkSettings settings;
    private final ConnectionSource source;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile Connection connection;

    /**
     * The statement in flight, for {@link #abort()} to cancel from another thread.
     *
     * <p>Read outside the lock, and it has to be: the thread that would release the lock is the
     * one being interrupted. The driver runs a statement asynchronously and polls the server for
     * its state, so a cancel issued here takes the driver's own transport lock between two of
     * those polls rather than waiting out the whole statement.
     */
    private volatile Statement running;

    private volatile boolean aborted;

    ThriftConnection(String label, SparkSettings settings) {
        this(label, settings, DriverManager::getConnection);
    }

    ThriftConnection(String label, SparkSettings settings, ConnectionSource source) {
        this.label = label;
        this.settings = settings;
        this.source = source;
    }

    /** True while a statement is in flight, which a caller asks before offering to reconnect. */
    boolean busy() {
        return lock.isLocked();
    }

    void open() throws SQLException {
        Properties properties = new Properties();
        // Names from the driver's JdbcConnectionParams, whose constants for the last three are
        // package-private, so they are spelled out here. auth/noSasl matches the
        // hive.server2.authentication=NOSASL the spark service starts its Thrift Server with,
        // and the two timeouts are milliseconds in this driver although Hive's own reads the
        // same property as seconds: established by disassembly, it hands the value to TSocket,
        // which passes it to Socket.setSoTimeout.
        properties.setProperty("auth", "noSasl");
        properties.setProperty("user", SESSION_USER);
        properties.setProperty("connectTimeout", Long.toString(CONNECT_TIMEOUT.toMillis()));
        // The transport outlives the server-side timeout by a margin, so a statement the server
        // gives up on reports the server's own message rather than dying as a socket read.
        properties.setProperty(
                "socketTimeout",
                Long.toString(settings.queryTimeout().plus(SOCKET_MARGIN).toMillis()));
        String url =
                "jdbc:hive2://%s:%d/default".formatted(settings.thriftHost(), settings.thriftPort());
        Connection opened = source.open(url, properties);
        Connection previous = connection;
        connection = opened;
        aborted = false;
        close(previous);
        LOG.infof("Spark Thrift Server connected (%s): %s", label, url);
    }

    /** Run one statement and read its rows. */
    QueryRows query(String sql) {
        return run(statement -> {
            try (ResultSet rows = statement.executeQuery(sql)) {
                return JdbcRows.read(rows, true);
            }
        });
    }

    /**
     * Run one statement for its effect.
     *
     * <p>Spark answers a {@code CREATE VIEW} with a described column and no rows, a shape the
     * Python's client could not fetch at all. This driver can, and reading it would still be
     * reading nothing.
     */
    void ddl(String sql) {
        run(statement -> {
            statement.execute(sql);
            return null;
        });
    }

    private <T> T run(Work<T> work) {
        lock.lock();
        try {
            Connection current = connection;
            if (current == null) {
                throw new EngineUnavailable("Spark Thrift Server not connected (" + label + ")");
            }
            aborted = false;
            try (Statement statement = current.createStatement()) {
                // The server's own bound on the statement, which the Python had no way to set:
                // its only guard was the socket timeout, so a job that never finished was
                // reported as a server that had gone quiet.
                statement.setQueryTimeout((int) settings.queryTimeout().toSeconds());
                running = statement;
                return work.on(statement);
            } finally {
                running = null;
            }
        } catch (SQLException e) {
            throw new EngineFailed(failureMessage(e), e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Ask the server to stop the statement in flight.
     *
     * <p>False when there was nothing running. The cancel goes over the same connection, which
     * is what lets it be issued from another thread at all: the driver polls for the statement's
     * state rather than blocking on one call, so this waits for one poll rather than for the
     * statement.
     *
     * <p>The Python instead shut the socket down under the waiting thread, which freed the
     * dashboard and left the session behind. Cancelling frees the dashboard too, and Spark still
     * carries on with a job whose operation was cancelled, so whoever cancels also kills the job
     * group.
     */
    boolean abort() {
        Statement current = running;
        if (current == null) {
            return false;
        }
        aborted = true;
        try {
            current.cancel();
            LOG.infof("Spark statement cancelled (%s)", label);
            return true;
        } catch (SQLException e) {
            LOG.warnf("Spark cancel failed (%s): %s", label, e);
            return false;
        }
    }

    /** Whether the failure that just arrived was this connection's own cancel. */
    boolean wasAborted() {
        return aborted;
    }

    /**
     * Whether the failure means the connection rather than the statement.
     *
     * <p>By the cause's simple name, because the transport exception the driver raises lives in
     * its own shaded package: importing it would name an internal that a repackaging is free to
     * move, where the name it was given has not changed since Thrift 0.9.
     */
    static boolean connectionIsGone(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof java.io.IOException
                    || cause.getClass().getSimpleName().equals("TTransportException")) {
                return true;
            }
        }
        return false;
    }

    /** Whether the failure means this session's views rather than the statement. */
    static boolean viewsAreStale(RuntimeException failure) {
        String text = String.valueOf(failure.getMessage());
        for (String marker : STALE_VIEW_MARKERS) {
            if (text.contains(marker)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The failure as a sentence.
     *
     * <p>Short, because this driver already reports the server's message: the Python's client
     * handed back the whole Thrift response, a wall of JVM frames with the message quoted inside
     * it, and had to pull the message out with two regular expressions.
     */
    private String failureMessage(SQLException failure) {
        if (aborted) {
            return CANCELLED_MESSAGE;
        }
        String message = Messages.oneLine(failure.getMessage());
        return message.isEmpty()
                ? failure.getClass().getSimpleName() + " (" + label + ")"
                : message;
    }

    void discard() {
        Connection current = connection;
        connection = null;
        close(current);
    }

    private void close(Connection target) {
        if (target == null) {
            return;
        }
        try {
            target.close();
        } catch (SQLException e) {
            LOG.debugf("Spark connection close failed (%s): %s", label, e);
        }
    }
}
