package com.thelastpickle.htap.backend.sql;

import com.thelastpickle.htap.backend.config.AccordSqlSettings;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import org.jboss.logging.Logger;

/**
 * cassandra-sql over the Postgres wire protocol: one connection, serialised by a lock.
 *
 * <p>This is a sixth interface and not a sixth access path. cassandra-sql keeps SQL rows in three
 * keyspaces of its own under an ordered key-value encoding of its own, so it cannot read a table
 * the sink wrote and it belongs in no comparison; that is why the class sits here rather than in
 * {@code engine}, where every class implements {@code QueryPath}. What it demonstrates instead is
 * the thing none of the five can do: a multi-statement transaction that commits or does not.
 *
 * <p>Three measurements shape the class, and each is a defect of the service rather than a
 * preference. A bound parameter silently returns no rows, so {@link #execute} takes one complete
 * SQL string and binds nothing. Every value arrives as text. And a statement may be a whole
 * transaction, so the string is passed through as it stands and the connection is left in
 * autocommit, which makes the SQL's own {@code BEGIN} the only transaction there is.
 */
@ApplicationScoped
public class SqlConsoleClient {

    private static final Logger LOG = Logger.getLogger(SqlConsoleClient.class);

    /**
     * A statement the parser accepts that touches no table, for proving a connection.
     *
     * <p>{@code SELECT 1 AS one} is not it: the alias is refused with "Table does not exist:
     * unknown", which is this parser's quirk about that one identifier rather than anything about
     * the protocol, since {@code SELECT 1 AS ok} and a bare {@code SELECT 1} both answer.
     */
    static final String PROBE_SQL = "SELECT 1";

    private final AccordSqlSettings settings;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile Connection connection;
    private volatile boolean connected;

    SqlConsoleClient(AccordSqlSettings settings) {
        this.settings = settings;
    }

    public boolean connected() {
        return connected;
    }

    /**
     * True while a statement is in flight.
     *
     * <p>Worth asking before re-proving the connection, which takes the same lock and would
     * otherwise wait the statement out. The status route asks.
     */
    public boolean busy() {
        return lock.isLocked();
    }

    /**
     * Open a connection at startup, and never fatally.
     *
     * <p>Expected to fail on a cold stack, because the backend and cassandra-sql start together and
     * that service creates three keyspaces and thirteen tables before it answers. {@link
     * #ensureReady()} opens it on first use instead.
     */
    void onStart(@Observes StartupEvent event) {
        // On a virtual thread for the reason EngineStartup gives: cassandra-sql takes some 36
        // seconds to create its own keyspaces, and the HTTP port must not wait for it.
        Thread.ofVirtual().name("sql-console-connect").start(this::connect);
    }

    public void connect() {
        lock.lock();
        try {
            open();
        } catch (SQLException e) {
            drop();
            LOG.infof("cassandra-sql not connected yet: %s", e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Prove the connection with a round trip before a caller runs a batch over it.
     *
     * <p>Measured rather than assumed: without the round trip, every statement of the first batch
     * after the service restarted failed with "not connected", because a dead socket is only
     * discovered by the statement that uses it and the statements after that one found no
     * connection at all. {@code SELECT 1} answers in about 2 ms.
     */
    public boolean ensureReady() {
        lock.lock();
        try {
            if (connection != null) {
                try {
                    probe(connection);
                    return true;
                } catch (SQLException stale) {
                    LOG.debugf("cassandra-sql connection had gone: %s", stale.getMessage());
                    drop();
                }
            }
            open();
            return true;
        } catch (SQLException e) {
            drop();
            LOG.warnf("cassandra-sql connection failed: %s", e.getMessage());
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Run one SQL string and report its last result.
     *
     * <p>The string may hold several statements separated by semicolons, which is how a
     * {@code BEGIN}/{@code COMMIT} transaction is sent, and the last one decides what comes back.
     * That is what makes the rollback preset work: its closing {@code SELECT} is what the caller
     * sees, and it returns no row.
     */
    SqlAnswer execute(String sql) throws SQLException {
        lock.lock();
        try {
            if (connection == null) {
                // Opened here rather than refused, so one failed statement does not fail every
                // statement after it in the same batch.
                open();
            }
            long started = System.nanoTime();
            try (Statement statement = connection.createStatement()) {
                SqlAnswer answer = lastResult(statement, sql);
                return answer.withDuration((System.nanoTime() - started) / 1_000_000.0);
            } catch (SQLException e) {
                // Only a connection-class failure costs the connection. A refused statement is the
                // routine case on these routes rather than the exception -- a reset always has its
                // two DROP TYPE refusals and /tables one per empty table -- and dropping on those
                // would rebuild the connection once per refusal, which is a fresh
                // DriverManager.getConnection and a probe, not a probe alone.
                if (connectionLost(e)) {
                    drop();
                }
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Whether this failure means the connection is gone rather than the statement refused.
     *
     * <p>SQLState class 08 is the standard's connection-exception class, which pgjdbc uses:
     * {@code 08006} for a connection failure and {@code 08003} for one already closed, against class
     * 42 or 22 for a server-side refusal. A failure carrying no state at all is treated as lost,
     * since that is what a driver reports when it never got an answer.
     */
    static boolean connectionLost(SQLException failure) {
        String state = failure.getSQLState();
        return state == null || state.startsWith("08");
    }

    /**
     * The last of the results a multi-statement string produced.
     *
     * <p>JDBC exposes the *first* result from {@code execute} and advances through the rest, where
     * psycopg's cursor described the last. Walking to the end is what keeps the two the same: the
     * rollback preset ends in a {@code SELECT} whose empty result is the whole point, and a reader
     * that stopped at the first result would report the {@code BEGIN} instead.
     *
     * <p>A result that is an update count clears what a result set before it held, because that is
     * what a trailing {@code UPDATE} means: nothing to show.
     */
    static SqlAnswer lastResult(Statement statement, String sql) throws SQLException {
        SqlAnswer answer = SqlAnswer.NOTHING;
        boolean isResultSet = statement.execute(sql);
        while (true) {
            if (isResultSet) {
                try (ResultSet rows = statement.getResultSet()) {
                    answer = read(rows);
                }
            } else {
                answer = SqlAnswer.NOTHING;
            }
            isResultSet = statement.getMoreResults();
            if (!isResultSet && statement.getUpdateCount() == -1) {
                return answer;
            }
        }
    }

    private static SqlAnswer read(ResultSet rows) throws SQLException {
        ResultSetMetaData metadata = rows.getMetaData();
        int width = metadata.getColumnCount();
        List<String> columns = new ArrayList<>(width);
        for (int i = 1; i <= width; i++) {
            columns.add(metadata.getColumnLabel(i));
        }
        List<List<String>> values = new ArrayList<>();
        while (rows.next()) {
            List<String> row = new ArrayList<>(width);
            for (int i = 1; i <= width; i++) {
                row.add(rows.getString(i));
            }
            values.add(row);
        }
        return new SqlAnswer(List.copyOf(columns), values, 0.0);
    }

    /** The lock is held. */
    private void open() throws SQLException {
        Connection opened = DriverManager.getConnection(url(), properties());
        try {
            probe(opened);
        } catch (SQLException e) {
            close(opened);
            throw e;
        }
        Connection previous = connection;
        connection = opened;
        connected = true;
        close(previous);
        LOG.infof("cassandra-sql connected: %s:%d", settings.host(), settings.port());
    }

    String url() {
        return "jdbc:postgresql://%s:%d/%s".formatted(settings.host(), settings.port(),
                settings.database());
    }

    Properties properties() {
        Properties properties = new Properties();
        // A label rather than a credential: the service authenticates nobody, and a password
        // property would be refused as an unused one.
        properties.setProperty("user", settings.user());
        properties.setProperty("connectTimeout",
                Long.toString(settings.connectTimeout().toSeconds()));
        // Mandatory, and measured. In the default extended mode this driver sends Parse/Bind and
        // cassandra-sql answers a DataRow with no RowDescription before it, on which the driver
        // raises java.lang.IllegalStateException: "Received resultset tuples, but no field
        // structure for them" -- not a SQLException, so no catch here would report it as a
        // statement error. Simple mode answers SELECT 1 in 9.8 ms.
        properties.setProperty("preferQueryMode", "simple");
        return properties;
    }

    private static void probe(Connection target) throws SQLException {
        try (Statement statement = target.createStatement();
                ResultSet rows = statement.executeQuery(PROBE_SQL)) {
            rows.next();
        }
    }

    /** The lock is held. */
    private void drop() {
        Connection current = connection;
        connection = null;
        connected = false;
        close(current);
    }

    private static void close(Connection target) {
        if (target == null) {
            return;
        }
        try {
            target.close();
        } catch (SQLException e) {
            LOG.debugf("cassandra-sql connection close failed: %s", e);
        }
    }

    @PreDestroy
    void shutdown() {
        lock.lock();
        try {
            drop();
        } finally {
            lock.unlock();
        }
    }
}
