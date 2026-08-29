package com.thelastpickle.htap.backend.engine;

import com.thelastpickle.htap.backend.config.PrestoSettings;
import com.thelastpickle.htap.backend.query.Dialects;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
 * Presto over the Cassandra connector: full SQL and a distributed scan, still over the request
 * path.
 *
 * <p>One connection, serialised. The coordinator holds no per-connection state worth keeping
 * warm, so a pool would buy nothing here, and the comparison runs one statement per path at a
 * time anyway.
 */
@ApplicationScoped
public class PrestoPath implements QueryPath {

    private static final Logger LOG = Logger.getLogger(PrestoPath.class);

    /** As the Python's {@code RECONNECT_INTERVAL_S}. */
    static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

    /** Presto reaches its tables through the connector's catalog, so a table is {@code demo.t}. */
    static final String TABLE_PREFIX = "demo.";

    private final PrestoSettings settings;
    private final ConnectionGate gate = new ConnectionGate("Presto", RETRY_INTERVAL);
    private final ReentrantLock lock = new ReentrantLock();
    private volatile Connection connection;

    @Inject
    PrestoPath(PrestoSettings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "presto";
    }

    @Override
    public void connect(boolean force) {
        gate.connect(force, this::open);
    }

    @Override
    public boolean connected() {
        return gate.connected();
    }

    @Override
    public String dialect(String sql, int limit) {
        return Dialects.sql(sql, TABLE_PREFIX, limit);
    }

    @Override
    public QueryRows query(String sql) {
        lock.lock();
        try {
            return read(sql);
        } catch (SQLException e) {
            // Any failure drops the connection rather than reasoning about which failures are
            // fatal to it: the JDBC driver holds an HTTP client whose next request would raise
            // the same error, and re-establishing costs one `SELECT 1`.
            discard();
            gate.invalidate();
            throw new EngineFailed("Presto query failed: " + readableError(e), e);
        } finally {
            lock.unlock();
        }
    }

    private QueryRows read(String sql) throws SQLException {
        try (Statement statement = connection().createStatement();
                ResultSet rows = statement.executeQuery(sql)) {
            return JdbcRows.read(rows, false);
        }
    }

    private Connection connection() {
        if (!gate.connected()) {
            connect();
        }
        Connection current = connection;
        if (current == null) {
            throw new EngineUnavailable("Presto not connected");
        }
        return current;
    }

    private void open() throws SQLException {
        Properties properties = new Properties();
        // A label rather than a credential: the coordinator runs with no authentication and
        // shows this in its query list, which is how a scan is attributed to the dashboard.
        properties.setProperty("user", settings.user());
        String url = "jdbc:presto://%s:%d/%s/%s"
                .formatted(settings.host(), settings.port(), settings.catalog(), settings.schema());
        Connection opened = DriverManager.getConnection(url, properties);
        try (Statement statement = opened.createStatement();
                ResultSet rows = statement.executeQuery("SELECT 1")) {
            // The driver connects lazily, so `getConnection` against a dead coordinator
            // succeeds and the first real query is what fails. One trivial statement is what
            // makes a connected path mean the coordinator answered.
            rows.next();
        } catch (SQLException e) {
            close(opened);
            throw e;
        }
        Connection previous = connection;
        connection = opened;
        close(previous);
    }

    /**
     * The server's message without the driver's wrapping.
     *
     * <p>Presto's JDBC driver prefixes the coordinator's own text with the query id and its
     * error name, which the pages then show ahead of the sentence that says what was wrong.
     */
    static String readableError(SQLException failure) {
        String message = failure.getMessage();
        if (message == null || message.isBlank()) {
            return failure.getClass().getSimpleName();
        }
        int marker = message.indexOf("Query failed (");
        if (marker >= 0) {
            int end = message.indexOf(')', marker);
            if (end >= 0) {
                return message.substring(end + 1).strip().replaceFirst("^:\\s*", "");
            }
        }
        return message.strip();
    }

    private void discard() {
        Connection current = connection;
        connection = null;
        close(current);
    }

    private static void close(Connection target) {
        if (target == null) {
            return;
        }
        try {
            target.close();
        } catch (SQLException e) {
            LOG.debugf("Presto connection close failed: %s", e);
        }
    }

    @PreDestroy
    void shutdown() {
        gate.invalidate();
        discard();
    }
}
