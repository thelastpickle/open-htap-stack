package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * What the path does with no cluster to talk to.
 *
 * <p>No container and no Cassandra: the settings name a closed port on the loopback address,
 * so the driver refuses at once rather than waiting out a connect timeout.
 */
class CassandraPathTest {

    /** A port nothing listens on, which the operating system refuses immediately. */
    private static final int CLOSED_PORT = 1;

    @Test
    void anUnreachableClusterLeavesThePathDisconnected() {
        CassandraPath path = new CassandraPath(settings());
        path.connect();
        assertFalse(path.connected(), "a failed connect reports itself rather than raising");
    }

    /** The re-sync button, where a failure is the answer rather than something to hide. */
    @Test
    void aForcedConnectRaises() {
        CassandraPath path = new CassandraPath(settings());
        EngineUnavailable refused = assertThrows(EngineUnavailable.class, () -> path.connect(true));
        assertTrue(
                refused.getMessage().startsWith("Cassandra connection failed"),
                "the message names the path, and said: " + refused.getMessage());
    }

    @Test
    void aReadWithNoSessionRefusesRatherThanReturningNull() {
        CassandraPath path = new CassandraPath(settings());
        assertThrows(EngineUnavailable.class, path::session);
    }

    /**
     * The failure that means the session is gone, against three that do not.
     *
     * <p>{@code NoNodeAvailableException} is the one the Python sink reported for ten hours:
     * an empty error map, meaning no host left to try. A query error must not invalidate,
     * because a wrong column name would then drop a session that works, and neither must a
     * closed connection, which the driver's own pool re-establishes.
     *
     * <p>Only the subclass is passed here, and that is what the driver allows: {@code
     * AllNodesFailedException}'s constructors are protected, and both {@code fromErrors}
     * overloads answer {@code new NoNodeAvailableException()} for an empty collection, so a
     * non-empty one over a stub {@code Node} is the only way to build the base class from
     * outside the driver.
     */
    @Test
    void onlyASessionFailureMarksThePathDisconnected() {
        assertTrue(CassandraPath.sessionIsGone(new NoNodeAvailableException()));

        assertFalse(CassandraPath.sessionIsGone(new ClosedConnectionException("connection reset")));
        assertFalse(CassandraPath.sessionIsGone(new InvalidQueryException(null, "no such column")));
        assertFalse(CassandraPath.sessionIsGone(new IllegalStateException("a defect here")));
    }

    /**
     * The wiring, and not only the predicate: a session failure passing through {@code
     * guarded} is what has to leave the path disconnected, and a query failure is what has to
     * leave it alone. Deleting the {@code invalidate()} call would keep the test above green.
     *
     * <p>A connect that does nothing is what puts the path in the state this needs: connected,
     * with no cluster behind it, which is exactly the state the ten-hour failure was.
     */
    @Test
    void aSessionFailureInUseDisconnectsThePathAndAQueryFailureDoesNot() {
        CassandraPath path = new CassandraPath(settings(), () -> {});
        path.connect();
        assertTrue(path.connected(), "the stub connect reports success");

        assertThrows(
                InvalidQueryException.class,
                () -> path.guarded(() -> {
                    throw new InvalidQueryException(null, "no such column");
                }));
        assertTrue(path.connected(), "a query error is the statement's business");

        assertThrows(
                NoNodeAvailableException.class,
                () -> path.guarded(() -> {
                    throw new NoNodeAvailableException();
                }));
        assertFalse(path.connected(), "no host left to try means the session is gone");
    }

    private static CassandraSettings settings() {
        return new CassandraSettings() {
            @Override
            public String host() {
                return "127.0.0.1";
            }

            @Override
            public int port() {
                return CLOSED_PORT;
            }

            @Override
            public String keyspace() {
                return "demo";
            }

            @Override
            public String datacenter() {
                return "datacenter1";
            }

            @Override
            public int sidecarPort() {
                return CLOSED_PORT;
            }

            @Override
            public Optional<String> translateAddressesTo() {
                return Optional.empty();
            }
        };
    }
}
