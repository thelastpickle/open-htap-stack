package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.engine.QueryPath;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Rebuilding this backend's client for a path, one line per path.
 *
 * <p>The connect is forced, which is the whole of what this control adds: a path's ordinary connect
 * is a no-op while it believes it is connected, and a stale session is in exactly that state. A forced
 * connect raises, so the engine's own words reach the operator who pressed the button.
 */
class ReconnectionTest {

    private final FakePath cassandra = new FakePath("cassandra");
    private final FakePath presto = new FakePath("presto");
    private final FakePath cqlite = new FakePath("cqlite");

    private final Reconnection reconnection = new Reconnection(
            new QueryPaths(List.<QueryPath>of(cassandra, presto, cqlite)));

    /** What {@code all} means, and what a named target is checked against. */
    @Test
    void theTargetsAreEveryPathsNameInOrder() {
        assertEquals(List.of("cassandra", "presto", "cqlite"), reconnection.targets());
    }

    @Test
    void aPathThatCameBackSaysSo() {
        Reconnection.Outcome outcome = reconnection.reconnect(List.of("cassandra"));

        assertTrue(outcome.ok());
        assertEquals(List.of("cassandra: reconnected"), outcome.actions());
        assertEquals(1, cassandra.connects());
    }

    /** Forced, because the path already believes it is connected and would otherwise do nothing. */
    @Test
    void aPathThatBelievesItIsConnectedIsStillRebuilt() {
        reconnection.reconnect(reconnection.targets());

        assertEquals(1, cassandra.connects());
        assertEquals(1, presto.connects());
        assertEquals(1, cqlite.connects());
    }

    /** The engine's own words, since a failure here is the answer rather than something to hide. */
    @Test
    void aPathThatRefusedTheConnectionReportsWhatItSaid() {
        presto.unreachable();

        Reconnection.Outcome outcome = reconnection.reconnect(List.of("presto"));

        assertFalse(outcome.ok());
        assertEquals(List.of("presto: presto connection failed"), outcome.actions());
    }

    /**
     * A path that neither raised nor came back, which is what a connect that swallowed its own
     * failure leaves behind.
     */
    @Test
    void aPathThatCameBackDisconnectedIsStillUnreachable() {
        cqlite.disconnected();

        Reconnection.Outcome outcome = reconnection.reconnect(List.of("cqlite"));

        assertFalse(outcome.ok());
        assertEquals(List.of("cqlite: still unreachable"), outcome.actions());
    }

    /** Connecting would queue behind the statement, and a control that hangs explains nothing. */
    @Test
    void aPathWithAStatementInFlightIsNotTouched() {
        presto.busy(true);

        Reconnection.Outcome outcome = reconnection.reconnect(List.of("presto"));

        assertFalse(outcome.ok());
        assertEquals(List.of("presto: busy with a query, so stop that first"), outcome.actions());
        assertEquals(0, presto.connects());
    }

    /** One line per path, in the order asked, and one path failing does not stop the rest. */
    @Test
    void everyPathReportsAndOneFailingDoesNotStopTheOthers() {
        presto.unreachable();

        Reconnection.Outcome outcome = reconnection.reconnect(reconnection.targets());

        assertFalse(outcome.ok());
        assertEquals(
                List.of("cassandra: reconnected", "presto: presto connection failed",
                        "cqlite: reconnected"),
                outcome.actions());
    }
}
