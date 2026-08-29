package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.SocketException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/**
 * Which failures mean the connection or the views rather than the statement.
 *
 * <p>Both questions are answered from the failure alone, so no Thrift Server is needed. The
 * transport exception is matched by its simple name, which is why this test can raise one of its
 * own: the driver's lives in a shaded package that a repackaging is free to move.
 */
class ThriftConnectionTest {

    /** Named as the driver's own is, to exercise the match the shaded package forces. */
    private static final class TTransportException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private TTransportException(String message) {
            super(message);
        }
    }

    @Test
    void aTransportFailureAnywhereInTheCauseChainMeansTheConnection() {
        assertTrue(ThriftConnection.connectionIsGone(
                new EngineFailed("read timed out", new TTransportException("java.net.SocketTimeout"))));
        assertTrue(ThriftConnection.connectionIsGone(new TTransportException("closed")));
    }

    @Test
    void anIoFailureAnywhereInTheCauseChainMeansTheConnection() {
        assertTrue(ThriftConnection.connectionIsGone(
                new EngineFailed("gone", new SQLException("transport", new SocketException("reset")))));
        assertTrue(ThriftConnection.connectionIsGone(new IOException("broken pipe")));
    }

    /** A statement Spark refused leaves the session usable, so it must not read as a dead one. */
    @Test
    void aRefusedStatementDoesNotMeanTheConnection() {
        assertFalse(ThriftConnection.connectionIsGone(
                new EngineFailed("AMBIGUOUS_REFERENCE: reference `id` is ambiguous")));
        assertFalse(ThriftConnection.connectionIsGone(new SQLException("syntax error")));
    }

    @Test
    void theFourMarkersOfAStaleViewAreRecognised() {
        assertTrue(ThriftConnection.viewsAreStale(
                new EngineFailed("[TABLE_OR_VIEW_NOT_FOUND] The table or view `events`")));
        assertTrue(ThriftConnection.viewsAreStale(new EngineFailed("Table `events` cannot be found")));
        assertTrue(ThriftConnection.viewsAreStale(new EngineFailed("view demo.events does not exist")));
        assertTrue(ThriftConnection.viewsAreStale(new EngineFailed("[UNRESOLVED_COLUMN] `event_type`")));
    }

    /**
     * Only a missing view is worth re-registering for: retrying registration after any other
     * failure re-enters the code path that resolves a table definition, which is where the
     * failures come from in the first place.
     */
    @Test
    void anyOtherFailureIsNotAStaleView() {
        assertFalse(ThriftConnection.viewsAreStale(new EngineFailed("Job aborted due to stage failure")));
        assertFalse(ThriftConnection.viewsAreStale(new EngineFailed("read timed out")));
    }

    /** A failure carrying no message reads as the text "null" rather than raising here. */
    @Test
    void aFailureWithNoMessageIsNotAStaleView() {
        assertFalse(ThriftConnection.viewsAreStale(new EngineFailed(null)));
    }
}
