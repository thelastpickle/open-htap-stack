package com.thelastpickle.htap.backend.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** How the client addresses cassandra-sql, and how it finds a multi-statement string's answer. */
class SqlConsoleClientTest {

    private static final List<String> COLUMNS = List.of("flight_id", "distance_km");

    @Test
    void theUrlNamesTheHost_portAndTheDatabaseItAnswersAs() {
        SqlConsoleClient client = new SqlConsoleClient(ConsoleFakes.settings("sql-host", 5555));
        assertEquals("jdbc:postgresql://sql-host:5555/cassandra_sql", client.url());
    }

    /**
     * Simple query mode is mandatory, and there is no password.
     *
     * <p>In the default extended mode the driver raises an {@code IllegalStateException} on this
     * service's answer, which no catch of {@code SQLException} would report as a statement error.
     */
    @Test
    void theConnectionAsksForSimpleQueryMode() {
        Properties properties = new SqlConsoleClient(ConsoleFakes.settings()).properties();
        assertEquals("simple", properties.getProperty("preferQueryMode"));
        assertEquals("htap-mission-control", properties.getProperty("user"));
        assertEquals("5", properties.getProperty("connectTimeout"));
        assertNull(properties.getProperty("password"), "the service authenticates nobody");
    }

    /** The alias measurement: {@code SELECT 1 AS one} is refused, so the probe carries no alias. */
    @Test
    void theProbeIsAStatementThatTouchesNoTable() {
        assertEquals("SELECT 1", SqlConsoleClient.PROBE_SQL);
    }

    @Test
    void oneSelectAnswersItsOwnRows() throws SQLException {
        Statement statement =
                ConsoleFakes.statement(COLUMNS, List.of(List.of(List.of("9001", "21.4"))));
        SqlAnswer answer = SqlConsoleClient.lastResult(statement, "SELECT flight_id, distance_km;");

        assertEquals(COLUMNS, answer.columns());
        assertEquals(List.of(List.of("9001", "21.4")), answer.rows());
    }

    @Test
    void oneInsertAnswersNothing() throws SQLException {
        Statement statement = ConsoleFakes.statement(COLUMNS, List.of(1));
        assertEquals(SqlAnswer.NOTHING, SqlConsoleClient.lastResult(statement, "INSERT ...;"));
    }

    /**
     * The rollback preset's shape: three statements that count, then the {@code SELECT} that proves
     * the write was held.
     *
     * <p>JDBC exposes the first result where psycopg's cursor described the last, so a reader that
     * stopped at the first would report the {@code BEGIN} and the page would show nothing.
     */
    @Test
    void aTransactionsClosingSelectIsWhatComesBack() throws SQLException {
        Statement statement = ConsoleFakes.statement(COLUMNS, List.of(0, 1, 0, List.of()));
        SqlAnswer answer = SqlConsoleClient.lastResult(statement, "BEGIN; INSERT; ROLLBACK; SELECT;");

        assertEquals(COLUMNS, answer.columns());
        assertEquals(List.of(), answer.rows());
    }

    /**
     * A refused statement keeps the connection; a broken one does not.
     *
     * <p>A refusal is the routine case on these routes -- a reset always has its two {@code DROP
     * TYPE} refusals and {@code /tables} one per empty table -- so dropping the connection for those
     * would rebuild it once per refusal. SQLState class 08 is the standard's connection-exception
     * class, and a failure carrying no state at all is what a driver reports when it got no answer.
     */
    @Test
    void onlyAConnectionClassFailureCostsTheConnection() {
        assertFalse(SqlConsoleClient.connectionLost(new SQLException("Aggregation failed", "42601")));
        assertFalse(SqlConsoleClient.connectionLost(new SQLException("invalid input", "22P02")));
        assertTrue(SqlConsoleClient.connectionLost(new SQLException("An I/O error", "08006")));
        assertTrue(SqlConsoleClient.connectionLost(new SQLException("closed", "08003")));
        // The shape pgjdbc actually produces when it got no answer: PSQLState.UNKNOWN_STATE is
        // constructed with "", not null, so testing for null alone would have kept a dead
        // connection.  Both are treated as unknown.
        assertTrue(SqlConsoleClient.connectionLost(new SQLException("no answer", "")));
        assertTrue(SqlConsoleClient.connectionLost(new SQLException("no state at all")));
    }

    /** A trailing update count means nothing to show, so it clears the result set before it. */
    @Test
    void aTrailingUpdateClearsTheResultBeforeIt() throws SQLException {
        Statement statement =
                ConsoleFakes.statement(COLUMNS, List.of(List.of(List.of("9001", "21.4")), 1));
        assertEquals(
                SqlAnswer.NOTHING, SqlConsoleClient.lastResult(statement, "SELECT ...; UPDATE ...;"));
    }
}
