package com.thelastpickle.htap.backend.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.SqlConsoleResult;
import com.thelastpickle.htap.backend.api.dto.SqlConsoleStatus;
import com.thelastpickle.htap.backend.api.dto.SqlQuirk;
import com.thelastpickle.htap.backend.api.dto.SqlStatementResult;
import com.thelastpickle.htap.backend.sql.ConsoleFakes.ScriptedClient;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** What each route runs, and how a refused statement becomes a field rather than a failure. */
class SqlConsoleTest {

    @Test
    void aStatementsAnswerIsCarriedThrough() {
        ScriptedClient client = new ScriptedClient()
                .queue(ConsoleFakes.answer(List.of("n"), List.of(List.of("8"))));
        SqlConsoleResult result = console(client).execute("SELECT COUNT(*) AS n FROM drones;");

        assertEquals("cassandra-sql", result.engine());
        assertEquals(List.of("SELECT COUNT(*) AS n FROM drones;"), client.ran);
        assertEquals(1, result.statements().size());
        SqlStatementResult only = result.statements().getFirst();
        assertEquals(List.of("n"), only.columns());
        assertEquals(List.of(List.of("8")), only.rows());
        assertEquals(1, only.rowCount());
        assertNull(only.error());
        assertEquals(0, result.errorCount());
    }

    /**
     * A refusal is a field, and the batch continues past it.
     *
     * <p>Which is what a reset needs: its two {@code DROP TYPE} statements are refused on a stack
     * whose ENUM types are absent, and the twenty-two after them are the ones that matter.
     */
    @Test
    void aRefusedStatementIsReportedAndTheBatchContinues() {
        ScriptedClient client = new ScriptedClient()
                .queue(new SQLException("DROP TYPE failed: ENUM type does not exist"))
                .always(ConsoleFakes.oneCell("1"));
        SqlConsoleResult result = console(client).reset();

        assertEquals(
                ConsoleSchema.RESET.size() + ConsoleSchema.schemaAndSeed().size(),
                result.statements().size());
        assertEquals(1, result.errorCount());
        assertEquals(
                "DROP TYPE failed: ENUM type does not exist",
                result.statements().getFirst().error());
        assertEquals(0, result.statements().getFirst().rowCount());
        assertNull(result.statements().get(1).error());
    }

    /** A driver that raises something other than a {@code SQLException} is reported the same way. */
    @Test
    void aRuntimeFailureIsReportedRatherThanThrown() {
        ScriptedClient client = new ScriptedClient()
                .queue(new IllegalStateException("Received resultset tuples"));
        SqlConsoleResult result = console(client).execute("SELECT 1");

        assertEquals(1, result.errorCount());
        assertEquals("Received resultset tuples", result.statements().getFirst().error());
    }

    /** The count is the rows the statement produced; the rows carried are the ones the page can hold. */
    @Test
    void aLargeResultIsTruncatedAndSaysSo() {
        List<List<String>> rows = new ArrayList<>();
        for (int i = 0; i < SqlConsole.MAX_ROWS + 7; i++) {
            rows.add(List.of(Integer.toString(i)));
        }
        ScriptedClient client = new ScriptedClient().queue(ConsoleFakes.answer(List.of("n"), rows));
        SqlStatementResult only =
                console(client).execute("SELECT * FROM flight_legs;").statements().getFirst();

        assertEquals(SqlConsole.MAX_ROWS, only.rows().size());
        assertEquals(SqlConsole.MAX_ROWS + 7, only.rowCount());
        assertEquals(List.of("0"), only.rows().getFirst());
    }

    @Test
    void aResetDropsBeforeItCreates() {
        ScriptedClient client = new ScriptedClient().always(ConsoleFakes.oneCell("1"));
        console(client).reset();

        List<String> expected = new ArrayList<>(ConsoleSchema.RESET);
        expected.addAll(ConsoleSchema.schemaAndSeed());
        assertEquals(expected, client.ran);
    }

    @Test
    void theSchemaRouteSendsTheSchemaThenTheSeed() {
        ScriptedClient client = new ScriptedClient().always(ConsoleFakes.oneCell("1"));
        console(client).createSchema();
        assertEquals(ConsoleSchema.schemaAndSeed(), client.ran);
    }

    /** One statement per table, because a {@code UNION ALL} fails whole if one table is empty. */
    @Test
    void oneCountIsAskedPerTable() {
        ScriptedClient client = new ScriptedClient().always(ConsoleFakes.oneCell("5"));
        SqlConsoleResult result = console(client).tableCounts();

        assertEquals(ConsoleSchema.counts(), client.ran);
        assertEquals(ConsoleSchema.TABLES.size(), result.statements().size());
    }

    /** Each defect is run beside its control, in that order, so a page shows the pair. */
    @Test
    void everyQuirkRunsItsProbeAndThenItsControl() {
        ScriptedClient client = new ScriptedClient().always(ConsoleFakes.oneCell("21.4"));
        List<SqlQuirk> quirks = console(client).quirks();

        assertEquals(ConsoleQuirks.ALL.size(), quirks.size());
        List<String> expected = ConsoleQuirks.ALL.stream()
                .flatMap(quirk -> List.of(quirk.probe(), quirk.control()).stream())
                .toList();
        assertEquals(expected, client.ran);
        SqlQuirk first = quirks.getFirst();
        assertEquals(ConsoleQuirks.ALL.getFirst().id(), first.id());
        assertEquals(ConsoleQuirks.ALL.getFirst().probe(), first.probe().sql());
        assertEquals(ConsoleQuirks.ALL.getFirst().control(), first.control().sql());
    }

    @Test
    void theStatusNamesTheServiceAndItsThreeOwnKeyspaces() {
        ScriptedClient client = new ScriptedClient();
        SqlConsoleStatus status = new SqlConsole(client, ConsoleFakes.settings("sql-host", 5555))
                .status();

        assertEquals("cassandra-sql", status.engine());
        assertTrue(status.connected());
        assertEquals("sql-host", status.host());
        assertEquals(5555, status.port());
        assertEquals("cassandra_sql", status.database());
        assertEquals(
                List.of("cassandra_sql", "cassandra_sql_internal", "pg_catalog"),
                status.keyspaces());
    }

    /**
     * The status route does not re-prove a connection a statement is using.
     *
     * <p>Proving it takes the client's own lock, so asking while a batch is in flight would wait that
     * batch out and report nothing sooner.
     */
    @Test
    void theStatusDoesNotWaitForAStatementInFlight() {
        ScriptedClient busy = new ScriptedClient();
        busy.reportsBusy = true;
        busy.reportsConnected = true;
        assertTrue(console(busy).status().connected());

        ScriptedClient gone = new ScriptedClient();
        gone.reportsConnected = false;
        assertFalse(console(gone).status().connected());
    }

    @Test
    void readyAndAddressAnswerWhatTheRouteRefusesWith() {
        ScriptedClient client = new ScriptedClient();
        SqlConsole console = new SqlConsole(client, ConsoleFakes.settings("sql-host", 5555));
        assertTrue(console.ready());
        client.ready = false;
        assertFalse(console.ready());
        assertEquals("sql-host:5555", console.address());
    }

    /** The statement reaches the service as it was written: no splitting, and nothing added. */
    @Test
    void theStatementIsPassedThroughUnchanged() {
        ScriptedClient client = new ScriptedClient().always(SqlAnswer.NOTHING);
        String transaction = ConsolePresets.ALL.getFirst().sql();
        console(client).execute(transaction);
        assertEquals(List.of(transaction), client.ran);
    }

    private static SqlConsole console(ScriptedClient client) {
        return new SqlConsole(client, ConsoleFakes.settings());
    }
}
