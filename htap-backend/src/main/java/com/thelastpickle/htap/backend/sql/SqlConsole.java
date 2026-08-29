package com.thelastpickle.htap.backend.sql;

import com.thelastpickle.htap.backend.api.dto.SqlConsoleResult;
import com.thelastpickle.htap.backend.api.dto.SqlConsoleStatus;
import com.thelastpickle.htap.backend.api.dto.SqlPreset;
import com.thelastpickle.htap.backend.api.dto.SqlQuirk;
import com.thelastpickle.htap.backend.api.dto.SqlStatementResult;
import com.thelastpickle.htap.backend.config.AccordSqlSettings;
import com.thelastpickle.htap.backend.support.Round;
import jakarta.enterprise.context.ApplicationScoped;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

/** What each console route runs, and how a refused statement becomes a field rather than a failure. */
@ApplicationScoped
public class SqlConsole {

    /**
     * The largest result carried back to the page.
     *
     * <p>cassandra-sql widens its own scan limit to 100,000 rows whenever {@code ORDER BY} is
     * present, so an unbounded {@code SELECT} here is not a small thing to send to a browser.
     */
    static final int MAX_ROWS = 500;

    /** Named so the page can say plainly that these hold none of the demo's tables. */
    static final List<String> KEYSPACES =
            List.of("cassandra_sql", "cassandra_sql_internal", "pg_catalog");

    private final SqlConsoleClient client;
    private final AccordSqlSettings settings;

    SqlConsole(SqlConsoleClient client, AccordSqlSettings settings) {
        this.client = client;
        this.settings = settings;
    }

    public SqlConsoleStatus status() {
        if (!client.busy()) {
            client.ensureReady();
        }
        return new SqlConsoleStatus(
                SqlConsoleResult.ENGINE,
                client.connected(),
                settings.host(),
                settings.port(),
                settings.database(),
                KEYSPACES);
    }

    public List<SqlPreset> presets() {
        return ConsolePresets.ALL;
    }

    /** Whether the service answered a round trip just now, which is what a batch needs. */
    public boolean ready() {
        return client.ensureReady();
    }

    public String address() {
        return settings.host() + ":" + settings.port();
    }

    /** The tables and their rows. Not idempotent: {@code UNIQUE} is held, so a second seed is refused. */
    public SqlConsoleResult createSchema() {
        return runMany(ConsoleSchema.schemaAndSeed());
    }

    /** Drop everything the console owns, then create and seed it again. */
    public SqlConsoleResult reset() {
        return runMany(Stream.concat(
                        ConsoleSchema.RESET.stream(), ConsoleSchema.schemaAndSeed().stream())
                .toList());
    }

    /** A row count per table, one statement each. */
    public SqlConsoleResult tableCounts() {
        return runMany(ConsoleSchema.counts());
    }

    public SqlConsoleResult execute(String sql) {
        return runMany(List.of(sql));
    }

    /** Each defect and its control, run against the live service rather than quoted. */
    public List<SqlQuirk> quirks() {
        return ConsoleQuirks.ALL.stream()
                .map(quirk -> new SqlQuirk(
                        quirk.id(),
                        quirk.title(),
                        quirk.summary(),
                        quirk.expected(),
                        runOne(quirk.probe()),
                        runOne(quirk.control())))
                .toList();
    }

    private SqlConsoleResult runMany(List<String> statements) {
        long started = System.nanoTime();
        List<SqlStatementResult> results = statements.stream().map(this::runOne).toList();
        return new SqlConsoleResult(
                SqlConsoleResult.ENGINE,
                results,
                Round.places((System.nanoTime() - started) / 1_000_000.0, 2),
                (int) results.stream().filter(result -> result.error() != null).count());
    }

    /**
     * One statement, with a refusal reported as a field.
     *
     * <p>A statement the service rejects is a result and not a server error: showing what
     * cassandra-sql refuses is half of what the console is for, and a batch continues past one.
     */
    private SqlStatementResult runOne(String sql) {
        SqlAnswer answer;
        try {
            answer = client.execute(sql);
        } catch (SQLException | RuntimeException e) {
            return SqlStatementResult.failed(sql, String.valueOf(e.getMessage()));
        }
        List<List<String>> rows = answer.rows();
        return new SqlStatementResult(
                sql,
                answer.columns(),
                rows.size() > MAX_ROWS ? rows.subList(0, MAX_ROWS) : rows,
                rows.size(),
                Round.places(answer.durationMs(), 2),
                null);
    }
}
