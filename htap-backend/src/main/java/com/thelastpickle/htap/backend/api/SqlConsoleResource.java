package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.SqlConsoleRequest;
import com.thelastpickle.htap.backend.api.dto.SqlConsoleResult;
import com.thelastpickle.htap.backend.api.dto.SqlConsoleStatus;
import com.thelastpickle.htap.backend.api.dto.SqlPreset;
import com.thelastpickle.htap.backend.api.dto.SqlQuirk;
import com.thelastpickle.htap.backend.sql.ConsoleGate;
import com.thelastpickle.htap.backend.sql.SqlConsole;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.function.Supplier;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * cassandra-sql: a Postgres-dialect SQL console, with transactions, over Accord.
 *
 * <p>Kept apart from {@link QueryResource} for the same reason {@link TransactionsResource} is: that
 * class rejects every write keyword, which is what keeps the read console honest. This console is
 * mostly writes, so it needs its own routes rather than a hole in that check.
 *
 * <p>Nothing here touches {@code demo.events}. cassandra-sql stores SQL rows in its own keyspaces
 * under an encoding of its own, so it is a sixth interface and not a sixth access path, and it
 * appears in no comparison.
 *
 * <p>The project states wider limits still, and {@code README.md} quotes them verbatim rather than
 * softening them: a proof of concept, "~40% (core features only)" SQL compliance, and journals that
 * do not compact.
 */
@Path("/api/sql-console")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "sql-console")
public class SqlConsoleResource {

    private final SqlConsole console;
    private final ConsoleGate gate;

    SqlConsoleResource(SqlConsole console, ConsoleGate gate) {
        this.console = console;
        this.gate = gate;
    }

    /** Is cassandra-sql reachable, and what is it? */
    @GET
    @Path("/status")
    public SqlConsoleStatus status() {
        return console.status();
    }

    @GET
    @Path("/presets")
    public List<SqlPreset> presets() {
        return console.presets();
    }

    /**
     * Create the tables and insert the rows the presets read.
     *
     * <p><b>Not idempotent.</b> Each statement is sent on its own, so a duplicate {@code CREATE} is
     * reported against that statement and the run continues; but the seed is refused outright on a
     * second run, because {@code UNIQUE} is the one declared constraint this engine holds: "UNIQUE
     * constraint violation: operators_licence_unique on columns (licence)", and the same for
     * {@code drones.serial} and {@code zones.zone_code}. Use the reset route to get back to the
     * seeded state.
     */
    @POST
    @Path("/schema")
    public SqlConsoleResult createSchema() {
        return alone(console::createSchema);
    }

    /**
     * Drop everything this page owns, then create and seed it again.
     *
     * <p><b>Destructive</b>, and the only way back to the seeded state. Two things make it necessary
     * rather than convenient. {@code UNIQUE} is enforced, so re-running the schema route cannot
     * re-seed a table that already holds its rows. And the oversubscribe preset decrements
     * {@code zones.capacity} without reading it, so a page that has run that preset reports a
     * capacity no longer matching the airspace the Accord subtab admits drones to.
     *
     * <p>Drops nothing outside the five tables, the two ENUMs and the sequence, so a keyspace
     * cassandra-sql created for its own use is untouched.
     */
    @POST
    @Path("/reset")
    public SqlConsoleResult reset() {
        return alone(console::reset);
    }

    /**
     * The four join defects, each run beside the control that isolates it.
     *
     * <p>Needs the transaction preset to have run: three of the four read {@code flight_legs}, which
     * the seed leaves empty.
     */
    @GET
    @Path("/quirks")
    public List<SqlQuirk> quirks() {
        return alone(console::quirks);
    }

    /**
     * A row count per table, one statement each.
     *
     * <p>A table with no rows yet reports an error rather than zero, because {@code COUNT(*)} over an
     * empty table raises here. Left as the error it is rather than rewritten to zero: this route is
     * one of the places the page shows what the engine does.
     *
     * <p>One statement each for that reason and not because {@code UNION ALL} is broken: a
     * {@code UNION ALL} of four counts answers correctly. But one that includes the empty table fails
     * whole, so a single statement here would report nothing about the three tables that do have
     * rows.
     */
    @GET
    @Path("/tables")
    public SqlConsoleResult tableCounts() {
        return alone(console::tableCounts);
    }

    /**
     * Run one SQL string, which may hold a whole {@code BEGIN}/{@code COMMIT} transaction.
     *
     * <p>No parameters, and that is deliberate rather than a simplification: a bound parameter
     * returns no rows here with no error raised, so an interface offering one would be offering a
     * silent wrong answer.
     */
    @POST
    @Path("/execute")
    @Consumes(MediaType.APPLICATION_JSON)
    public SqlConsoleResult execute(SqlConsoleRequest asked) {
        if (asked == null) {
            throw new ApiException(422, "Expected a body carrying the statement to run");
        }
        asked.outOfRange().ifPresent(reason -> {
            throw new ApiException(422, reason);
        });
        return alone(() -> console.execute(asked.sql()));
    }

    /**
     * One batch at a time, over a service that answered a round trip first.
     *
     * <p>The gate is taken before the connection is proved, and that order matters: proving it takes
     * the client's own lock, so a caller doing it first would wait out the very batch it is about to
     * be refused for. The refusal has to be answerable while a statement is in flight.
     */
    private <T> T alone(Supplier<T> work) {
        if (!gate.tryEnter()) {
            throw new ApiException(409, "a statement is already running");
        }
        try {
            if (!console.ready()) {
                throw new ApiException(503, "cassandra-sql is not reachable at " + console.address());
            }
            return work.get();
        } finally {
            gate.leave();
        }
    }
}
