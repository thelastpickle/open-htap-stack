package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.sql.ConsoleGate;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

/**
 * The console's routes, with cassandra-sql unreachable.
 *
 * <p>What the engine answers a statement is asserted by the dashboard step of the workflow, which is
 * the only place that service runs. What is decidable here is the request handling: which route needs
 * a connection, which needs the gate, and what a caller is told when either refuses.
 */
@QuarkusTest
class SqlConsoleApiTest {

    /** The test profile points accord-sql at a port nothing listens on. */
    private static final String UNREACHABLE = "cassandra-sql is not reachable at 127.0.0.1:1";

    @Inject
    ConsoleGate gate;

    /** Reachability is a field rather than a failure: the page shows the service as down. */
    @Test
    void theStatusAnswersWithNothingListening() {
        when().get("/api/sql-console/status")
                .then()
                .statusCode(200)
                .body("engine", equalTo("cassandra-sql"))
                .body("connected", equalTo(false))
                .body("host", equalTo("127.0.0.1"))
                .body("port", equalTo(1))
                .body("database", equalTo("cassandra_sql"))
                .body("keyspaces",
                        contains("cassandra_sql", "cassandra_sql_internal", "pg_catalog"));
    }

    /** The presets are text this backend owns, so they answer with no service at all. */
    @Test
    void thePresetsAnswerWithoutAService() {
        when().get("/api/sql-console/presets")
                .then()
                .statusCode(200)
                .body("$", hasSize(8))
                .body("[0].id", equalTo("transaction"))
                .body("[0].sql", startsWith("BEGIN;"))
                .body("[1].id", equalTo("rollback"));
    }

    /** Every route that runs a statement says which address it could not reach. */
    @Test
    void eachRunningRouteRefusesWithTheAddressItTried() {
        when().get("/api/sql-console/tables")
                .then()
                .statusCode(503)
                .body("detail", equalTo(UNREACHABLE));
        when().get("/api/sql-console/quirks")
                .then()
                .statusCode(503)
                .body("detail", equalTo(UNREACHABLE));
        when().post("/api/sql-console/schema")
                .then()
                .statusCode(503)
                .body("detail", equalTo(UNREACHABLE));
        when().post("/api/sql-console/reset")
                .then()
                .statusCode(503)
                .body("detail", equalTo(UNREACHABLE));
        execute("{\"sql\": \"SELECT 1\"}").statusCode(503).body("detail", equalTo(UNREACHABLE));
    }

    /** A body with no statement is refused, and no connection is attempted for it. */
    @Test
    void aBodyWithNoStatementIsRefused() {
        execute("{}").statusCode(422).body("detail", equalTo("sql must not be empty"));
        execute("{\"sql\": \"  \"}").statusCode(422).body("detail", equalTo("sql must not be empty"));
    }

    /**
     * A route that refused leaves the gate open.
     *
     * <p>A gate left shut by a batch that could not run would make every statement after it a 409,
     * which is the failure mode a refusal must not have.
     */
    @Test
    void aRefusedRouteLeavesTheGateOpen() {
        when().post("/api/sql-console/reset").then().statusCode(503);
        assertTrue(gate.tryEnter(), "the gate was left shut");
        gate.leave();
    }

    /**
     * With a batch in flight, a second is refused rather than queued.
     *
     * <p>Answered without touching the client, which is why the gate is taken before the connection is
     * proved: proving it would wait on the lock the running batch holds.
     */
    @Test
    void aSecondBatchIsRefusedWhileOneIsRunning() {
        assertTrue(gate.tryEnter());
        try {
            when().get("/api/sql-console/tables")
                    .then()
                    .statusCode(409)
                    .body("detail", equalTo("a statement is already running"));
        } finally {
            gate.leave();
        }
    }

    private static ValidatableResponse execute(String body) {
        return given().contentType(ContentType.JSON)
                .body(body)
                .when()
                .post("/api/sql-console/execute")
                .then();
    }
}
