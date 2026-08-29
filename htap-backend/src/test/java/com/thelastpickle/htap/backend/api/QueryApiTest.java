package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.query.Asked;
import com.thelastpickle.htap.backend.query.Run;
import com.thelastpickle.htap.backend.query.RunMode;
import com.thelastpickle.htap.backend.query.SingleRunGate;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The console and the schema page with none of the five engines reachable.
 *
 * <p>Three outcomes have to stay distinguishable by status, because the compare page and the
 * console both read the status before the body: 503 for a path that could not be reached, 400 for a
 * request this backend refuses, and 200 with rows. Only the first two can be shown here; an
 * answered statement needs an engine, and is checked by running one against the stack.
 */
@QuarkusTest
class QueryApiTest {

    /** A bucket as the sink spells it, which is the text the three SQL dialects share. */
    private static final String BUCKET = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}";

    @Inject
    SingleRunGate gate;

    @Test
    void aStatementOnAnUnreachablePathIsRefusedWithTheDetailFieldTheFrontendReads() {
        for (String engine : List.of("cassandra", "presto", "spark", "spark_bulk", "cqlite")) {
            sql("{\"sql\": \"SELECT * FROM events\", \"engine\": \"" + engine + "\"}")
                    .statusCode(503)
                    .body("detail", not(nullValue()));
        }
    }

    @Test
    void anEngineThisBackendDoesNotHaveIsNamedInTheRefusal() {
        sql("{\"sql\": \"SELECT * FROM events\", \"engine\": \"duckdb\"}")
                .statusCode(400)
                .body("detail", equalTo("Unknown engine: duckdb"));
    }

    /**
     * The engine is resolved before the statement is read, so a request naming neither an engine
     * this backend has nor a statement it would run is refused for the engine.
     */
    @Test
    void theEngineIsResolvedBeforeTheStatementIsRead() {
        sql("{\"sql\": \"DELETE FROM events\", \"engine\": \"duckdb\"}")
                .statusCode(400)
                .body("detail", equalTo("Unknown engine: duckdb"));
    }

    /** Refused here rather than by an engine, so the refusal arrives with no engine reachable. */
    @Test
    void aStatementTheConsoleWillNotRunIsRefusedBeforeAnyEngineIsAsked() {
        sql("{\"sql\": \"DELETE FROM events\"}")
                .statusCode(400)
                .body("detail", equalTo("Only SELECT queries are allowed"));

        sql("{\"sql\": \"SELECT * FROM (delete from events) x\"}")
                .statusCode(400)
                .body("detail", equalTo("Forbidden keyword in a read-only console: DELETE"));

        sql("{\"sql\": \"SELECT 1; SELECT 2\"}")
                .statusCode(400)
                .body("detail", equalTo("Only a single statement is allowed"));

        sql("{\"sql\": \"   \"}").statusCode(400).body("detail", equalTo("Empty query"));
    }

    /** A body-less POST reaches the route as null, and is refused as an empty statement is. */
    @Test
    void aPostWithNoBodyIsRefusedAsAnEmptyStatementIs() {
        given().contentType(ContentType.JSON)
                .when()
                .post("/api/query/sql")
                .then()
                .statusCode(400)
                .body("detail", equalTo("Empty query"));
    }

    @Test
    void theEngineSelectorListsTheFivePathsInOrderAndNoneIsConnected() {
        io.restassured.path.json.JsonPath answered =
                when().get("/api/query/engines").then().statusCode(200).extract().jsonPath();

        assertEquals(
                List.of("cassandra", "presto", "spark", "spark_bulk", "cqlite"),
                List.copyOf(answered.getMap("engines").keySet()));
        assertEquals(
                List.of(false, false, false, false, false),
                List.copyOf(answered.getMap("engines").values()));
    }

    /**
     * With no Cassandra to ask, no closed window can be shown to hold anything, so the window
     * still filling is named and nothing is claimed of it. That is also the state a stack minutes
     * old is in, where every event so far is in the window now filling.
     */
    @Test
    void theWindowChoiceNamesTheFillingWindowAndClaimsNothingOfIt() {
        when().get("/api/query/window")
                .then()
                .statusCode(200)
                .body("bucket_minutes", equalTo(15))
                .body("shards", equalTo(16))
                .body("bucket", matchesPattern(BUCKET))
                .body("current", matchesPattern(BUCKET))
                .body("closed", equalTo(false))
                .body("settled", equalTo(false))
                .body("settled_detail", equalTo("the window is still filling"));
    }

    /** The window named is the one now filling, which is the same field twice. */
    @Test
    void theBucketAndTheCurrentWindowAreTheSameWhenNoneIsClosed() {
        io.restassured.path.json.JsonPath answered = when().get("/api/query/window").jsonPath();

        assertEquals(answered.getString("current"), answered.getString("bucket"));
    }

    /**
     * A schema nobody could read is an error and no tables, rather than an empty keyspace: a page
     * showing no tables would otherwise look like a keyspace that has none.
     */
    @Test
    void theCqlSchemaReportsWhyItCouldNotBeReadRatherThanShowingNoTables() {
        when().get("/api/schema/cql")
                .then()
                .statusCode(200)
                .body("engine", equalTo("cassandra"))
                .body("keyspace", equalTo("demo"))
                .body("tables", emptyIterable())
                .body("indexes", emptyIterable())
                .body("storage_keyspaces", equalTo(List.of("demo")))
                .body("warnings", emptyIterable())
                .body("error", not(nullValue()));
    }

    /**
     * The SQL half answers apart from the CQL half, and names the address it could not reach.
     *
     * <p>Two routes for that reason: either engine can be down while the other answers, and one
     * route reading both would blank the half it could still show.
     */
    @Test
    void theSqlSchemaReportsTheServiceItCouldNotReach() {
        when().get("/api/schema/sql")
                .then()
                .statusCode(200)
                .body("engine", equalTo("cassandra-sql"))
                .body("keyspace", equalTo("cassandra_sql"))
                .body("tables", emptyIterable())
                .body("storage_keyspaces",
                        equalTo(List.of("cassandra_sql", "cassandra_sql_internal", "pg_catalog")))
                .body("error", equalTo("cassandra-sql is not reachable at 127.0.0.1:1"));
    }

    /** A comparison of nothing is a selector with nothing ticked, and is told so. */
    @Test
    void aComparisonNamingNoPathItCouldRunIsRefused() {
        benchmark("{\"sql\": \"SELECT 1\", \"engines\": [\"duckdb\"]}")
                .statusCode(400)
                .body("detail", equalTo("Unknown engine(s): duckdb"));

        benchmark("{\"sql\": \"SELECT 1\", \"engines\": []}")
                .statusCode(400)
                .body("detail", equalTo("Choose at least one engine to compare"));
    }

    /** The statement is validated before the gate is taken, so a bad one leaves it free. */
    @Test
    void aStatementTheConsoleWillNotRunIsRefusedBeforeTheGateIsTaken() {
        benchmark("{\"sql\": \"   \"}").statusCode(400).body("detail", equalTo("Empty query"));

        benchmark("{\"sql\": \"SELECT 1\"}").statusCode(200);
    }

    /**
     * 409, which is what the compare page tells a viewer to go and look at the Health page about:
     * two overlapping runs would each be timed while the other ran.
     */
    @Test
    void aSecondComparisonArrivingWhileOneRunsIsRefusedWithTheDetailField() {
        Run first = gate.begin(
                new Asked("SELECT 1", List.of("cassandra"), RunMode.SEQUENTIAL, 25, false),
                List.of());
        try {
            benchmark("{\"sql\": \"SELECT 1\"}")
                    .statusCode(409)
                    .body("detail", containsString("A comparison has been running for"));
        } finally {
            gate.end(first);
        }
    }

    /** Four kinds of line, in this order, and the paths in the order the caller asked for. */
    @Test
    void theStreamReportsAStartLineABaselineEachPathAndADoneLine() {
        List<String> lines = stream(
                "{\"sql\": \"SELECT 1\", \"engines\": [\"cqlite\", \"cassandra\"]}");

        assertEquals(List.of("start", "baseline", "engine", "engine", "done"),
                lines.stream().map(line -> field(line, "event")).toList());
        assertEquals(List.of("cqlite", "cassandra"),
                lines.stream()
                        .filter(line -> "engine".equals(field(line, "event")))
                        .map(line -> field(line, "engine"))
                        .toList());
        assertEquals("false", field(lines.getLast(), "cancelled"));
    }

    /** The gate is released when the stream ends, so the next comparison is not refused. */
    @Test
    void theGateIsFreeOnceTheStreamHasFinished() {
        stream("{\"sql\": \"SELECT 1\", \"engines\": [\"cassandra\"]}");

        assertTrue(gate.running().isEmpty());
    }

    /** No question is a request this backend refuses, where an unanswerable one is not. */
    @Test
    void aQuestionInNoWordsIsRefused() {
        nl("{\"prompt\": \"   \"}").statusCode(400).body("detail", equalTo("Empty prompt"));
        nl("{}").statusCode(400).body("detail", equalTo("Empty prompt"));
    }

    /**
     * 200 with the statement and the reason, because the translation is itself what a viewer came
     * for: a status would throw it away and leave the page with nothing to show.
     */
    @Test
    void aQuestionAnUnreachableEngineCouldNotAnswerStillReportsTheStatement() {
        nl("{\"prompt\": \"which drones are hottest\"}")
                .statusCode(200)
                .body("generated_sql",
                        equalTo("SELECT entity_id, event_time, latitude, longitude, altitude_m, "
                                + "speed_mps, temp_internal_c, risk_score "
                                + "FROM demo.drone_latest_status ORDER BY temp_internal_c DESC"))
                .body("render_hint", equalTo("table"))
                .body("result", nullValue())
                .body("error", not(nullValue()));
    }

    /** Read from the question, so it is reported whether the statement ran or not. */
    @Test
    void theRenderHintIsReportedOnARefusalToo() {
        nl("{\"prompt\": \"where are the drones\"}")
                .statusCode(200)
                .body("render_hint", equalTo("map"))
                .body("error", not(nullValue()));
    }

    /** One field of one NDJSON line, read as text so a null and a number both compare. */
    private static String field(String line, String name) {
        return new io.restassured.path.json.JsonPath(line).getString(name);
    }

    private static List<String> stream(String body) {
        String answered = given().contentType(ContentType.JSON)
                .body(body)
                .when()
                .post("/api/query/benchmark/stream")
                .then()
                .statusCode(200)
                .contentType("application/x-ndjson")
                .header("X-Accel-Buffering", "no")
                .header("Cache-Control", "no-store")
                .extract()
                .asString();
        return answered.lines().filter(line -> !line.isBlank()).toList();
    }

    private static io.restassured.response.ValidatableResponse benchmark(String body) {
        return post("/api/query/benchmark", body);
    }

    private static io.restassured.response.ValidatableResponse nl(String body) {
        return post("/api/query/nl", body);
    }

    private static io.restassured.response.ValidatableResponse sql(String body) {
        return post("/api/query/sql", body);
    }

    private static io.restassured.response.ValidatableResponse post(String path, String body) {
        return given().contentType(ContentType.JSON).body(body).when().post(path).then();
    }
}
