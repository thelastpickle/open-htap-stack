package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
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

    private static io.restassured.response.ValidatableResponse sql(String body) {
        return given().contentType(ContentType.JSON)
                .body(body)
                .when()
                .post("/api/query/sql")
                .then();
    }
}
