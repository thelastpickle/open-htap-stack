package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

/**
 * The Health page's routes with none of the five engines reachable.
 *
 * <p>Which is the state the page matters most in, so what is checked here is that every control still
 * answers and says why. Two statuses carry meaning the page reads before the body: 400 for a request
 * this backend refuses, and 502 for an engine that was asked and refused.
 */
@QuarkusTest
class PlatformApiTest {

    /** No engine answers, so both listings are the reason rather than an empty page. */
    @Test
    void theWorkInFlightNamesEveryPathThatCouldNotBeListed() {
        when().get("/api/platform/running")
                .then()
                .statusCode(200)
                .body("comparison", nullValue())
                .body("queries", emptyIterable())
                .body("unreadable.keySet()", contains("presto", "spark", "cassandra", "cqlite"))
                .body("unreadable.cassandra",
                        equalTo("Cassandra keeps no list of running queries to read"))
                .body("unreadable.cqlite", equalTo("no scan is running"));
    }

    /** A control that stopped nothing reports so, since reporting success would read as working. */
    @Test
    void cancellingWithNothingRunningIsARefusalRatherThanASuccess() {
        when().post("/api/platform/running/cancel-comparison")
                .then()
                .statusCode(200)
                .body("ok", equalTo(false))
                .body("actions", contains("no comparison was running"));
    }

    /** The two paths with no handle to kill by are refused here rather than reaching an engine. */
    @Test
    void onlyTheTwoEnginesThatHandOutAHandleCanBeKilledBy() {
        for (String engine : new String[] {"cassandra", "cqlite", "spark_bulk", null}) {
            kill("{\"engine\": " + quoted(engine) + ", \"id\": \"q-1\"}")
                    .statusCode(400)
                    .body("detail", equalTo("Only presto and spark hand out a query handle to kill"
                            + " by; engine was " + engine));
        }
    }

    @Test
    void aKillNamingNoQueryIsRefused() {
        kill("{\"engine\": \"presto\", \"id\": \"   \"}")
                .statusCode(400)
                .body("detail", equalTo("Name the query to kill"));
    }

    /**
     * 502 rather than 500: this route asked another service and could not reach it, which is not the
     * request having been wrong.
     */
    @Test
    void anEngineThatCouldNotBeAskedIsAGatewayFailure() {
        kill("{\"engine\": \"presto\", \"id\": \"q-1\"}")
                .statusCode(502)
                .body("detail", not(nullValue()));
        kill("{\"engine\": \"spark\", \"id\": \"7\"}")
                .statusCode(502)
                .body("detail", not(nullValue()));
    }

    @Test
    void aReconnectTargetThisBackendDoesNotHaveIsNamedInTheRefusal() {
        reconnect("{\"target\": \"duckdb\"}")
                .statusCode(400)
                .body("detail", equalTo("Unknown reconnect target: duckdb"));
        reconnect("{}")
                .statusCode(400)
                .body("detail", equalTo("Unknown reconnect target: null"));
    }

    /** One line per path, and not ok, because no path can be rebuilt with no engine listening. */
    @Test
    void reconnectingEveryPathReportsOneLinePerPathAndSucceedsAtNone() {
        reconnect("{\"target\": \"all\"}")
                .statusCode(200)
                .body("ok", equalTo(false))
                .body("actions", hasSize(5));
    }

    /** A single target is the same route with one line, which is what a per-path button sends. */
    @Test
    void reconnectingOnePathReportsThatPathAlone() {
        reconnect("{\"target\": \"cqlite\"}")
                .statusCode(200)
                .body("ok", equalTo(false))
                .body("actions", hasSize(1));
    }

    private static String quoted(String engine) {
        return engine == null ? "null" : "\"" + engine + "\"";
    }

    private static io.restassured.response.ValidatableResponse kill(String body) {
        return post("/api/platform/running/kill", body);
    }

    private static io.restassured.response.ValidatableResponse reconnect(String body) {
        return post("/api/platform/reconnect", body);
    }

    private static io.restassured.response.ValidatableResponse post(String path, String body) {
        return given().contentType(ContentType.JSON).body(body).when().post(path).then();
    }
}
