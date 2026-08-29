package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The Explore page's four routes, with nothing reachable.
 *
 * <p>Cassandra is unreachable in this profile, so what is observable here is the refusals and the
 * request handling. The search itself is asserted against a running stack by the dashboard step of
 * the workflow, which is the only place a populated index exists.
 */
@QuarkusTest
class VectorApiTest {

    /** The loop is one bean for the life of the application, so a toggle must not outlive a test. */
    @AfterEach
    void turnTheLoopOff() {
        live("{\"enabled\": false}").statusCode(200).body("enabled", equalTo(false));
    }

    @Test
    void aSearchWithNoClusterSaysCassandraIsUnavailable() {
        search("{\"query\": \"restricted airspace\"}")
                .statusCode(503)
                .body("detail", equalTo("Cassandra unavailable"));
    }

    @Test
    void aLimitOutsideItsRangeIsRefusedBeforeAnythingIsEmbedded() {
        search("{\"query\": \"restricted airspace\", \"limit\": 0}")
                .statusCode(422)
                .body("detail", equalTo("limit must be between 1 and 50, got 0"));
    }

    @Test
    void aSearchWithNoBodyAtAllIsRefusedRatherThanCrashing() {
        given().contentType(ContentType.JSON)
                .when()
                .post("/api/vector/search")
                .then()
                .statusCode(422)
                .body("detail", equalTo("Expected a body carrying the query to search for"));
    }

    @Test
    void aBulkIndexWithNoClusterIsRefusedRatherThanStarted() {
        when().post("/api/vector/index-all")
                .then()
                .statusCode(503)
                .body("detail", equalTo("Cassandra unavailable"));
    }

    /** The names are the frontend's, and the Explore page polls this one every few seconds. */
    @Test
    void theLoopReportsTheNamesThePageReads() {
        when().get("/api/vector/live")
                .then()
                .statusCode(200)
                .body("enabled", equalTo(false))
                .body("embedder", equalTo("local"))
                .body("interval_s", equalTo(5.0f))
                .body("embedded", equalTo(0))
                .body("tracked", equalTo(0))
                .body("behind_s", nullValue())
                .body("error", nullValue());
    }

    @Test
    void theToggleTakesEffectOnTheAnswerItself() {
        live("{\"enabled\": true}").statusCode(200).body("enabled", equalTo(true));

        when().get("/api/vector/live").then().body("enabled", equalTo(true));
    }

    @Test
    void aToggleThatSaysNothingIsRefused() {
        live("{}")
                .statusCode(422)
                .body("detail",
                        equalTo("Expected a body saying whether to enable live embedding"));
    }

    private static ValidatableResponse search(String body) {
        return given().contentType(ContentType.JSON)
                .body(body)
                .when()
                .post("/api/vector/search")
                .then();
    }

    private static ValidatableResponse live(String body) {
        return given().contentType(ContentType.JSON)
                .body(body)
                .when()
                .post("/api/vector/live")
                .then();
    }
}
