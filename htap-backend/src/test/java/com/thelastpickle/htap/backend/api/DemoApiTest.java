package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

/**
 * The two demo routes, with nothing reachable.
 *
 * <p>What is observable in this profile is the refusal and the shape of the latency answer. That
 * the scenario's rows reach the map and the alert feed is asserted by the workflow's dashboard step
 * against a real cluster, which is the only place a fleet exists to flag.
 */
@QuarkusTest
class DemoApiTest {

    @Test
    void theScenarioIsRefusedWhenThereIsNoClusterToWriteTo() {
        when().post("/api/demo/trigger-breach-scenario")
                .then()
                .statusCode(503)
                .body("detail", equalTo("Cassandra unavailable"));
    }

    /**
     * A tier that cannot answer reports null and not zero, and the route answers 200 regardless: the
     * page shows an em dash per tier, so one tier being down must not take the other two with it.
     */
    @Test
    void everyTierThatCannotAnswerReportsNothing() {
        when().get("/api/demo/latency")
                .then()
                .statusCode(200)
                .body("cassandra_point_read_ms", nullValue())
                .body("presto_scan_ms", nullValue())
                .body("vector_search_ms", nullValue());
    }

    /** The frontend reads this by index in places, so the offset and the width are the contract. */
    @Test
    void theTimestampIsSpelledAsPythonsAwareIsoformatSpelledIt() {
        when().get("/api/demo/latency")
                .then()
                .statusCode(200)
                .body(
                        "timestamp",
                        matchesRegex("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{6})?\\+00:00"));
    }
}
