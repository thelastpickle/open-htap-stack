package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

/**
 * The three streaming routes, with no broker and no registry.
 *
 * <p>What the tail reports once records arrive is asserted by {@code CdcTailTest} and by the dashboard
 * step of the workflow; what is decidable here is that each route answers while both services are
 * unreachable, which is the state a page opened on a cold stack finds.
 */
@QuarkusTest
class StreamingApiTest {

    /** The counters answer whatever the loop is doing, so the page can show the topic as down. */
    @Test
    void theStatusAnswersWithNoBroker() {
        when().get("/api/streaming/cdc/status")
                .then()
                .statusCode(200)
                .body("topic", equalTo("cdc-mutations"))
                .body("bootstrap", equalTo("127.0.0.1:1"))
                .body("registry", equalTo("http://127.0.0.1:1/apis/ccompat/v7"))
                .body("buffer_size", equalTo(200))
                .body("buffered", equalTo(0))
                .body("consumed", equalTo(0))
                .body("decode_failures", equalTo(0))
                .body("partitions", emptyIterable())
                .body("schema_ids", emptyIterable())
                .body("$", hasKey("latency_p50_ms"));
    }

    /** The stream route is the status and a window of records, which is empty here. */
    @Test
    void theStreamAnswersWithItsStatusAndNoRecords() {
        when().get("/api/streaming/cdc?limit=10")
                .then()
                .statusCode(200)
                .body("status.topic", equalTo("cdc-mutations"))
                .body("records", emptyIterable());
    }

    /**
     * A limit outside the range is clamped rather than refused.
     *
     * <p>A divergence from FastAPI, which declared {@code Query(50, ge=1, le=500)} and answered 422;
     * the reason is beside the parameter. A body field is still refused, as {@code /vector/search}
     * and the settings route both are, so it is query parameters alone that clamp here.
     */
    @Test
    void aLimitOutsideTheRangeIsClamped() {
        when().get("/api/streaming/cdc?limit=0").then().statusCode(200);
        when().get("/api/streaming/cdc?limit=99999&since=4").then().statusCode(200);
    }

    /** The contract is reported as unreachable, and the subject it would have been read from. */
    @Test
    void theSchemaRouteNamesTheSubjectItCouldNotRead() {
        when().get("/api/streaming/cdc/schema")
                .then()
                .statusCode(200)
                .body("subject", equalTo("cdc-mutations-value"))
                .body("registry", equalTo("http://127.0.0.1:1/apis/ccompat/v7"))
                .body("fields", emptyIterable())
                .body("payload_fields", emptyIterable())
                .body("error", startsWith("ConnectException"));
    }
}
