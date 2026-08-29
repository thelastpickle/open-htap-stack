package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

/**
 * Every read path with no Cassandra to read from.
 *
 * <p>That state is the one worth pinning, because it is what a browser meets while the stack
 * starts and it is where the Python and this port could most easily disagree: each route
 * decides for itself whether an unreachable engine is an empty answer, a refusal, or zeros.
 * The field names here are the contract the frontend was written against.
 */
@QuarkusTest
class ReadPathApiTest {

    /** Python's {@code datetime.now(timezone.utc).isoformat()}, which the pages parse. */
    private static final String OFFSET_STAMP =
            "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{6})?\\+00:00";

    @Test
    void theApiReportsItselfUpWithoutClaimingAnythingElseIs() {
        when().get("/api/health")
                .then()
                .statusCode(200)
                .body("status", equalTo("ok"))
                .body("timestamp", matchesPattern(OFFSET_STAMP));
    }

    @Test
    void theKpisAreZerosAndTheScoreIsNoneReachable() {
        when().get("/api/overview/kpis")
                .then()
                .statusCode(200)
                .body("total_drones", equalTo(0))
                .body("active_flying_drones", equalTo(0))
                .body("grounded_drones", equalTo(0))
                .body("avg_speed_mps", equalTo(0.0f))
                .body("min_altitude_m", equalTo(0.0f))
                .body("near_zone_count", equalTo(0))
                .body("predicted_breach_count", equalTo(0))
                .body("total_events", equalTo(0))
                .body("ingestion_rate_per_sec", equalTo(0.0f))
                .body("platform_health_score", equalTo(0.0f))
                .body("latest_alerts", emptyIterable());
    }

    /** The window is clamped and reported, so the page can label what it actually got. */
    @Test
    void theIngestionHistoryReportsTheWindowItClampedTo() {
        when().get("/api/overview/ingestion-history?hours=100")
                .then()
                .statusCode(200)
                .body("hours", equalTo(48))
                .body("buckets", emptyIterable());

        when().get("/api/overview/ingestion-history?hours=0")
                .then()
                .statusCode(200)
                .body("hours", equalTo(1));
    }

    @Test
    void theCsvDownloadIsAHeaderRowAndAFilename() {
        String csv = when().get("/api/overview/ingestion-history/csv?hours=3")
                .then()
                .statusCode(200)
                .contentType(startsWith("text/csv"))
                .header(
                        "Content-Disposition",
                        equalTo("attachment; filename=\"ingestion_log_3h.csv\""))
                .extract()
                .asString();

        org.junit.jupiter.api.Assertions.assertEquals("time,timestamp,count\n", csv);
    }

    /**
     * A failed re-sync says why and sends no key performance indicator (KPI) object at all,
     * rather than sending zeros.
     */
    @Test
    void aResyncAgainstAnUnreachableClusterFailsAndOmitsTheKpis() {
        when().post("/api/overview/resync")
                .then()
                .statusCode(200)
                .body("success", equalTo(false))
                .body("message", not(nullValue()))
                .body("$", not(org.hamcrest.Matchers.hasKey("kpis")));
    }

    @Test
    void theLiveMapIsEmptyButStillTimestamped() {
        when().get("/api/map/live")
                .then()
                .statusCode(200)
                .body("drones", emptyIterable())
                .body("zones", emptyIterable())
                .body("timestamp", matchesPattern(OFFSET_STAMP));
    }

    /**
     * Zeros rather than a 400, and the order is why: the connection is tested before the
     * polygon is parsed, so an unreachable Cassandra answers before the text is looked at.
     */
    @Test
    void polygonStatsAnswersZerosBeforeItParsesAnything() {
        given().contentType(ContentType.JSON)
                .body("{\"polygon_wkt\": \"not a polygon\"}")
                .when()
                .post("/api/map/polygon-stats")
                .then()
                .statusCode(200)
                .body("drone_count", equalTo(0))
                .body("avg_speed_mps", equalTo(0.0f))
                .body("max_speed_mps", equalTo(0.0f))
                .body("avg_altitude_m", equalTo(0.0f))
                .body("max_altitude_m", equalTo(0.0f))
                .body("avg_temp_internal_c", equalTo(0.0f))
                // All six and no seventh: a name RestAssured cannot find reads as null, so an
                // assertion that a field is absent has to ask the object for its keys.
                .body("$", aMapWithSize(6));
    }

    /**
     * A body-less POST, which reaches the route as a null parameter.
     *
     * <p>400 rather than FastAPI's 422: pydantic refused the request before the route saw it,
     * where this port refuses it inside the route and answers as it does for text that will
     * not parse. What both spellings share is the {@code detail} field the pages read.
     */
    @Test
    void aPostWithNoBodyIsRefusedWithTheDetailFieldTheFrontendReads() {
        given().contentType(ContentType.JSON)
                .when()
                .post("/api/map/polygon-stats")
                .then()
                .statusCode(400)
                .body("detail", equalTo("Could not parse polygon_wkt"));

        given().contentType(ContentType.JSON)
                .when()
                .post("/api/zones/what-if")
                .then()
                .statusCode(400)
                .body("detail", equalTo("Could not parse polygon_wkt"));
    }

    /**
     * A body carrying no {@code polygon_wkt}, which pydantic refused as it refused no body at
     * all: the field was a required {@code str}. Both routes have to agree with that and with
     * each other, and the polygon route is where they could differ, because it tests the
     * connection before it parses.
     */
    @Test
    void aPostWithNoPolygonFieldIsRefusedLikeOneWithNoBody() {
        given().contentType(ContentType.JSON)
                .body("{}")
                .when()
                .post("/api/map/polygon-stats")
                .then()
                .statusCode(400)
                .body("detail", equalTo("Could not parse polygon_wkt"));

        given().contentType(ContentType.JSON)
                .body("{}")
                .when()
                .post("/api/zones/what-if")
                .then()
                .statusCode(400)
                .body("detail", equalTo("Could not parse polygon_wkt"));
    }

    @Test
    void anAssetReadRefusesWithTheDetailFieldTheFrontendReads() {
        when().get("/api/map/drone/drone-1")
                .then()
                .statusCode(503)
                .body("detail", equalTo("Cassandra unavailable"));

        when().get("/api/map/drone/drone-1/trail")
                .then()
                .statusCode(503)
                .body("detail", equalTo("Cassandra unavailable"));
    }

    /** A radius search answers an empty list rather than refusing: the map draws nothing. */
    @Test
    void aNearbySearchIsEmptyRatherThanARefusal() {
        when().get("/api/map/drone/drone-1/nearby?meters=100000")
                .then()
                .statusCode(200)
                .body("drones", emptyIterable());
    }

    @Test
    void theAlertsListIsEmptyAndCountsNothing() {
        when().get("/api/alerts?limit=1000")
                .then()
                .statusCode(200)
                .body("alerts", emptyIterable())
                .body("total_count", equalTo(0));
    }

    @Test
    void theZoneListIsEmpty() {
        when().get("/api/zones").then().statusCode(200).body("zones", emptyIterable());
    }

    /** Parsed before the connection is tested, so this refusal does not need Cassandra. */
    @Test
    void aWhatIfZoneWithAnUnreadablePolygonIsRefused() {
        given().contentType(ContentType.JSON)
                .body("{\"polygon_wkt\": \"CIRCLE(10 50, 5)\"}")
                .when()
                .post("/api/zones/what-if")
                .then()
                .statusCode(400)
                .body("detail", equalTo("Could not parse polygon_wkt"));
    }

    @Test
    void aWhatIfZoneEchoesItsOwnDefaultsWhenItCannotBeScored() {
        given().contentType(ContentType.JSON)
                .body("{\"polygon_wkt\": \"POLYGON((10 50, 11 50, 11 51, 10 50))\"}")
                .when()
                .post("/api/zones/what-if")
                .then()
                .statusCode(200)
                .body("zone.zone_id", equalTo("what-if"))
                .body("zone.zone_name", equalTo("What-if zone"))
                .body("zone.severity", equalTo("warning"))
                .body("zone.enabled", equalTo(true))
                .body("drones_inside", equalTo(0))
                .body("drones_nearby", equalTo(0))
                .body("affected_drone_ids", emptyIterable());
    }

    @Test
    void everyServiceIsProbedAndNoneIsReachable() {
        when().get("/api/platform/health")
                .then()
                .statusCode(200)
                .body("services.size()", equalTo(5))
                .body("services.name", equalTo(java.util.List.of(
                        "Cassandra", "Kafka", "Presto", "Spark", "cassandra-sql")))
                .body("services.status", equalTo(java.util.List.of(
                        "down", "down", "down", "down", "down")))
                .body("overall_health_score", equalTo(0.0f))
                .body("total_drones", equalTo(0))
                .body("container_cli", equalTo("podman"));
    }

    /** FastAPI served its Swagger UI here, and the frontend's help links point at it. */
    @Test
    void theApiDocumentsItselfAtTheSamePathAsBefore() {
        when().get("/docs").then().statusCode(200).body(containsString("swagger-ui"));

        when().get("/q/openapi")
                .then()
                .statusCode(200)
                .body(containsString("/api/map/live"))
                .body(containsString("/api/overview/kpis"));
    }
}
