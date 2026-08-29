package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.equalTo;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The Settings page's five routes, over the settings this test's own profile declares.
 *
 * <p>The holder is one bean for the life of the application, so a test that changes the settings
 * would otherwise be read by the next one; each test here puts the startup values back afterwards,
 * through the same route the page's reset uses.
 *
 * <p>Cassandra is unreachable in this profile, which is what makes the cleanup route's refusal
 * observable: it is the one control on the page that writes.
 */
@QuarkusTest
class SettingsApiTest {

    private static final String STARTUP = """
            {"drones_enabled": 100, "events_per_sec": 2000, "outlier_percent": 5.0,
             "paused": false}""";

    @AfterEach
    void restore() {
        post(STARTUP).statusCode(200);
    }

    /** The names are the frontend's, so a snake_case slip here is a control that stops working. */
    @Test
    void theSettingsInForceAnswerTheNamesThePageReads() {
        when().get("/api/settings/demo")
                .then()
                .statusCode(200)
                .body("success", equalTo(true))
                .body("message", equalTo(""))
                .body("settings.drones_enabled", equalTo(100))
                .body("settings.events_per_sec", equalTo(2000))
                .body("settings.outlier_percent", equalTo(5.0f))
                .body("settings.paused", equalTo(false));
    }

    @Test
    void theDefaultsAreLabelledAsSuch() {
        when().get("/api/settings/demo/defaults")
                .then()
                .statusCode(200)
                .body("message", equalTo("Startup defaults"))
                .body("settings.drones_enabled", equalTo(100));
    }

    /** The round trip the producer depends on: what a POST left behind is what a GET answers. */
    @Test
    void whatWasPostedIsWhatTheProducerWouldPoll() {
        post("""
                {"drones_enabled": 40, "events_per_sec": 800, "outlier_percent": 1.5,
                 "paused": false}""")
                .statusCode(200)
                .body("message",
                        equalTo("Settings updated; the producer picks them up within its poll"
                                + " interval"))
                .body("settings.events_per_sec", equalTo(800));

        when().get("/api/settings/demo")
                .then()
                .body("settings.drones_enabled", equalTo(40))
                .body("settings.events_per_sec", equalTo(800))
                .body("settings.outlier_percent", equalTo(1.5f));
    }

    /** MAX_ENTITIES caps rather than refuses, and the message is how the page learns it did. */
    @Test
    void aFleetOverTheCeilingComesBackCapped() {
        post("""
                {"drones_enabled": 9000, "events_per_sec": 800, "outlier_percent": 5.0,
                 "paused": false}""")
                .statusCode(200)
                .body("message", equalTo("Fleet size capped at MAX_ENTITIES (2000)"))
                .body("settings.drones_enabled", equalTo(2000));
    }

    /** 422 as pydantic answered, and the detail field is the one the dashboard's fetch layer reads. */
    @Test
    void aFigureOutsideItsRangeIsRefusedWithTheFieldNamed() {
        post("""
                {"drones_enabled": 0, "events_per_sec": 800, "outlier_percent": 5.0,
                 "paused": false}""")
                .statusCode(422)
                .body("detail", equalTo("drones_enabled must be between 1 and 100000, got 0"));
    }

    @Test
    void aPostWithNoBodyAtAllIsRefusedRatherThanCrashing() {
        given().contentType(ContentType.JSON)
                .when()
                .post("/api/settings/demo")
                .then()
                .statusCode(422)
                .body("detail", equalTo("Expected a body carrying the four demo settings"));
    }

    @Test
    void thePauseControlTogglesAndSaysWhichWayItWent() {
        when().post("/api/settings/demo/pause")
                .then()
                .statusCode(200)
                .body("message", equalTo("Data generation paused"))
                .body("settings.paused", equalTo(true));

        when().post("/api/settings/demo/pause")
                .then()
                .statusCode(200)
                .body("message", equalTo("Data generation resumed"))
                .body("settings.paused", equalTo(false));
    }

    /** A 200 carrying {@code success: false}, because the page prints the message either way. */
    @Test
    void clearingTheFleetStateWithNoClusterSaysWhyItCouldNot() {
        when().post("/api/settings/demo/cleanup")
                .then()
                .statusCode(200)
                .body("success", equalTo(false))
                .body("message", equalTo("Cassandra not connected"));
    }

    private static ValidatableResponse post(String body) {
        return given().contentType(ContentType.JSON)
                .body(body)
                .when()
                .post("/api/settings/demo")
                .then();
    }
}
