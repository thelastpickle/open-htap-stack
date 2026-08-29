package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.TransactionSchemaReport;
import com.thelastpickle.htap.backend.transaction.TransactionGate;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

/**
 * The transaction routes' three refusals, with nothing reachable.
 *
 * <p>What a transaction does when it reaches a node is asserted by the dashboard step of the
 * workflow, which is the only place an Accord-enabled cluster exists. What is decidable here is the
 * request handling: which refusals are the route's own, and whether a run that failed left the gate
 * shut behind it.
 */
@QuarkusTest
class TransactionsApiTest {

    private static final String SESSION = "6bd0c9f4-1c9a-4a2e-8f5c-6d1f2a3b4c5d";

    @Inject
    TransactionGate gate;

    /** The probe answers in the node's own words rather than failing, which is what it is for. */
    @Test
    void theSchemaReportAnswersWithNoClusterToProbe() {
        when().get("/api/transactions/session/schema")
                .then()
                .statusCode(200)
                .body("ready", equalTo(false))
                .body("note", equalTo(TransactionSchemaReport.WIPE))
                .body("tables", aMapWithSize(3))
                .body("tables.sessions_open", not(equalTo(TransactionSchemaReport.ACCEPTS)));
    }

    @Test
    void aSessionIdentifierThatIsNotAUuidIsRefusedRatherThanBoundAsOne() {
        when().get("/api/transactions/session/txn-demo/not-a-uuid")
                .then()
                .statusCode(400)
                .body("detail", equalTo("session_id is not a UUID"));
    }

    /**
     * A bad identifier is read before the gate is taken, so a run in flight is not what answers.
     *
     * <p>Were the parse inside the gate, this refusal would be reported as a 409 while a
     * demonstration ran, which is a different problem from the one the caller has.
     */
    @Test
    void aBadIdentifierIsRefusedBeforeTheGateIsTaken() {
        assertTrue(gate.tryEnter());
        try {
            given().queryParam("user_id", "txn-demo")
                    .queryParam("session_id", "not-a-uuid")
                    .queryParam("seq", 0)
                    .when()
                    .post("/api/transactions/session/step")
                    .then()
                    .statusCode(400);
        } finally {
            gate.leave();
        }
    }

    /** A sequence number has no nearest sensible answer below zero, so it is refused rather than clamped. */
    @Test
    void aNegativeSequenceNumberIsRefused() {
        given().queryParam("user_id", "txn-demo")
                .queryParam("session_id", SESSION)
                .queryParam("seq", -1)
                .when()
                .post("/api/transactions/session/step")
                .then()
                .statusCode(422)
                .body("detail", equalTo("seq must not be negative"));
    }

    /** Refused and not queued: a caller made to wait would be timed while the run ahead finished. */
    @Test
    void aSecondRunIsRefusedWhileOneHoldsTheGate() {
        assertTrue(gate.tryEnter());
        try {
            when().post("/api/transactions/clearance/demo")
                    .then()
                    .statusCode(409)
                    .body("detail", equalTo("a transaction demo is already running"));
        } finally {
            gate.leave();
        }
    }

    /**
     * A node that cannot answer is temporary, and the gate opens again after it.
     *
     * <p>The second half is the point: the page polls, so a single unreachable run that left the gate
     * shut would refuse every later call with a 409 and say nothing about the node.
     */
    @Test
    void anUnreachableNodeIsTemporaryAndLeavesTheGateOpen() {
        when().post("/api/transactions/clearance/reset").then().statusCode(503);

        assertTrue(gate.tryEnter(), "a failed run left the gate shut");
        gate.leave();
    }

    @Test
    void everyReadOfTheLedgerNeedsTheNodeToo() {
        when().get("/api/transactions/clearance/state").then().statusCode(503);
    }

    /**
     * The step route answers 503 as its siblings do, rather than an unmapped 500.
     *
     * <p>It is the one route whose own code catches the transaction's failure, and both of its paths
     * then read the timeline, which is a second Accord read: with the node down that read raised out
     * of the catch and out of the route, and the page driving the demo a step at a time was given a
     * 500 with no detail to show.
     */
    @Test
    void aStepAgainstAnUnreachableNodeSaysSoRatherThanFailing() {
        given().queryParam("user_id", "txn-demo")
                .queryParam("session_id", SESSION)
                .queryParam("seq", 1)
                .when()
                .post("/api/transactions/session/step")
                .then()
                .statusCode(503)
                // The detail is the defect, not the status: `unavailable` builds it from the
                // failure's message, so a 503 carrying nothing or the string "null" would be the
                // detail-less answer this test exists to rule out.
                .body("detail", not(emptyOrNullString()))
                .body("detail", not(equalTo("null")));
    }
}
