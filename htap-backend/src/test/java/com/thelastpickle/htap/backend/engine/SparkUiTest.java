package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.thelastpickle.htap.backend.config.SparkSettings;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Seeing and killing Spark jobs over the application UI.
 *
 * <p>Against a server of this test's own, because what has to hold is that a cancel kills this
 * backend's own jobs and nothing else: the Thrift Server is one application shared by everything that
 * connects to it, so matching is the whole of the safety here.
 */
class SparkUiTest {

    private static final Instant NOW = Instant.parse("2026-08-17T12:02:52.041Z");

    private static final String ONE_APPLICATION = """
            [{"id": "app-20260817120000-0000", "name": "Thrift JDBC/ODBC Server"}]""";

    /** A labelled job, one with only a call site, one with an explicit null, and one nameless. */
    private static final String FOUR_JOBS = """
            [
              {"jobId": 12, "status": "RUNNING",
               "description": "SELECT count(*)\\n  FROM demo.events",
               "submissionTime": "2026-08-17T12:01:52.041GMT",
               "numTasks": 200, "numCompletedTasks": 37},
              {"jobId": 13, "status": "running", "name": "collect at Dataset.scala:3242",
               "submissionTime": "2026-08-17T12:02:22.041GMT",
               "numTasks": 8, "numCompletedTasks": 8},
              {"jobId": 14, "description": null, "name": null,
               "submissionTime": "2026-08-17T12:02:52.041GMT"},
              {"jobId": 15}
            ]""";

    private HttpServer server;
    private final List<String> asked = new CopyOnWriteArrayList<>();
    private final Map<String, String> bodies = new ConcurrentHashMap<>();
    private volatile int killStatus = 200;

    @BeforeEach
    void start() throws IOException {
        bodies.put("/api/v1/applications", ONE_APPLICATION);
        bodies.put("/api/v1/applications/app-20260817120000-0000/jobs", FOUR_JOBS);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::answer);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    /** The UI lists one application per Java Virtual Machine, and this container runs only one. */
    @Test
    void theFirstApplicationIsTheThriftServers() {
        assertEquals("app-20260817120000-0000", sparkUi().applicationId().orElseThrow());
    }

    /** No application means the Thrift Server has not started, which is no jobs rather than a failure. */
    @Test
    void aUiWithNoApplicationHasNoJobs() {
        bodies.put("/api/v1/applications", "[]");

        assertTrue(sparkUi().applicationId().isEmpty());
        assertEquals(List.of(), sparkUi().runningJobs());
    }

    /** Filtered server-side: the SQL endpoint answers every execution since start-up with its plan. */
    @Test
    void onlyRunningJobsAreAskedFor() {
        sparkUi().runningJobs();

        assertEquals(
                List.of("GET /api/v1/applications",
                        "GET /api/v1/applications/app-20260817120000-0000/jobs?status=running"),
                asked);
    }

    @Test
    void aJobCarriesItsStatementItsAgeAndItsProgress() {
        SparkJob first = sparkUi().runningJobs().getFirst();

        assertEquals("12", first.id());
        assertEquals("running", first.state());
        assertEquals("SELECT count(*) FROM demo.events", first.sql());
        assertEquals(60.0, first.runningS());
        assertEquals(37, first.tasksDone());
        assertEquals(200, first.tasksTotal());
    }

    /**
     * The description is the statement the Thrift Server set for the job group, and the call site is
     * all there is for a job submitted any other way. An explicitly null field arrives too, and
     * {@code JsonNode.asText} would answer the four characters "null" for it.
     */
    @Test
    void aJobWithNoDescriptionFallsBackToItsCallSiteAndThenToAName() {
        List<String> statements = sparkUi().runningJobs().stream().map(SparkJob::sql).toList();

        assertEquals(
                List.of("SELECT count(*) FROM demo.events", "collect at Dataset.scala:3242",
                        "a Spark job", "a Spark job"),
                statements);
    }

    /** A job the UI reported without a status is running: it came back from the running listing. */
    @Test
    void aJobWithNoStatusIsRunning() {
        assertEquals("running", sparkUi().runningJobs().getLast().state());
    }

    /** A statement is matched as the job list reports it, so the two collapse the same way. */
    @Test
    void onlyTheJobsWorkingOnTheseStatementsAreKilled() {
        List<String> killed = sparkUi().killJobsFor(List.of("SELECT count(*)\n   FROM demo.events"));

        assertEquals(List.of("12"), killed);
        assertEquals(List.of("POST /jobs/job/kill/?id=12"), kills());
    }

    /** A {@code spark-sql} session in the container must survive a comparison being cancelled. */
    @Test
    void aJobNobodyAskedAboutIsLeftAlone() {
        assertEquals(List.of(), sparkUi().killJobsFor(List.of("SELECT 1")));
        assertEquals(List.of(), kills());
    }

    /** Killing needs no statements at all when the comparison ran none, and asks nothing. */
    @Test
    void nothingToMatchKillsNothing() {
        assertEquals(List.of(), sparkUi().killJobsFor(List.of()));
    }

    /** The UI's own kill link, which is a {@code POST}: a {@code GET} on it does nothing. */
    @Test
    void aKillIsAPostCarryingTheJobId() {
        sparkUi().killJob("12");

        assertEquals(List.of("POST /jobs/job/kill/?id=12"), kills());
    }

    /**
     * The handler answers a redirect to the jobs page, so a 3xx is acceptance. A 4xx is the one
     * setting this depends on being off, and the message says so rather than reporting a bare status.
     */
    @Test
    void aRedirectIsAcceptanceAndARefusalNamesTheSettingItNeeds() {
        killStatus = 302;
        sparkUi().killJob("12");

        killStatus = 405;

        EngineFailed refused = assertThrows(EngineFailed.class, () -> sparkUi().killJob("12"));

        assertEquals("the Spark UI refused to kill job 12 (HTTP 405); spark.ui.killEnabled must be on",
                refused.getMessage());
    }

    @Test
    void aUiThatIsNotListeningCouldNotBeReached() {
        server.stop(0);

        EngineUnavailable gone =
                assertThrows(EngineUnavailable.class, () -> sparkUi().runningJobs());

        assertTrue(gone.getMessage().startsWith("the Spark UI could not be reached: "));
    }

    private List<String> kills() {
        return asked.stream().filter(request -> request.contains("/jobs/job/kill")).toList();
    }

    private void answer(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        asked.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
        String body = bodies.get(path);
        int status = body == null ? killStatus : 200;
        byte[] answered = (body == null ? "" : body).getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, answered.length);
        try (var out = exchange.getResponseBody()) {
            out.write(answered);
        }
    }

    private SparkUi sparkUi() {
        return Engines.sparkUi(server.getAddress().getPort(), Clock.fixed(NOW, ZoneOffset.UTC));
    }
}
