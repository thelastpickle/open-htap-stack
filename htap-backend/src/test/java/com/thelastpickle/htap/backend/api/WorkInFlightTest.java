package com.thelastpickle.htap.backend.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.thelastpickle.htap.backend.api.dto.QueryInFlight;
import com.thelastpickle.htap.backend.api.dto.RunningWork;
import com.thelastpickle.htap.backend.engine.Engines;
import com.thelastpickle.htap.backend.query.Asked;
import com.thelastpickle.htap.backend.query.QueryPaths;
import com.thelastpickle.htap.backend.query.Run;
import com.thelastpickle.htap.backend.query.RunMode;
import com.thelastpickle.htap.backend.query.SingleRunGate;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What the Health page reads about work in flight.
 *
 * <p>Two of the five paths keep no list, and an engine that cannot be asked is a third case; all
 * three answer with the reason rather than being left out, because this page is what an operator opens
 * when the dashboard has gone slow and a listing that vanishes under exactly those conditions is the
 * wrong shape of failure.
 *
 * <p>The paths come from the container so the names are the running application's, and the two
 * engine clients are built against a server of this test's own.
 */
@QuarkusTest
class WorkInFlightTest {

    private static final Instant NOW = Instant.parse("2026-08-17T12:05:00Z");

    private static final String ONE_QUERY = """
            [{"queryId": "q-1", "state": "RUNNING", "query": "SELECT count(*) FROM demo.events",
              "queryStats": {"createTime": "2026-08-17T12:04:00.000Z"},
              "session": {"user": "htap-mission-control", "source": "htap-dashboard"}}]""";

    private static final String ANOTHERS_QUERY = """
            [{"queryId": "q-2", "state": "RUNNING", "query": "SELECT 2",
              "queryStats": {"createTime": "2026-08-17T12:04:00.000Z"},
              "session": {"user": "somebody-at-a-cli"}}]""";

    private static final String ONE_APPLICATION = """
            [{"id": "app-1", "name": "Thrift JDBC/ODBC Server"}]""";

    private static final String ONE_JOB = """
            [{"jobId": 7, "status": "RUNNING", "description": "SELECT 1",
              "submissionTime": "2026-08-17T12:04:30.041GMT",
              "numTasks": 40, "numCompletedTasks": 9}]""";

    @Inject
    QueryPaths paths;

    @Inject
    SingleRunGate gate;

    private HttpServer server;
    private final Map<String, String> bodies = new ConcurrentHashMap<>();

    @BeforeEach
    void start() throws IOException {
        bodies.put("/v1/query", ONE_QUERY);
        bodies.put("/api/v1/applications", ONE_APPLICATION);
        bodies.put("/api/v1/applications/app-1/jobs", ONE_JOB);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::answer);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    /** No comparison is the ordinary state, and it is null rather than an empty object. */
    @Test
    void withNoComparisonRunningTheFieldIsAbsent() {
        assertNull(work().now().comparison());
    }

    @Test
    void aPrestoQueryCarriesTheSourceItDeclared() {
        QueryInFlight query = engine("presto").getFirst();

        assertEquals("q-1", query.id());
        assertEquals("running", query.state());
        assertEquals("SELECT count(*) FROM demo.events", query.sql());
        assertEquals(60.0, query.runningS());
        assertEquals("htap-dashboard", query.submitter());
        assertEquals(0, query.tasksTotal());
    }

    /** A query that declared no source is attributed to its user, which is what a CLI leaves. */
    @Test
    void aQueryWithNoSourceIsAttributedToItsUser() {
        bodies.put("/v1/query", ANOTHERS_QUERY);

        assertEquals("somebody-at-a-cli", engine("presto").getFirst().submitter());
    }

    /** Spark records no submitter: the Thrift Server submitted every job in it. */
    @Test
    void aSparkJobCarriesItsProgressAndNoSubmitter() {
        QueryInFlight job = engine("spark").getFirst();

        assertEquals("7", job.id());
        assertEquals("SELECT 1", job.sql());
        assertEquals(30.0, job.runningS());
        assertEquals("", job.submitter());
        assertEquals(9, job.tasksDone());
        assertEquals(40, job.tasksTotal());
    }

    /** Two paths keep no list at all, and each says why rather than being omitted. */
    @Test
    void thePathsWithNoListingSayWhy() {
        Map<String, String> unreadable = work().now().unreadable();

        assertEquals(List.of("cassandra", "cqlite"), List.copyOf(unreadable.keySet()));
        assertEquals("Cassandra keeps no list of running queries to read", unreadable.get(
                "cassandra"));
        assertEquals("no scan is running", unreadable.get("cqlite"));
    }

    /**
     * The engine's own words beside an empty list, because an empty list alone says the engine is
     * idle.
     */
    @Test
    void anEngineThatCouldNotBeAskedReportsWhatItSaid() {
        server.stop(0);

        RunningWork now = work().now();

        assertEquals(List.of(), now.queries());
        assertTrue(now.unreadable().get("presto").startsWith(
                "the Presto coordinator could not be reached: "), now.unreadable().toString());
        assertTrue(now.unreadable().get("spark").startsWith("the Spark UI could not be reached: "),
                now.unreadable().toString());
    }

    /** One engine failing must not take the other's listing with it. */
    @Test
    void oneEngineFailingLeavesTheOthersListing() {
        bodies.put("/v1/query", "not json");

        RunningWork now = work().now();

        assertEquals(List.of("spark"), now.queries().stream().map(QueryInFlight::engine).toList());
        assertEquals("the Presto coordinator answered /v1/query with something unreadable",
                now.unreadable().get("presto"));
    }

    /** The gate's run is what the page shows, and it is the same object the cancel button stops. */
    @Test
    void theComparisonHoldingTheGateIsReported() {
        Run run = gate.begin(
                new Asked("SELECT 1", List.of("cassandra"), RunMode.SEQUENTIAL, 10, false),
                List.of());
        try {
            assertEquals("SELECT 1", work().now().comparison().sql());
            assertEquals(List.of("cassandra"), work().now().comparison().engines());
        } finally {
            gate.end(run);
        }
    }

    private List<QueryInFlight> engine(String name) {
        return work().now().queries().stream()
                .filter(query -> query.engine().equals(name))
                .toList();
    }

    private WorkInFlight work() {
        int port = server.getAddress().getPort();
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        return new WorkInFlight(
                gate, paths, Engines.prestoQueries(port, clock), Engines.sparkUi(port, clock));
    }

    private void answer(HttpExchange exchange) throws IOException {
        String body = bodies.getOrDefault(exchange.getRequestURI().getPath(), "[]");
        byte[] answered = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, answered.length);
        try (var out = exchange.getResponseBody()) {
            out.write(answered);
        }
    }
}
