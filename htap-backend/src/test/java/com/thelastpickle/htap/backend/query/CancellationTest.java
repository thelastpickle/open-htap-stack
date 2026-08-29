package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.thelastpickle.htap.backend.engine.Engines;
import com.thelastpickle.htap.backend.engine.QueryPath;
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
 * Stopping a comparison, and which of the four mechanisms each path needs.
 *
 * <p>What has to hold is that a cancel stops this backend's own work and nothing else: a
 * {@code presto-cli} session and a {@code spark-sql} job in the same containers must survive it, so
 * the Presto half is filtered by user and the Spark half matched by statement. A mechanism that fails
 * is reported and does not stop the others, because a coordinator that cannot be reached is no reason
 * to leave a Spark job running.
 */
class CancellationTest {

    private static final Instant NOW = Instant.parse("2026-08-17T12:05:00Z");

    /** One query of this backend's own and one of somebody else's, both running. */
    private static final String TWO_QUERIES = """
            [
              {"queryId": "ours", "state": "RUNNING", "query": "SELECT 1",
               "queryStats": {"createTime": "2026-08-17T12:04:00.000Z"},
               "session": {"user": "htap-mission-control"}},
              {"queryId": "theirs", "state": "RUNNING", "query": "SELECT 2",
               "queryStats": {"createTime": "2026-08-17T12:04:00.000Z"},
               "session": {"user": "someone-at-a-cli"}}
            ]""";

    private static final String ONE_APPLICATION = """
            [{"id": "app-1", "name": "Thrift JDBC/ODBC Server"}]""";

    private static final String TWO_JOBS = """
            [
              {"jobId": 12, "status": "RUNNING", "description": "SELECT 1 /* spark */ LIMIT 10",
               "submissionTime": "2026-08-17T12:04:00.041GMT"},
              {"jobId": 99, "status": "RUNNING", "description": "SELECT * FROM somebody_elses",
               "submissionTime": "2026-08-17T12:04:00.041GMT"}
            ]""";

    private final FakePath cassandra = new FakePath("cassandra");
    private final FakePath presto = new FakePath("presto");
    private final FakePath spark = new FakePath("spark");
    private final FakePath sparkBulk = new FakePath("spark_bulk");
    private final FakePath cqlite = new FakePath("cqlite");

    private final QueryPaths paths = new QueryPaths(
            List.<QueryPath>of(cassandra, presto, spark, sparkBulk, cqlite));
    private final SingleRunGate gate = new SingleRunGate();

    private HttpServer server;
    private final List<String> asked = new CopyOnWriteArrayList<>();
    private final Map<String, String> bodies = new ConcurrentHashMap<>();

    @BeforeEach
    void start() throws IOException {
        bodies.put("/v1/query", TWO_QUERIES);
        bodies.put("/api/v1/applications", ONE_APPLICATION);
        bodies.put("/api/v1/applications/app-1/jobs", TWO_JOBS);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::answer);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    /** Answered as a refusal by the route, since a control that stopped nothing reads as working. */
    @Test
    void withNoComparisonRunningThereIsNothingToStop() {
        assertEquals(List.of(), cancellation().cancel());
    }

    /** The flag first, because it is what stops the paths a sequential run has not reached. */
    @Test
    void everyMechanismTheRunNeedsFiresAndSaysSo() {
        Run run = begin("cassandra", "presto", "spark", "spark_bulk", "cqlite");

        List<String> actions = cancellation().cancel();

        assertTrue(run.cancelled());
        assertEquals(
                List.of("stopped the paths that had not started yet",
                        "cancelled 1 Presto quer(y/ies): ours",
                        "took the connection away from spark",
                        "took the connection away from spark_bulk",
                        "stopped the cqlite scan",
                        "killed 1 Spark job(s): 12"),
                actions);
    }

    /** A {@code presto-cli} session in the container must survive a cancel here. */
    @Test
    void onlyThisBackendsOwnPrestoQueriesAreCancelled() {
        begin("presto");

        cancellation().cancel();

        assertEquals(List.of("DELETE /v1/query/ours"),
                asked.stream().filter(request -> request.startsWith("DELETE")).toList());
    }

    /** Matched by the statement the run recorded, so a {@code spark-sql} job beside it is left alone. */
    @Test
    void onlyTheSparkJobsRunningThisRunsStatementsAreKilled() {
        begin("spark");

        cancellation().cancel();

        assertEquals(List.of("POST /jobs/job/kill/?id=12"),
                asked.stream().filter(request -> request.contains("kill")).toList());
    }

    /** A path the run never asked for is not stopped, so nothing else on it is disturbed. */
    @Test
    void aPathTheRunNeverAskedForIsLeftAlone() {
        begin("cassandra");

        assertEquals(List.of("stopped the paths that had not started yet"),
                cancellation().cancel());
        assertEquals(0, spark.aborts());
        assertEquals(0, cqlite.aborts());
        assertEquals(List.of(), asked);
    }

    /** Cassandra is absent on purpose: its legs are milliseconds, so there is never one to stop. */
    @Test
    void aPathWithNothingToStopReportsNothing() {
        spark.withNothingToAbort();
        begin("spark");

        List<String> actions = cancellation().cancel();

        assertEquals(1, spark.aborts());
        assertTrue(actions.stream().noneMatch(line -> line.contains("connection away")), actions
                .toString());
    }

    /** A coordinator that cannot be reached is no reason to leave the Spark job running. */
    @Test
    void aMechanismThatFailsIsReportedAndTheOthersStillFire() {
        bodies.put("/v1/query", "not json");
        begin("presto", "cqlite");

        List<String> actions = cancellation().cancel();

        assertEquals("could not cancel the Presto query: the Presto coordinator answered "
                + "/v1/query with something unreadable", actions.get(1));
        assertEquals("stopped the cqlite scan", actions.get(2));
    }

    private Run begin(String... engines) {
        List<String> chosen = List.of(engines);
        List<String> statements = chosen.stream()
                .filter(name -> name.startsWith("spark"))
                .map(name -> paths.byName(name).orElseThrow().dialect("SELECT 1", 10))
                .toList();
        return gate.begin(
                new Asked("SELECT 1", chosen, RunMode.SEQUENTIAL, 10, false), statements);
    }

    private Cancellation cancellation() {
        int port = server.getAddress().getPort();
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        return new Cancellation(
                gate,
                paths,
                Engines.prestoQueries(port, clock),
                Engines.prestoAt(port),
                Engines.sparkUi(port, clock));
    }

    private void answer(HttpExchange exchange) throws IOException {
        asked.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
        String body = bodies.getOrDefault(exchange.getRequestURI().getPath(), "");
        byte[] answered = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, answered.length);
        try (var out = exchange.getResponseBody()) {
            out.write(answered);
        }
    }
}
