package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.thelastpickle.htap.backend.config.PrestoSettings;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reading and cancelling Presto work over the coordinator's REST interface.
 *
 * <p>Against a server of this test's own, because the URLs and the methods are the contract: the
 * cancel is a {@code DELETE} on the query's own path, and any other request would be accepted by a
 * coordinator and cancel nothing.
 */
class PrestoQueriesTest {

    private static final Instant NOW = Instant.parse("2026-08-17T12:05:00Z");

    /**
     * Three states that are over and three that are not, so the filter is tested rather than the
     * absence of finished queries from one particular sample.
     */
    private static final String SIX_QUERIES = """
            [
              {"queryId": "20260817_120100_00001_abcde", "state": "RUNNING",
               "query": "SELECT count(*) FROM cassandra.demo.events",
               "queryStats": {"createTime": "2026-08-17T12:01:00.000Z"},
               "session": {"user": "htap-mission-control", "source": "htap-dashboard"}},
              {"queryId": "20260817_120400_00002_abcde", "state": "PLANNING",
               "query": "SELECT 2", "queryStats": {"createTime": "2026-08-17T12:04:00.000Z"},
               "session": {"user": "someone-else", "source": ""}},
              {"queryId": "20260817_120200_00003_abcde", "state": "QUEUED",
               "query": "SELECT 3", "queryStats": {"createTime": "2026-08-17T12:02:00.000Z"},
               "session": {"user": "htap-mission-control"}},
              {"queryId": "finished", "state": "FINISHED", "query": "SELECT 4",
               "queryStats": {"createTime": "2026-08-17T12:00:00.000Z"}, "session": {}},
              {"queryId": "failed", "state": "FAILED", "query": "SELECT 5",
               "queryStats": {"createTime": "2026-08-17T12:00:00.000Z"}, "session": {}},
              {"queryId": "cancelled", "state": "CANCELED", "query": "SELECT 6",
               "queryStats": {"createTime": "2026-08-17T12:00:00.000Z"}, "session": {}}
            ]""";

    private HttpServer server;
    private final AtomicReference<String> asked = new AtomicReference<>();
    private final AtomicReference<String> method = new AtomicReference<>();
    private final AtomicReference<String> body = new AtomicReference<>(SIX_QUERIES);
    private volatile int status = 200;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::answer);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    /** {@code PLANNING} is exactly where a query worth noticing gets stuck, so it is running here. */
    @Test
    void aQueryTheCoordinatorHasFinishedWithIsNotRunning() {
        assertEquals(
                List.of("20260817_120100_00001_abcde", "20260817_120200_00003_abcde",
                        "20260817_120400_00002_abcde"),
                queries().running().stream().map(RunningQuery::id).sorted().toList());
    }

    /** Longest-running first, because that is the one an operator opened the page about. */
    @Test
    void theLongestRunningQueryIsFirst() {
        assertEquals(
                List.of(240.0, 180.0, 60.0),
                queries().running().stream().map(RunningQuery::runningS).toList());
    }

    @Test
    void aStateIsLowerCasedAndTheSessionCarriesTheAttribution() {
        RunningQuery first = queries().running().getFirst();

        assertEquals("running", first.state());
        assertEquals("SELECT count(*) FROM cassandra.demo.events", first.sql());
        assertEquals("htap-mission-control", first.user());
        assertEquals("htap-dashboard", first.source());
    }

    /** A session field the coordinator omits is empty, not the four characters "null". */
    @Test
    void aQueryWithNoSourceReportsAnEmptyOne() {
        RunningQuery queued = queries().running().stream()
                .filter(query -> query.state().equals("queued"))
                .findFirst()
                .orElseThrow();

        assertEquals("", queued.source());
    }

    /** The page shows a line rather than a plan, and the cut is the same one a Spark label uses. */
    @Test
    void aStatementIsFlattenedAndCutAtThreeHundredCharacters() {
        String sql = "SELECT\n  " + "x".repeat(400);
        body.set(oneQuery(sql));

        String reported = queries().running().getFirst().sql();

        assertEquals(300, reported.length());
        assertEquals("SELECT " + "x".repeat(293), reported);
    }

    /** The path is the contract: a cancel aimed anywhere else is accepted and cancels nothing. */
    @Test
    void aCancelIsADeleteOnTheQuerysOwnPath() {
        queries().kill("20260817_120100_00001_abcde");

        assertEquals("DELETE", method.get());
        assertEquals("/v1/query/20260817_120100_00001_abcde", asked.get());
    }

    @Test
    void aCoordinatorThatRefusesTheCancelSaysWhichQueryAndWhichStatus() {
        status = 403;

        EngineFailed refused =
                assertThrows(EngineFailed.class, () -> queries().kill("20260817_120100_00001_abcde"));

        assertEquals("Presto refused to cancel 20260817_120100_00001_abcde (HTTP 403)",
                refused.getMessage());
    }

    /**
     * The two failures are told apart by type, because the running-work page reports an engine it
     * could not reach and one that refused it differently.
     */
    @Test
    void aCoordinatorThatIsNotListeningIsUnavailableAndOneThatRefusesHasFailed() {
        status = 500;

        assertThrows(EngineFailed.class, () -> queries().running());

        server.stop(0);

        EngineUnavailable gone = assertThrows(EngineUnavailable.class, () -> queries().running());

        assertTrue(gone.getMessage().startsWith("the Presto coordinator could not be reached: "));
    }

    @Test
    void aListingThatWillNotParseIsAFailureNamingTheRequest() {
        body.set("not json");

        EngineFailed unreadable = assertThrows(EngineFailed.class, () -> queries().running());

        assertEquals("the Presto coordinator answered /v1/query with something unreadable",
                unreadable.getMessage());
    }

    private static String oneQuery(String sql) {
        return """
                [{"queryId": "q", "state": "RUNNING", "query": %s,
                  "queryStats": {"createTime": "2026-08-17T12:04:00.000Z"}, "session": {}}]"""
                .formatted(new ObjectMapper().valueToTree(sql));
    }

    private void answer(HttpExchange exchange) throws IOException {
        asked.set(exchange.getRequestURI().getPath());
        method.set(exchange.getRequestMethod());
        byte[] answered = body.get().getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, answered.length);
        try (var out = exchange.getResponseBody()) {
            out.write(answered);
        }
    }

    private PrestoQueries queries() {
        return Engines.prestoQueries(
                server.getAddress().getPort(), Clock.fixed(NOW, ZoneOffset.UTC));
    }
}
