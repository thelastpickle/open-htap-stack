package com.thelastpickle.htap.backend.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * How large the snapshot a bulk read is about to read is, and what happens when the Sidecar will
 * not say.
 *
 * <p>Against a server of this test's own, because the URL is as much of the contract as the sum
 * is: the Sidecar answers 404 for a path it does not recognise, and a wrong path would then look
 * exactly like a snapshot that had expired.
 */
class SidecarTest {

    /** Two components of one SSTable and one of another, so a sum is not just the first file. */
    private static final String THREE_FILES = """
            {"snapshotFilesInfo": [
              {"fileName": "nb-1-big-Data.db", "size": 488777346},
              {"fileName": "nb-1-big-Index.db", "size": 1024},
              {"fileName": "nb-2-big-Data.db", "size": 96}
            ]}""";

    private HttpServer server;
    private final AtomicReference<String> asked = new AtomicReference<>();
    private final AtomicReference<String> body = new AtomicReference<>(THREE_FILES);
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

    @Test
    void everyComponentOfTheSnapshotCounts() {
        assertEquals(OptionalLong.of(488_778_466L), sidecar().snapshotBytes("events", "snap-1"));
    }

    /** The path the Sidecar publishes, which a 404 would otherwise be indistinguishable from. */
    @Test
    void theKeyspaceTheTableAndTheSnapshotAreAllInThePath() {
        sidecar().snapshotBytes("events", "htap_dashboard_events_1756382400_7");

        assertEquals(
                "/api/v1/keyspaces/demo/tables/events/snapshots/htap_dashboard_events_1756382400_7",
                asked.get());
    }

    /**
     * A snapshot the Sidecar will not list is absent rather than zero, and the bulk path reads that
     * as a snapshot that has gone: absent is what makes it take a fresh one.
     */
    @Test
    void aSnapshotTheSidecarWillNotListIsAbsent() {
        status = 404;
        body.set("{}");

        assertTrue(sidecar().snapshotBytes("events", "gone").isEmpty());
    }

    @Test
    void anAnswerThatWillNotParseIsAbsent() {
        body.set("not json");

        assertTrue(sidecar().snapshotBytes("events", "snap-1").isEmpty());
    }

    /** A listing with no files is zero rather than absent: the snapshot exists and is empty. */
    @Test
    void anEmptyListingIsZeroBytes() {
        body.set("{\"snapshotFilesInfo\": []}");

        assertEquals(OptionalLong.of(0), sidecar().snapshotBytes("events", "snap-1"));
    }

    /** A file the Sidecar reported without a size counts as nothing rather than failing the sum. */
    @Test
    void aFileWithNoSizeIsCountedAsNothing() {
        body.set("{\"snapshotFilesInfo\": [{\"fileName\": \"nb-1-big-Data.db\"}, {\"size\": 7}]}");

        assertEquals(OptionalLong.of(7), sidecar().snapshotBytes("events", "snap-1"));
    }

    /** Failing to size a snapshot must not fail the read that is about to happen. */
    @Test
    void aSidecarThatIsNotListeningIsAbsentRatherThanRaising() {
        server.stop(0);

        assertFalse(sidecar().snapshotBytes("events", "snap-1").isPresent());
    }

    private void answer(HttpExchange exchange) throws IOException {
        asked.set(exchange.getRequestURI().getPath());
        byte[] answered = body.get().getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, answered.length);
        try (var out = exchange.getResponseBody()) {
            out.write(answered);
        }
    }

    private Sidecar sidecar() {
        return new Sidecar(settings(server.getAddress().getPort()), new ObjectMapper());
    }

    private static CassandraSettings settings(int sidecarPort) {
        return new CassandraSettings() {
            @Override
            public String host() {
                return "127.0.0.1";
            }

            @Override
            public int port() {
                return 9042;
            }

            @Override
            public String keyspace() {
                return "demo";
            }

            @Override
            public String datacenter() {
                return "datacenter1";
            }

            @Override
            public int sidecarPort() {
                return sidecarPort;
            }

            @Override
            public Optional<String> translateAddressesTo() {
                return Optional.empty();
            }
        };
    }
}
