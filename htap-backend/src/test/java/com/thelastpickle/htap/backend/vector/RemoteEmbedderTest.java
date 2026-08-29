package com.thelastpickle.htap.backend.vector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The embeddings endpoint, against a server of this test's own.
 *
 * <p>What is worth testing is the two ways a failure is treated: an endpoint that will not answer
 * degrades to the local embedder, and one that answers with the wrong width is refused, because
 * accepting it would leave the one column holding two embedding spaces.
 */
class RemoteEmbedderTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private HttpServer server;
    private final LocalEmbedder local = new LocalEmbedder();
    private final AtomicReference<String> asked = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<String> sent = new AtomicReference<>();
    private final AtomicReference<String> body = new AtomicReference<>(embedding(1536));
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
    void theEndpointsVectorIsWhatIsReturned() {
        float[] vector = embedder("").embed("a restricted zone");

        assertEquals(1536, vector.length);
        assertEquals(0.0f, vector[0]);
        assertEquals(1.0f, vector[1]);
        assertEquals(2.0f, vector[2]);
    }

    /** The path, the model and the key are the request's whole contract. */
    @Test
    void theRequestNamesTheModelAndCarriesTheKey() throws IOException {
        embedder("").embed("a restricted zone");

        assertEquals("/embeddings", asked.get());
        assertEquals("Bearer a-test-key", authorization.get());
        assertEquals("text-embedding-3-small", JSON.readTree(sent.get()).path("model").asText());
        assertEquals("a restricted zone", JSON.readTree(sent.get()).path("input").asText());
    }

    /** A base URL is configured either way round, and neither gives {@code //embeddings}. */
    @Test
    void aTrailingSlashOnTheBaseUrlIsNotDoubled() {
        embedder("/").embed("a restricted zone");

        assertEquals("/embeddings", asked.get());
    }

    @Test
    void anEndpointThatRefusesTheRequestFallsBackToTheLocalEmbedder() {
        status = 401;

        assertArrayEquals(local.embed("a restricted zone"), embedder("").embed("a restricted zone"));
    }

    @Test
    void anUnreadableAnswerFallsBackToTheLocalEmbedder() {
        body.set("not json");

        assertArrayEquals(local.embed("a restricted zone"), embedder("").embed("a restricted zone"));
    }

    @Test
    void anEndpointThatIsNotListeningFallsBackToTheLocalEmbedder() {
        Embedder remote = embedder("");
        server.stop(0);

        assertArrayEquals(local.embed("a restricted zone"), remote.embed("a restricted zone"));
    }

    /**
     * Not a fallback: a model of the wrong width is a misconfiguration, and embedding half a table
     * with one model and half with another would make every similarity between them meaningless.
     */
    @Test
    void aWrongDimensionCountIsRefusedRatherThanSoftened() {
        body.set(embedding(768));

        EmbeddingFailed refused =
                assertThrows(EmbeddingFailed.class, () -> embedder("").embed("a restricted zone"));

        assertEquals(
                "Embedding model returned 768 dimensions but the payload_vector column holds 1536",
                refused.getMessage());
    }

    @Test
    void theEmbedderNamesItselfAsTheExplorePageReportsIt() {
        assertEquals("remote", embedder("").kind());
    }

    /** A response whose vector is 0, 1, 2, …, so a reordering or an off-by-one would show. */
    private static String embedding(int dimensions) {
        StringBuilder vector = new StringBuilder();
        for (int i = 0; i < dimensions; i++) {
            vector.append(i == 0 ? "" : ",").append(i);
        }
        return "{\"data\": [{\"embedding\": [" + vector + "]}]}";
    }

    private void answer(HttpExchange exchange) throws IOException {
        asked.set(exchange.getRequestURI().getPath());
        authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
        sent.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        byte[] answered = body.get().getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, answered.length);
        try (var out = exchange.getResponseBody()) {
            out.write(answered);
        }
    }

    private Embedder embedder(String trailing) {
        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + trailing;
        return new RemoteEmbedder(Vectors.endpointAt(baseUrl), JSON, local);
    }
}
