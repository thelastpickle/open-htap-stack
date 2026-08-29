package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.thelastpickle.htap.backend.config.OpenRouterSettings;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The model translator, and the several ways it declines.
 *
 * <p>Every failure is the same outcome, which is empty, so the caller falls back to the rules: the
 * demo has to work with no account anywhere. What is worth testing is that each of those failures
 * really is empty rather than an exception reaching the route, and that an answer which is not a
 * {@code SELECT} is discarded whole rather than repaired.
 */
class TranslatorTest {

    private HttpServer server;
    private final AtomicReference<String> body = new AtomicReference<>(answer("SELECT 1"));
    private final AtomicReference<String> sent = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private volatile int status = 200;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::respond);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    /** No key is the default state of the demo, and it must cost no request at all. */
    @Test
    void withNoKeyNothingIsAsked() {
        assertTrue(translator(Optional.empty()).toSql("hottest drones").isEmpty());
        assertNull(sent.get());
    }

    /** A key of whitespace is a compose variable somebody left blank, and is no key. */
    @Test
    void aBlankKeyIsNoKey() {
        assertTrue(translator(Optional.of("   ")).toSql("hottest drones").isEmpty());
        assertNull(sent.get());
    }

    @Test
    void theStatementAModelAnsweredIsTheTranslation() {
        body.set(answer("SELECT entity_id FROM demo.drone_latest_status"));

        assertEquals("SELECT entity_id FROM demo.drone_latest_status",
                translator().toSql("list the drones").orElseThrow());
    }

    /** A model asked for raw SQL sends a fence anyway, so one is taken off rather than refused. */
    @Test
    void aFencedStatementIsUnwrapped() {
        body.set(answer("```sql\nSELECT 1\n```"));

        assertEquals("SELECT 1", translator().toSql("anything").orElseThrow());
    }

    /** Prose, an apology and a statement that writes are all discarded the same way. */
    @Test
    void anAnswerThatIsNotASelectIsDiscardedWhole() {
        for (String content : new String[] {
            "I am sorry, I cannot help with that.",
            "DELETE FROM demo.drone_latest_status",
            "Here is the SQL: SELECT 1",
            ""
        }) {
            body.set(answer(content));

            assertTrue(translator().toSql("anything").isEmpty(), content);
        }
    }

    /** A model that answered nothing at the path the reader walks is empty, not a failure. */
    @Test
    void anAnswerWithNoChoicesIsEmpty() {
        body.set("{}");

        assertTrue(translator().toSql("anything").isEmpty());
    }

    @Test
    void aRefusalIsEmptyRatherThanAFailure() {
        status = 429;

        assertTrue(translator().toSql("anything").isEmpty());
    }

    @Test
    void aBodyThatWillNotParseIsEmpty() {
        body.set("not json");

        assertTrue(translator().toSql("anything").isEmpty());
    }

    @Test
    void anEndpointThatIsNotListeningIsEmpty() {
        server.stop(0);

        assertTrue(translator().toSql("anything").isEmpty());
    }

    /**
     * The request is the contract with the account: the key travels as a bearer token, the model is
     * the configured one, and the temperature is zero because a demo that answers one question two
     * ways looks broken.
     */
    @Test
    void theRequestNamesTheModelTheSchemaAndNoVariety() throws IOException {
        translator().toSql("hottest drones");

        JsonNode asked = new ObjectMapper().readTree(sent.get());
        assertEquals("Bearer test-key", authorization.get());
        assertEquals("openai/gpt-4o-mini", asked.path("model").asText());
        assertEquals(0.0, asked.path("temperature").asDouble());
        assertEquals(500, asked.path("max_tokens").asInt());
        assertEquals("system", asked.path("messages").path(0).path("role").asText());
        assertTrue(asked.path("messages").path(0).path("content").asText()
                .contains(NaturalLanguage.SCHEMA));
        assertEquals("user", asked.path("messages").path(1).path("role").asText());
        assertEquals("hottest drones", asked.path("messages").path(1).path("content").asText());
    }

    private static String answer(String content) {
        return "{\"choices\": [{\"message\": {\"content\": %s}}]}"
                .formatted(new ObjectMapper().valueToTree(content));
    }

    private void respond(HttpExchange exchange) throws IOException {
        authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
        sent.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        byte[] answered = body.get().getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, answered.length);
        try (var out = exchange.getResponseBody()) {
            out.write(answered);
        }
    }

    private Translator translator() {
        return translator(Optional.of("test-key"));
    }

    private Translator translator(Optional<String> apiKey) {
        return new Translator(settings(apiKey, server.getAddress().getPort()), new ObjectMapper());
    }

    private static OpenRouterSettings settings(Optional<String> apiKey, int port) {
        return new OpenRouterSettings() {
            @Override
            public Optional<String> apiKey() {
                return apiKey;
            }

            @Override
            public String model() {
                return "openai/gpt-4o-mini";
            }

            @Override
            public String url() {
                return "http://127.0.0.1:" + port + "/api/v1/chat/completions";
            }

            @Override
            public Duration timeout() {
                return Duration.ofSeconds(5);
            }
        };
    }
}
