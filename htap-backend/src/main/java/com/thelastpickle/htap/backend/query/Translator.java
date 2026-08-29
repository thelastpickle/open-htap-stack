package com.thelastpickle.htap.backend.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.config.OpenRouterSettings;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * A question translated into SQL by a chat model, when one is configured.
 *
 * <p>Empty on any failure, so the caller falls back to {@link NaturalLanguage}: no key, no network, a
 * refusal, a timeout and an answer that is not a {@code SELECT} are all the same outcome here, which
 * is that the rules translate the question instead. The demo must work with no account anywhere.
 */
@ApplicationScoped
public class Translator {

    private static final Logger LOG = Logger.getLogger(Translator.class);

    /**
     * Enough for a statement and not enough for an essay.
     *
     * <p>A model that has been asked for raw SQL and answers at length has misunderstood, and the
     * answer is discarded below for not beginning {@code SELECT} rather than truncated into
     * something that parses.
     */
    private static final int MAX_TOKENS = 500;

    private final OpenRouterSettings settings;
    private final ObjectMapper json;
    private final HttpClient http;

    @Inject
    Translator(OpenRouterSettings settings, ObjectMapper json) {
        this.settings = settings;
        this.json = json;
        this.http = HttpClient.newBuilder().connectTimeout(settings.timeout()).build();
    }

    /** The statement a model made of the question, or empty. */
    public Optional<String> toSql(String prompt) {
        if (!settings.configured()) {
            return Optional.empty();
        }
        try {
            HttpResponse<String> response = http.send(request(prompt),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                LOG.infof("translation refused with HTTP %d, using the patterns instead",
                        response.statusCode());
                return Optional.empty();
            }
            JsonNode answered = json.readTree(response.body());
            return sql(answered.path("choices").path(0).path("message").path("content").asText(""));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (Exception e) {
            LOG.infof("translation failed, using the patterns instead: %s", e);
            return Optional.empty();
        }
    }

    /**
     * The model's answer as a statement, or empty.
     *
     * <p>A fence is stripped because a model asked for raw SQL sends one anyway, and anything that
     * does not then begin {@code SELECT} is discarded whole: prose, an apology and a {@code DELETE}
     * are all refused the same way, and the statement validator would refuse the last of them again.
     */
    private static Optional<String> sql(String content) {
        String stripped = content.replaceAll("```(?:sql)?", "").strip();
        return stripped.toUpperCase(Locale.ROOT).startsWith("SELECT")
                ? Optional.of(stripped)
                : Optional.empty();
    }

    private HttpRequest request(String prompt) throws Exception {
        String system = """
                You translate questions into a single Presto SQL SELECT statement.
                The only table is %s.
                Return raw SQL only: no prose, no markdown fence, no trailing semicolon."""
                .formatted(NaturalLanguage.SCHEMA);
        Map<String, Object> body = Map.of(
                "model", settings.model(),
                "messages", List.of(
                        Map.of("role", "system", "content", system),
                        Map.of("role", "user", "content", prompt)),
                "max_tokens", MAX_TOKENS,
                // Nothing about translating one question wants variety, and a demo that answers the
                // same question two ways looks broken.
                "temperature", 0.0);
        return HttpRequest.newBuilder(URI.create(settings.url()))
                .timeout(settings.timeout())
                .header("Authorization", "Bearer " + settings.apiKey().orElseThrow())
                .header("Content-Type", "application/json")
                // OpenRouter shows this on the account's activity page, which is how a spend is
                // attributed to this demo rather than to whatever else the key is used for.
                .header("X-Title", "HTAP Mission Control")
                .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)))
                .build();
    }
}
