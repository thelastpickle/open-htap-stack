package com.thelastpickle.htap.backend.vector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.config.VectorSettings;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.jboss.logging.Logger;

/**
 * An OpenAI-compatible embeddings endpoint, with the local embedder underneath it.
 *
 * <p>Any failure reaching the endpoint answers locally and logs why, so a key that has expired
 * degrades the demo's ranking rather than stopping it. The one refusal that is not softened is a
 * wrong dimension count; see {@link EmbeddingFailed}.
 */
public final class RemoteEmbedder implements Embedder {

    private static final Logger LOG = Logger.getLogger(RemoteEmbedder.class);

    private final VectorSettings settings;
    private final ObjectMapper json;
    private final LocalEmbedder local;
    private final HttpClient http;

    public RemoteEmbedder(VectorSettings settings, ObjectMapper json, LocalEmbedder local) {
        this.settings = settings;
        this.json = json;
        this.local = local;
        this.http = HttpClient.newBuilder().connectTimeout(settings.timeout()).build();
    }

    @Override
    public String kind() {
        return "remote";
    }

    @Override
    public float[] embed(String text) {
        float[] vector;
        try {
            vector = request(text);
        } catch (EmbeddingFailed e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warnf("embedding endpoint interrupted, using local embedder");
            return local.embed(text);
        } catch (Exception e) {
            LOG.warnf(e, "embedding endpoint failed, using local embedder");
            return local.embed(text);
        }
        if (vector.length != LocalEmbedder.DIMENSIONS) {
            throw new EmbeddingFailed("Embedding model returned " + vector.length
                    + " dimensions but the payload_vector column holds "
                    + LocalEmbedder.DIMENSIONS);
        }
        return vector;
    }

    private float[] request(String text) throws Exception {
        String body = json.writeValueAsString(Map.of("model", settings.model(), "input", text));
        HttpRequest post = HttpRequest.newBuilder(URI.create(endpoint()))
                .timeout(settings.timeout())
                .header("Authorization", "Bearer " + settings.apiKey().orElseThrow())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = http.send(post, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException(
                    "HTTP " + response.statusCode() + " from " + endpoint());
        }
        JsonNode embedding = json.readTree(response.body()).path("data").path(0).path("embedding");
        float[] vector = new float[embedding.size()];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = (float) embedding.get(i).asDouble();
        }
        return vector;
    }

    private String endpoint() {
        String base = settings.baseUrl();
        return (base.endsWith("/") ? base.substring(0, base.length() - 1) : base) + "/embeddings";
    }
}
