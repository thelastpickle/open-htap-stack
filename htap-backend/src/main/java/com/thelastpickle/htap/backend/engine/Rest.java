package com.thelastpickle.htap.backend.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Asking an engine's own web interface, for the two engines that have one.
 *
 * <p>Presto's coordinator and Spark's application UI are read for the same reason: a connection busy
 * with a query is exactly the connection that cannot be asked about it, and neither engine can be
 * asked to cancel over the connection it is working on. The two failure kinds are worth telling
 * apart by the caller, so a server that cannot be reached raises {@link EngineUnavailable} and one
 * that answers a refusal raises {@link EngineFailed}.
 */
final class Rest {

    private final String subject;
    private final Duration timeout;
    private final ObjectMapper json;
    private final HttpClient http;

    /**
     * @param subject how the server is named in a failure, in the words the page shows
     * @param timeout the bound on one request; short, because the running-work page polls these
     */
    Rest(String subject, Duration timeout, ObjectMapper json) {
        this.subject = subject;
        this.timeout = timeout;
        this.json = json;
        // Redirects are not followed, which is the default and is what the Spark kill handler needs:
        // it answers a redirect to its jobs page, and following it would fetch an HTML page to
        // learn nothing the status has not already said.
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    /** The parsed body of a {@code GET}, or a failure naming which request it was. */
    JsonNode get(URI url) {
        HttpResponse<String> response = send(request(url).GET().build());
        if (response.statusCode() / 100 != 2) {
            throw new EngineFailed(
                    subject + " answered HTTP " + response.statusCode() + " for " + url.getPath());
        }
        try {
            return json.readTree(response.body());
        } catch (Exception e) {
            throw new EngineFailed(
                    subject + " answered " + url.getPath() + " with something unreadable", e);
        }
    }

    HttpRequest.Builder request(URI url) {
        return HttpRequest.newBuilder(url).timeout(timeout);
    }

    HttpResponse<String> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EngineUnavailable("interrupted while asking " + subject);
        } catch (Exception e) {
            throw new EngineUnavailable(subject + " could not be reached: " + e);
        }
    }
}
