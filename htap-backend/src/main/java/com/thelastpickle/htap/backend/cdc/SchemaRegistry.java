package com.thelastpickle.htap.backend.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.config.CdcSettings;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;

/**
 * The registry the publisher writes its schemas to, asked two ways.
 *
 * <p>By id, for a record that names one, and by subject, for the page that shows the contract. A
 * schema id is immutable in the registry, so one held by id needs no expiry: the publisher registers
 * a new id when the table's columns change.
 */
@ApplicationScoped
public class SchemaRegistry {

    /** Long enough for a registry that has just started, short enough not to stall the tail. */
    static final Duration TIMEOUT = Duration.ofSeconds(10);

    /** One {@code GET}, so what the registry said can be scripted without a socket. */
    interface Http {
        Reply get(URI url) throws IOException, InterruptedException;
    }

    record Reply(int status, String body) {}

    private final CdcSettings settings;
    private final ObjectMapper json;
    private final Http http;

    /** Schema id to the parsed schema, filled once per id and never invalidated. */
    private final Map<Integer, Schema> byId = new ConcurrentHashMap<>();

    @Inject
    SchemaRegistry(CdcSettings settings, ObjectMapper json) {
        this(settings, json, overHttp());
    }

    SchemaRegistry(CdcSettings settings, ObjectMapper json, Http http) {
        this.settings = settings;
        this.json = json;
        this.http = http;
    }

    /** The schema a record names, fetched once per id. */
    Schema schema(int schemaId) throws IOException, InterruptedException {
        Schema held = byId.get(schemaId);
        if (held != null) {
            return held;
        }
        JsonNode body = answered(URI.create(settings.registry() + "/schemas/ids/" + schemaId));
        Schema parsed = new Schema.Parser().parse(body.path("schema").asText());
        byId.put(schemaId, parsed);
        return parsed;
    }

    /** The ids the tail has looked up, which is how many schemas the topic has carried. */
    List<Integer> ids() {
        return byId.keySet().stream().sorted().toList();
    }

    /** The latest version of a subject, or null where the registry answers 404. */
    JsonNode latest(String subject) throws IOException, InterruptedException {
        URI url = URI.create(settings.registry() + "/subjects/" + subject + "/versions/latest");
        Reply reply = http.get(url);
        // 404 is an ordinary state on a stack that is minutes old: the subject appears with the
        // publisher's first mutation, so it is answered as "not registered yet" rather than as a
        // registry that failed.
        return reply.status() == 404 ? null : parsed(reply, url);
    }

    private JsonNode answered(URI url) throws IOException, InterruptedException {
        return parsed(http.get(url), url);
    }

    private JsonNode parsed(Reply reply, URI url) throws IOException {
        if (reply.status() / 100 != 2) {
            throw new IOException(
                    "the registry answered HTTP " + reply.status() + " for " + url.getPath());
        }
        return json.readTree(reply.body());
    }

    private static Http overHttp() {
        HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
        return url -> {
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder(url).timeout(TIMEOUT).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return new Reply(response.statusCode(), response.body());
        };
    }
}
