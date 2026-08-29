package com.thelastpickle.htap.backend.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.config.PrestoSettings;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Seeing and stopping Presto work, over the coordinator's own REST interface.
 *
 * <p>Not over the JDBC connection, and not as SQL against {@code system.runtime}: that connection is
 * serialised, so the one query worth asking about is the query holding the lock that would answer.
 * REST needs no connection, and a listing that is not itself a query does not appear in its own
 * results.
 */
@ApplicationScoped
public class PrestoQueries {

    /**
     * The bound on one listing or one cancel.
     *
     * <p>Short, because the Health page polls the listing: a coordinator that is slow to answer should
     * leave the page saying so rather than holding the whole response up.
     */
    static final Duration TIMEOUT = Duration.ofSeconds(3);

    /** A statement as the page shows it: a line, not a plan. */
    static final int SQL_LIMIT = 300;

    /** The three states a query is in once the coordinator has finished with it. */
    private static final Set<String> SETTLED = Set.of("FINISHED", "FAILED", "CANCELED");

    private final PrestoSettings settings;
    private final Rest rest;
    private final Clock clock;

    @Inject
    PrestoQueries(PrestoSettings settings, ObjectMapper json) {
        this(settings, json, Clock.systemUTC());
    }

    PrestoQueries(PrestoSettings settings, ObjectMapper json, Clock clock) {
        this.settings = settings;
        this.rest = new Rest("the Presto coordinator", TIMEOUT, json);
        this.clock = clock;
    }

    /**
     * Every query the coordinator has not finished with, longest-running first.
     *
     * <p>Fetched unfiltered and filtered here. The coordinator selects by state, but only one state
     * per request, and there are several a query can be in without having finished, including
     * {@code PLANNING}, which is exactly where a query worth noticing gets stuck. So this is one
     * request for the coordinator's recent history rather than several or a partial answer.
     */
    public List<RunningQuery> running() {
        JsonNode listed = rest.get(url("/v1/query"));
        Instant now = clock.instant();
        List<RunningQuery> running = new ArrayList<>();
        for (JsonNode query : listed) {
            String state = query.path("state").asText("");
            if (SETTLED.contains(state)) {
                continue;
            }
            JsonNode session = query.path("session");
            running.add(new RunningQuery(
                    query.path("queryId").asText(""),
                    state.toLowerCase(Locale.ROOT),
                    oneLine(query.path("query").asText("")),
                    Stamps.ageS(query.path("queryStats").path("createTime").asText(null), now),
                    session.path("user").asText(""),
                    session.path("source").asText("")));
        }
        return running.stream()
                .sorted(Comparator.comparingDouble(RunningQuery::runningS).reversed())
                .toList();
    }

    /**
     * Ask the coordinator to cancel one query.
     *
     * <p>A {@code DELETE} is the coordinator's own cancel, so it needs no session and works while this
     * backend's connection is busy with the very query being killed.
     */
    public void kill(String queryId) {
        HttpResponse<String> response =
                rest.send(rest.request(url("/v1/query/" + queryId)).DELETE().build());
        if (response.statusCode() >= 400) {
            throw new EngineFailed(
                    "Presto refused to cancel " + queryId + " (HTTP " + response.statusCode() + ")");
        }
    }

    static String oneLine(String sql) {
        return Messages.oneLine(sql, SQL_LIMIT);
    }

    private URI url(String path) {
        return URI.create("http://%s:%d%s".formatted(settings.host(), settings.port(), path));
    }
}
