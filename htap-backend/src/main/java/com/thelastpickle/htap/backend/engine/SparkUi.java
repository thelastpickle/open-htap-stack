package com.thelastpickle.htap.backend.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.config.SparkSettings;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Seeing and stopping Spark work, over the application UI's REST interface.
 *
 * <p>The Thrift Server is one long-lived Spark application, so every job it runs is visible here
 * whether this backend submitted it or somebody ran {@code spark-sql} in the container. Over HTTP
 * rather than through the Thrift connection for the same reason the Presto listing is: a connection
 * busy with a query is exactly the connection that cannot be asked about it.
 */
@ApplicationScoped
public class SparkUi {

    /** The bound on one listing or one kill, matching {@link PrestoQueries#TIMEOUT} and for its reason. */
    static final Duration TIMEOUT = Duration.ofSeconds(3);

    /** A job description can be a whole statement; the page wants a line, not a plan. */
    static final int DESCRIPTION_LIMIT = 300;

    private final SparkSettings settings;
    private final Rest rest;
    private final Clock clock;

    @Inject
    SparkUi(SparkSettings settings, ObjectMapper json) {
        this(settings, json, Clock.systemUTC());
    }

    SparkUi(SparkSettings settings, ObjectMapper json, Clock clock) {
        this.settings = settings;
        this.rest = new Rest("the Spark UI", TIMEOUT, json);
        this.clock = clock;
    }

    /**
     * The Thrift Server's application, or empty when the UI has none.
     *
     * <p>The UI lists one application per JVM and the Thrift Server is the only one in that
     * container, so the first is the right one.
     */
    public Optional<String> applicationId() {
        JsonNode applications = rest.get(url("/api/v1/applications"));
        return applications.isEmpty()
                ? Optional.empty()
                : Optional.of(applications.get(0).path("id").asText(""));
    }

    /**
     * The jobs the application is running.
     *
     * <p>Jobs rather than SQL executions: the jobs endpoint filters server-side, while the SQL one
     * returns every execution since start-up with its whole query plan attached, which is megabytes
     * to answer a question about the present.
     */
    public List<SparkJob> runningJobs() {
        Optional<String> app = applicationId();
        if (app.isEmpty()) {
            return List.of();
        }
        JsonNode listed = rest.get(url("/api/v1/applications/" + app.get() + "/jobs?status=running"));
        Instant now = clock.instant();
        List<SparkJob> jobs = new ArrayList<>();
        for (JsonNode job : listed) {
            jobs.add(new SparkJob(
                    job.path("jobId").asText(""),
                    text(job, "status", "RUNNING").toLowerCase(Locale.ROOT),
                    label(description(job)),
                    Stamps.ageS(text(job, "submissionTime", ""), now),
                    job.path("numCompletedTasks").asInt(0),
                    job.path("numTasks").asInt(0)));
        }
        return jobs;
    }

    /**
     * Kill the running jobs working on any of these statements, and report which.
     *
     * <p>Matched by statement rather than killing whatever is running, because the Thrift Server is
     * one application shared by everything that connects to it: a {@code spark-sql} session in the
     * container would otherwise be collateral damage when a comparison is cancelled.
     *
     * <p>Killing is necessary and not merely tidy. Taking a client's connection away stops this
     * backend waiting, but Spark carries on: HiveServer2 does not notice a dropped session promptly,
     * so the job keeps its share of the cores and the next comparison would be timed against an
     * orphan.
     */
    public List<String> killJobsFor(Collection<String> statements) {
        Set<String> wanted = new HashSet<>();
        for (String statement : statements) {
            wanted.add(label(statement));
        }
        List<String> killed = new ArrayList<>();
        for (SparkJob job : runningJobs()) {
            if (wanted.contains(job.sql())) {
                killJob(job.id());
                killed.add(job.id());
            }
        }
        return killed;
    }

    /**
     * Ask the UI to kill one job, as its own kill link does.
     *
     * <p>Depends on {@code spark.ui.killEnabled}, which is Spark's default and is left at it. The
     * handler answers with a redirect to the jobs page, so any 2xx or 3xx means the request was
     * accepted; whether the job dies is then Spark's business, and the next listing is what confirms
     * it.
     */
    public void killJob(String jobId) {
        HttpRequest.Builder request = rest.request(url("/jobs/job/kill/?id=" + jobId));
        HttpResponse<String> response =
                rest.send(request.POST(HttpRequest.BodyPublishers.noBody()).build());
        if (response.statusCode() >= 400) {
            throw new EngineFailed("the Spark UI refused to kill job " + jobId + " (HTTP "
                    + response.statusCode() + "); spark.ui.killEnabled must be on");
        }
    }

    /** A statement as the job list reports it, which is how one is matched against the other. */
    static String label(String statement) {
        return Messages.oneLine(statement, DESCRIPTION_LIMIT);
    }

    /**
     * {@code description} is the statement the Thrift Server set for the job group; {@code name} is
     * the Spark call site, which is all there is for a job submitted any other way.
     */
    private static String description(JsonNode job) {
        String described = text(job, "description", "");
        return described.isBlank() ? text(job, "name", "a Spark job") : described;
    }

    /**
     * A field's text, with an absent, null or empty field taking the fallback.
     *
     * <p>All three cases arrive: the UI omits {@code description} for a job nobody labelled, and
     * {@code JsonNode.asText} would answer the four characters "null" for a field explicitly null.
     */
    private static String text(JsonNode node, String field, String fallback) {
        JsonNode value = node.path(field);
        return value.isMissingNode() || value.isNull() || value.asText("").isEmpty()
                ? fallback
                : value.asText("");
    }

    private URI url(String path) {
        return URI.create(
                "http://%s:%d%s".formatted(settings.uiHost(), settings.appUiPort(), path));
    }
}
