package com.thelastpickle.htap.backend.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.api.dto.BenchmarkRequest;
import com.thelastpickle.htap.backend.api.dto.BenchmarkResponse;
import com.thelastpickle.htap.backend.api.dto.EngineResult;
import com.thelastpickle.htap.backend.api.dto.EnginesResponse;
import com.thelastpickle.htap.backend.api.dto.NlQueryRequest;
import com.thelastpickle.htap.backend.api.dto.NlQueryResponse;
import com.thelastpickle.htap.backend.api.dto.SqlQueryRequest;
import com.thelastpickle.htap.backend.api.dto.SqlQueryResult;
import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.query.Comparison;
import com.thelastpickle.htap.backend.query.NaturalLanguage;
import com.thelastpickle.htap.backend.query.OltpImpact;
import com.thelastpickle.htap.backend.query.PathResult;
import com.thelastpickle.htap.backend.query.QueryPaths;
import com.thelastpickle.htap.backend.query.QueryRunner;
import com.thelastpickle.htap.backend.query.Run;
import com.thelastpickle.htap.backend.query.RunMode;
import com.thelastpickle.htap.backend.query.SingleRunGate;
import com.thelastpickle.htap.backend.query.Statements;
import com.thelastpickle.htap.backend.query.Translator;
import com.thelastpickle.htap.backend.query.WindowChoice;
import com.thelastpickle.htap.backend.query.Windows;
import io.smallrye.common.annotation.Blocking;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/** The console: one statement down one of the five access paths, and what may be asked of them. */
@Path("/api/query")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "query")
public class QueryResource {

    /** Newline-delimited JSON, which is what the stream route answers rather than server-sent events. */
    private static final String NDJSON = "application/x-ndjson";

    /**
     * How many rows a translated question may answer with.
     *
     * <p>Fixed rather than taken from the request: the natural-language page has no limit control, and
     * a question in words asks for the interesting rows rather than for all of them.
     */
    private static final int NL_LIMIT = 100;

    private final QueryPaths paths;
    private final QueryRunner runner;
    private final Windows windows;
    private final Comparison comparison;
    private final Translator translator;
    private final ObjectMapper json;

    QueryResource(
            QueryPaths paths,
            QueryRunner runner,
            Windows windows,
            Comparison comparison,
            Translator translator,
            ObjectMapper json) {
        this.paths = paths;
        this.runner = runner;
        this.windows = windows;
        this.comparison = comparison;
        this.translator = translator;
        this.json = json;
    }

    /**
     * Runs one {@code SELECT} on the chosen path.
     *
     * <p>Three outcomes, and the statuses tell them apart: 503 for a path that could not be
     * reached, 400 for a path that reached its engine and was refused, and 200 with rows. The
     * refusal is often the interesting answer here, {@code GROUP BY} on a non-key column being the
     * demo's own example, so the engine's own words are what the status carries.
     */
    @POST
    @Path("/sql")
    @Consumes(MediaType.APPLICATION_JSON)
    public SqlQueryResult sql(SqlQueryRequest request) {
        // A POST with no body reaches the route as null, as it does on the map routes: refused as
        // an empty statement rather than raising, so the page reads the same detail field either way.
        SqlQueryRequest asked =
                request == null ? new SqlQueryRequest(null, 0, null, false) : request;
        QueryPath path = paths.byName(asked.engine())
                .orElseThrow(() -> new ApiException(400, "Unknown engine: " + asked.engine()));
        PathResult result =
                runner.run(path, validated(asked.sql()), asked.limit(), asked.reuseSnapshot());
        if (!result.available()) {
            throw new ApiException(503, result.error() == null ? "Engine unavailable" : result.error());
        }
        if (result.error() != null) {
            throw new ApiException(400, result.error());
        }
        return new SqlQueryResult(
                result.columns(), result.rows(), result.rowCount(), result.queryTimeMs(), result.sql());
    }

    /**
     * Runs one question down several paths and reports what each cost.
     *
     * <p>The whole body at once, which is what the parallel mode needs: paths that overlap have one
     * probe over the whole window rather than one each, so there is nothing to report as each
     * finishes. A sequential run of the slower questions is minutes, and the stream route below is
     * for that.
     *
     * <p>Two refusals have statuses of their own. 409 for a second run arriving while one is going,
     * and 400 for a path this backend does not have; a path that is merely unreachable is a column
     * in the body, because a comparison that renders four paths and says why the fifth declined is
     * the answer.
     */
    @POST
    @Path("/benchmark")
    @Consumes(MediaType.APPLICATION_JSON)
    public BenchmarkResponse benchmark(BenchmarkRequest request) {
        BenchmarkRequest asked = request == null ? BenchmarkRequest.empty() : request;
        Run run = begun(asked, false);
        try {
            OltpImpact baseline = comparison.baseline(run).orElse(null);
            if (run.asked().mode() == RunMode.PARALLEL) {
                return BenchmarkResponse.of(run, baseline, comparison.together(run).orElse(null));
            }
            comparison.each(run, (engine, result) -> {});
            return BenchmarkResponse.of(run, baseline, null);
        } finally {
            comparison.end(run);
        }
    }

    /**
     * The same comparison, reported as each path answers.
     *
     * <p>Newline-delimited JSON: a {@code start} line, a {@code baseline} line once the reference read
     * has been sampled, one {@code engine} line per path as it lands, and a {@code done} line. The
     * caller's order is the run order, and the dashboard sends its quickest path first, so the first
     * answer arrives in milliseconds and the minutes-long ones fill in behind it.
     *
     * <p>The gate is taken before the body starts, so a second run is refused with a status. Once the
     * first line has gone out, a failure could only be reported in the body.
     */
    @POST
    @Path("/benchmark/stream")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(NDJSON)
    @Blocking
    public Response stream(BenchmarkRequest request) {
        BenchmarkRequest asked = request == null ? BenchmarkRequest.empty() : request;
        Run run = begun(asked, true);
        return Response.ok(lines(run), NDJSON)
                // nginx buffers a proxied response by default, which would hold every line until the
                // run finished and defeat the point of streaming. This header turns that off for one
                // response, so the dashboard's own nginx needs no rule of its own.
                .header("X-Accel-Buffering", "no")
                .header("Cache-Control", "no-store")
                .build();
    }

    /**
     * Answers a question in words, and says what it was translated into.
     *
     * <p>A model translates it where one is configured and the rules translate it otherwise, and
     * either way the statement is reported. A refusal is a 200 carrying the statement and the reason:
     * the translation is itself the answer a viewer came for, and a status would throw it away.
     */
    @POST
    @Path("/nl")
    @Consumes(MediaType.APPLICATION_JSON)
    public NlQueryResponse nl(NlQueryRequest request) {
        String prompt = request == null || request.prompt() == null ? "" : request.prompt().strip();
        if (prompt.isEmpty()) {
            throw new ApiException(400, "Empty prompt");
        }
        String sql = translator.toSql(prompt).orElseGet(() -> NaturalLanguage.toSql(prompt));
        String hint = NaturalLanguage.renderHint(prompt);
        String statement;
        try {
            statement = Statements.validate(sql);
        } catch (Statements.Refused e) {
            return NlQueryResponse.refused(sql, e.getMessage(), hint);
        }
        QueryPath path = paths.byName(NaturalLanguage.ENGINE).orElseThrow();
        PathResult result = runner.run(path, statement, NL_LIMIT, false);
        String issued = result.sql() == null ? sql : result.sql();
        if (result.error() != null) {
            return NlQueryResponse.refused(issued, result.error(), hint);
        }
        return new NlQueryResponse(
                issued,
                NaturalLanguage.ENGINE,
                new SqlQueryResult(
                        result.columns(),
                        result.rows(),
                        result.rowCount(),
                        result.queryTimeMs() == null ? 0.0 : result.queryTimeMs(),
                        result.sql()),
                null,
                hint);
    }

    @GET
    @Path("/engines")
    public EnginesResponse engines() {
        return new EnginesResponse(paths.status());
    }

    /**
     * Which window to compare over, and what may be claimed of it.
     *
     * <p>The choice is served as the query layer computes it, with no conversion: every field is
     * already the answer, and a second record here would only restate it.
     */
    @GET
    @Path("/window")
    public WindowChoice window() {
        return windows.choose();
    }

    private static String validated(String sql) {
        try {
            return Statements.validate(sql);
        } catch (Statements.Refused e) {
            throw new ApiException(400, e.getMessage());
        }
    }

    /**
     * Validate the request and take the one-at-a-time gate, or refuse with a status.
     *
     * <p>The two refusals are here rather than in mappers of their own, because this is the only route
     * pair that can raise either and the status is the interesting half: a 409 is what the compare
     * page tells a viewer to go and look at the Health page about.
     */
    private Run begun(BenchmarkRequest asked, boolean streamed) {
        String sql = validated(asked.sql());
        try {
            return streamed
                    ? comparison.beginStreamed(
                            sql, asked.engines(), asked.limit(), asked.reuseSnapshot())
                    : comparison.begin(
                            sql, asked.engines(), asked.mode(), asked.limit(), asked.reuseSnapshot());
        } catch (QueryPaths.Unknown e) {
            throw new ApiException(400, e.getMessage());
        } catch (SingleRunGate.Busy e) {
            throw new ApiException(409, e.getMessage());
        }
    }

    /**
     * The run's four kinds of line, written and flushed as each is known.
     *
     * <p>The gate is released in a {@code finally} that a client which has gone away also reaches, so
     * a browser closing a tab does not leave the next comparison refused until this process restarts.
     */
    private StreamingOutput lines(Run run) {
        return output -> {
            try {
                Map<String, Object> start = new LinkedHashMap<>();
                start.put("event", "start");
                start.put("engines", run.engines());
                start.put("sql", run.asked().sql());
                write(output, start);

                OltpImpact baseline = comparison.baseline(run).orElse(null);
                Map<String, Object> sampled = new LinkedHashMap<>();
                sampled.put("event", "baseline");
                sampled.put("oltp_baseline", baseline);
                write(output, sampled);

                comparison.each(run, (engine, result) -> {
                    Map<String, Object> answered = new LinkedHashMap<>();
                    answered.put("event", "engine");
                    answered.put("engine", engine);
                    answered.put("result", EngineResult.of(result, run.impacts().get(engine)));
                    write(output, answered);
                });

                Map<String, Object> done = new LinkedHashMap<>();
                done.put("event", "done");
                done.put("cancelled", run.cancelled());
                write(output, done);
            } finally {
                comparison.end(run);
            }
        };
    }

    /**
     * One line, flushed.
     *
     * <p>The flush is what makes this a stream: without it the container decides when to send, and a
     * viewer would get every line at the end, which is what the whole-body route above already does.
     *
     * <p>A write that fails is a client that has gone, and it is raised so the run stops rather than
     * carrying on writing lines nobody reads; the {@code finally} above still releases the gate.
     */
    private void write(OutputStream output, Map<String, Object> line) {
        try {
            output.write(json.writeValueAsString(line).getBytes(StandardCharsets.UTF_8));
            output.write('\n');
            output.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
