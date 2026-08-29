package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.EnginesResponse;
import com.thelastpickle.htap.backend.api.dto.SqlQueryRequest;
import com.thelastpickle.htap.backend.api.dto.SqlQueryResult;
import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.query.PathResult;
import com.thelastpickle.htap.backend.query.QueryPaths;
import com.thelastpickle.htap.backend.query.QueryRunner;
import com.thelastpickle.htap.backend.query.Statements;
import com.thelastpickle.htap.backend.query.WindowChoice;
import com.thelastpickle.htap.backend.query.Windows;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/** The console: one statement down one of the five access paths, and what may be asked of them. */
@Path("/api/query")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "query")
public class QueryResource {

    private final QueryPaths paths;
    private final QueryRunner runner;
    private final Windows windows;

    QueryResource(QueryPaths paths, QueryRunner runner, Windows windows) {
        this.paths = paths;
        this.runner = runner;
        this.windows = windows;
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
}
