package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.KillQueryRequest;
import com.thelastpickle.htap.backend.api.dto.OperationResult;
import com.thelastpickle.htap.backend.api.dto.PlatformHealthResponse;
import com.thelastpickle.htap.backend.api.dto.ReconnectRequest;
import com.thelastpickle.htap.backend.api.dto.RunningWork;
import com.thelastpickle.htap.backend.api.dto.ServiceHealth;
import com.thelastpickle.htap.backend.config.ContainerSettings;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.engine.PrestoQueries;
import com.thelastpickle.htap.backend.engine.SparkUi;
import com.thelastpickle.htap.backend.query.Cancellation;
import com.thelastpickle.htap.backend.query.Reconnection;
import com.thelastpickle.htap.backend.read.CassandraReads;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * Platform health, the work in flight, and the controls that stop it.
 *
 * <p>Restarting a service is deliberately absent. The dashboard is a container beside the
 * others with no control over them, which is the right way round for something reachable from
 * a browser; the page shows the host command instead.
 */
@Path("/api/platform")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "platform")
public class PlatformResource {

    private final PlatformProbe probe;
    private final CassandraPath cassandra;
    private final CassandraReads reads;
    private final ContainerSettings container;
    private final WorkInFlight work;
    private final Cancellation cancellation;
    private final Reconnection reconnection;
    private final PrestoQueries presto;
    private final SparkUi sparkUi;

    PlatformResource(
            PlatformProbe probe,
            CassandraPath cassandra,
            CassandraReads reads,
            ContainerSettings container,
            WorkInFlight work,
            Cancellation cancellation,
            Reconnection reconnection,
            PrestoQueries presto,
            SparkUi sparkUi) {
        this.probe = probe;
        this.cassandra = cassandra;
        this.reads = reads;
        this.container = container;
        this.work = work;
        this.cancellation = cancellation;
        this.reconnection = reconnection;
        this.presto = presto;
        this.sparkUi = sparkUi;
    }

    @GET
    @Path("/health")
    public PlatformHealthResponse health() {
        List<ServiceHealth> services = probe.services();
        double score = probe.remember(probe.score(services));
        return new PlatformHealthResponse(services, score, droneCount(), container.cli());
    }

    @GET
    @Path("/running")
    public RunningWork running() {
        return work.now();
    }

    /**
     * Stop the comparison in flight, so the next one can run.
     *
     * <p>Nothing running is a refusal rather than a success: a control that reports having stopped
     * nothing reads as though it had worked.
     */
    @POST
    @Path("/running/cancel-comparison")
    public OperationResult cancelComparison() {
        List<String> actions = cancellation.cancel();
        return actions.isEmpty()
                ? OperationResult.nothing("no comparison was running")
                : OperationResult.done(actions);
    }

    /**
     * Cancel one query, named by the handle its own engine gave it.
     *
     * <p>An engine that refuses the kill is a 502: this route asked something of another service and
     * that service said no, which is different from the request having been wrong.
     */
    @POST
    @Path("/running/kill")
    @Consumes(MediaType.APPLICATION_JSON)
    public OperationResult kill(KillQueryRequest request) {
        String engine = request == null ? null : request.engine();
        String id = request == null || request.id() == null ? "" : request.id().strip();
        // Tested for null first: ENGINES is an immutable list, whose contains(null) raises rather
        // than answering false, which would make a request naming no engine a 500.
        if (engine == null || !KillQueryRequest.ENGINES.contains(engine)) {
            throw new ApiException(400, "Only " + String.join(" and ", KillQueryRequest.ENGINES)
                    + " hand out a query handle to kill by; engine was " + engine);
        }
        if (id.isEmpty()) {
            throw new ApiException(400, "Name the query to kill");
        }
        try {
            if ("presto".equals(engine)) {
                presto.kill(id);
                return OperationResult.done(List.of("asked Presto to cancel " + id));
            }
            sparkUi.killJob(id);
            return OperationResult.done(List.of("asked Spark to kill job " + id));
        } catch (RuntimeException e) {
            throw new ApiException(502, Messages.oneLine(e));
        }
    }

    @POST
    @Path("/reconnect")
    @Consumes(MediaType.APPLICATION_JSON)
    public OperationResult reconnect(ReconnectRequest request) {
        String target = request == null ? null : request.target();
        List<String> targets = reconnection.targets();
        if (ReconnectRequest.ALL.equals(target)) {
            return result(reconnection.reconnect(targets));
        }
        if (!targets.contains(target)) {
            throw new ApiException(400, "Unknown reconnect target: " + target);
        }
        return result(reconnection.reconnect(List.of(target)));
    }

    private static OperationResult result(Reconnection.Outcome outcome) {
        return new OperationResult(outcome.ok(), outcome.actions());
    }

    private long droneCount() {
        if (!cassandra.connected()) {
            return 0L;
        }
        return Answers.orElse("/api/platform/health drone count", reads::droneCount, () -> 0L);
    }
}
