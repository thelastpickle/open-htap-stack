package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.PlatformHealthResponse;
import com.thelastpickle.htap.backend.api.dto.ServiceHealth;
import com.thelastpickle.htap.backend.config.ContainerSettings;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.read.CassandraReads;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * Platform health: what this backend can reach.
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

    PlatformResource(
            PlatformProbe probe,
            CassandraPath cassandra,
            CassandraReads reads,
            ContainerSettings container) {
        this.probe = probe;
        this.cassandra = cassandra;
        this.reads = reads;
        this.container = container;
    }

    @GET
    @Path("/health")
    public PlatformHealthResponse health() {
        List<ServiceHealth> services = probe.services();
        double score = probe.remember(probe.score(services));
        return new PlatformHealthResponse(services, score, droneCount(), container.cli());
    }

    private long droneCount() {
        if (!cassandra.connected()) {
            return 0L;
        }
        return Answers.orElse("/api/platform/health drone count", reads::droneCount, () -> 0L);
    }
}
