package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.Liveness;
import com.thelastpickle.htap.common.Timestamps;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.time.Instant;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/health")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "platform")
public class LivenessResource {

    /** Is the API up? Engine reachability lives at {@code /api/platform/health}. */
    @GET
    public Liveness liveness() {
        return new Liveness("ok", Timestamps.isoOffset(Instant.now()));
    }
}
