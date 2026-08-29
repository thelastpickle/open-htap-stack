package com.thelastpickle.htap.backend.api;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * A test-only route shaped like the real ones: no annotation about threading, and a plain value
 * returned. It exists so {@link DispatchThreadTest} can read the thread the framework chose.
 */
@Path("/test/dispatch")
public class DispatchProbeResource {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String threadName() {
        return Thread.currentThread().getName();
    }
}
