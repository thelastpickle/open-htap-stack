package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.BreachInjected;
import com.thelastpickle.htap.backend.api.dto.LatencyReport;
import com.thelastpickle.htap.backend.demo.BreachScenario;
import com.thelastpickle.htap.backend.demo.LatencyProbes;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.read.CassandraReads;
import com.thelastpickle.htap.backend.read.FleetRow;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/** The scripted breach a presenter triggers, and the per-tier latency figures beside it. */
@Path("/api/demo")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "demo")
public class DemoResource {

    /** Enough to pick from without the choosing scan growing with the fleet. */
    static final int CANDIDATES = 50;

    private final CassandraPath cassandra;
    private final CassandraReads reads;
    private final BreachScenario scenario;
    private final LatencyProbes probes;

    DemoResource(
            CassandraPath cassandra,
            CassandraReads reads,
            BreachScenario scenario,
            LatencyProbes probes) {
        this.cassandra = cassandra;
        this.reads = reads;
        this.scenario = scenario;
        this.probes = probes;
    }

    /** Flag a real asset as breaching, and write the alert that matches it. */
    @POST
    @Path("/trigger-breach-scenario")
    public BreachInjected triggerBreachScenario() {
        if (!cassandra.connected()) {
            cassandra.connect();
        }
        if (!cassandra.connected()) {
            throw new ApiException(503, "Cassandra unavailable");
        }

        List<FleetRow> candidates;
        try {
            candidates = reads.drones(CANDIDATES, true);
        } catch (RuntimeException e) {
            throw new ApiException(500, "Could not read the fleet: " + Messages.oneLine(e));
        }
        if (candidates.isEmpty()) {
            // The refusal is the honest answer: the scenario flags a real asset, and on a stack
            // whose producer has not warmed up there is none to flag.
            throw new ApiException(
                    409, "No flying assets yet — wait for the producer to warm up, then retry");
        }

        try {
            return scenario.inject(candidates);
        } catch (RuntimeException e) {
            throw new ApiException(500, "Could not inject the scenario: " + Messages.oneLine(e));
        }
    }

    /** One timed query per tier, as this backend observed it. */
    @GET
    @Path("/latency")
    public LatencyReport latency() {
        return probes.measure();
    }
}
