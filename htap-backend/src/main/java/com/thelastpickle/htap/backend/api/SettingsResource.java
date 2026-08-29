package com.thelastpickle.htap.backend.api;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.thelastpickle.htap.backend.api.dto.CleanupResult;
import com.thelastpickle.htap.backend.api.dto.DemoSettings;
import com.thelastpickle.htap.backend.api.dto.DemoSettingsResponse;
import com.thelastpickle.htap.backend.demo.DemoControls;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * The Settings page's controls, every one of which changes the running stack.
 *
 * <p>The producer polls {@link #demoSettings} and adopts what it finds, so nothing on this page is
 * decorative: the fleet size, the rate and the pause each take effect within one poll interval.
 */
@Path("/api/settings")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "settings")
public class SettingsResource {

    private final DemoControls controls;
    private final CassandraPath cassandra;

    SettingsResource(DemoControls controls, CassandraPath cassandra) {
        this.controls = controls;
        this.cassandra = cassandra;
    }

    /** The settings currently in force. Polled by the data producer. */
    @GET
    @Path("/demo")
    public DemoSettingsResponse demoSettings() {
        return DemoSettingsResponse.of(controls.current());
    }

    /** What the environment declared, which is what a restart would return to. */
    @GET
    @Path("/demo/defaults")
    public DemoSettingsResponse demoDefaults() {
        return DemoSettingsResponse.of(controls.startupState(), "Startup defaults");
    }

    @POST
    @Path("/demo")
    @Consumes(MediaType.APPLICATION_JSON)
    public DemoSettingsResponse updateDemoSettings(DemoSettings asked) {
        // A body-less POST arrives as null here, where pydantic refused the request before the
        // Python's route ran.
        if (asked == null) {
            throw new ApiException(422, "Expected a body carrying the four demo settings");
        }
        asked.outOfRange().ifPresent(reason -> {
            throw new ApiException(422, reason);
        });
        return controls.update(asked);
    }

    /** Stop or resume event generation. */
    @POST
    @Path("/demo/pause")
    public DemoSettingsResponse togglePause() {
        return controls.togglePause();
    }

    /**
     * Truncate {@code drone_latest_status}, so the KPIs settle on the fleet size now in force.
     *
     * <p>Reducing the fleet leaves the rows of retired assets behind, since nothing overwrites them
     * and the table is one row per asset; the KPIs then report a fleet larger than the one flying.
     * Event history and the zones are untouched, so the map redraws as fresh telemetry arrives.
     */
    @POST
    @Path("/demo/cleanup")
    public CleanupResult clearFleetState() {
        if (!cassandra.connected()) {
            return CleanupResult.failed("Cassandra not connected");
        }
        try {
            cassandra.execute(SimpleStatement.newInstance("TRUNCATE drone_latest_status"));
            return new CleanupResult(
                    true, "Fleet state cleared; KPIs rebuild as telemetry arrives");
        } catch (RuntimeException e) {
            // The driver's own words, because a truncate refused for a reason of its own is
            // something an operator acts on: this is the one write the dashboard makes.
            return CleanupResult.failed(Messages.oneLine(e));
        }
    }
}
