package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.AlertRecord;
import com.thelastpickle.htap.backend.api.dto.AlertsResponse;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.read.CassandraReads;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/alerts")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "alerts")
public class AlertsResource {

    private static final int MAX_LIMIT = 200;
    private static final int WINDOW_HOURS = 6;

    private final CassandraPath cassandra;
    private final CassandraReads reads;

    AlertsResource(CassandraPath cassandra, CassandraReads reads) {
        this.cassandra = cassandra;
        this.reads = reads;
    }

    /**
     * Recent alerts, newest first.
     *
     * <p>{@code severity} filters after the read, so {@code total_count} always reports how
     * many alerts were found in the window and the page's per-severity counts stay consistent
     * with each other.
     *
     * <p>{@code limit} out of range is clamped rather than refused, where FastAPI's
     * {@code Query(ge=1, le=200)} answered 422. The page never sends one, and clamping is the
     * kinder answer to a URL typed by hand.
     */
    @GET
    public AlertsResponse alerts(
            @QueryParam("severity") String severity,
            @QueryParam("limit") @DefaultValue("50") int limit) {
        if (!cassandra.connected()) {
            return AlertsResponse.empty();
        }
        return Answers.orElse(
                "/api/alerts",
                () -> {
                    List<AlertRecord> alerts = reads.alerts(
                                    Math.clamp(limit, 1, MAX_LIMIT), WINDOW_HOURS)
                            .stream()
                            .map(Dtos::alert)
                            .toList();
                    List<AlertRecord> matching = severity == null || severity.isEmpty()
                            ? alerts
                            : alerts.stream()
                                    .filter(alert -> severity.equals(alert.severity()))
                                    .toList();
                    return new AlertsResponse(matching, alerts.size());
                },
                AlertsResponse::empty);
    }
}
