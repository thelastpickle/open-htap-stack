package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.AlertRecord;
import com.thelastpickle.htap.backend.api.dto.AlertSummary;
import com.thelastpickle.htap.backend.api.dto.IngestionBucket;
import com.thelastpickle.htap.backend.api.dto.IngestionHistory;
import com.thelastpickle.htap.backend.api.dto.OverviewKpis;
import com.thelastpickle.htap.backend.api.dto.ResyncResult;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.read.BucketCount;
import com.thelastpickle.htap.backend.read.CassandraReads;
import com.thelastpickle.htap.backend.read.Kpis;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/** Overview dashboard: fleet key performance indicators (KPIs) and ingestion volume. */
@Path("/api/overview")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "overview")
public class OverviewResource {

    static final int MAX_HISTORY_HOURS = 48;
    static final String CSV_HEADER = "time,timestamp,count";

    /** Alerts beside the KPIs: enough for a panel, and one bounded read. */
    private static final int LATEST_ALERT_LIMIT = 5;

    private static final int ALERT_WINDOW_HOURS = 6;

    private final CassandraPath cassandra;
    private final CassandraReads reads;
    private final PlatformProbe probe;

    OverviewResource(CassandraPath cassandra, CassandraReads reads, PlatformProbe probe) {
        this.cassandra = cassandra;
        this.reads = reads;
        this.probe = probe;
    }

    @GET
    @Path("/kpis")
    public OverviewKpis kpis() {
        return OverviewKpis.of(fleetKpis(), probe.score(), latestAlerts());
    }

    /** Ingestion volume in 30-minute buckets over the last N hours. */
    @GET
    @Path("/ingestion-history")
    public IngestionHistory ingestionHistory(@QueryParam("hours") @DefaultValue("8") int hours) {
        int window = Math.clamp(hours, 1, MAX_HISTORY_HOURS);
        if (!cassandra.connected()) {
            return new IngestionHistory(window, List.of());
        }
        return Answers.orElse(
                "/api/overview/ingestion-history",
                () -> new IngestionHistory(window, buckets(window)),
                () -> new IngestionHistory(window, List.of()));
    }

    /** The same series as a CSV download. */
    @GET
    @Path("/ingestion-history/csv")
    @Produces("text/csv")
    public Response ingestionHistoryCsv(@QueryParam("hours") @DefaultValue("8") int hours) {
        int window = Math.clamp(hours, 1, MAX_HISTORY_HOURS);
        List<IngestionBucket> series = cassandra.connected()
                ? Answers.orElse(
                        "/api/overview/ingestion-history/csv", () -> buckets(window), List::of)
                : List.of();
        StringBuilder csv = new StringBuilder(CSV_HEADER);
        for (IngestionBucket bucket : series) {
            csv.append('\n')
                    .append(bucket.time())
                    .append(',')
                    .append(bucket.timestamp())
                    .append(',')
                    .append(bucket.count());
        }
        return Response.ok(csv.append('\n').toString())
                .header(
                        "Content-Disposition",
                        "attachment; filename=\"ingestion_log_" + window + "h.csv\"")
                .build();
    }

    /** Re-probe Cassandra and answer with fresh KPIs. */
    @POST
    @Path("/resync")
    public ResyncResult resync() {
        try {
            // force only when it is not already connected: a forced reconnect of a healthy
            // session would drop the pool this page is about to read through.
            cassandra.connect(!cassandra.connected());
            return new ResyncResult(true, "Re-sync complete", fleetKpis());
        } catch (RuntimeException e) {
            return ResyncResult.failed(String.valueOf(e.getMessage()));
        }
    }

    /** Every fleet KPI, or zeros when Cassandra cannot be reached. */
    private Kpis fleetKpis() {
        if (!cassandra.connected()) {
            cassandra.connect();
        }
        if (!cassandra.connected()) {
            return Kpis.zero();
        }
        return Answers.orElse("/api/overview/kpis", reads::kpis, Kpis::zero);
    }

    private List<AlertSummary> latestAlerts() {
        if (!cassandra.connected()) {
            return List.of();
        }
        return Answers.orElse(
                "/api/overview/kpis latest alerts",
                () -> reads.alerts(LATEST_ALERT_LIMIT, ALERT_WINDOW_HOURS).stream()
                        .map(Dtos::alert)
                        .map(AlertRecord::summary)
                        .toList(),
                List::of);
    }

    private List<IngestionBucket> buckets(int hours) {
        List<IngestionBucket> buckets = new ArrayList<>();
        for (BucketCount count : reads.ingestionHistory(hours)) {
            buckets.add(Dtos.bucket(count));
        }
        return buckets;
    }
}
