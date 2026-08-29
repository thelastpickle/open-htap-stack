package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.CdcSchemaView;
import com.thelastpickle.htap.backend.api.dto.CdcStreamResponse;
import com.thelastpickle.htap.backend.api.dto.CdcStreamStatus;
import com.thelastpickle.htap.backend.cdc.CdcContract;
import com.thelastpickle.htap.backend.cdc.CdcTail;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * A live tail of what the Sidecar publishes to Kafka.
 *
 * <p>Read from the topic and never from Cassandra: these mutations come out of the commit log, so the
 * page shows a change-data-capture pipeline rather than a query. Nothing here is an access path, and
 * the tail runs whether or not the page is open.
 */
@Path("/api/streaming")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "streaming")
public class StreamingResource {

    /**
     * The largest window one response may carry, which the buffer bounds further.
     *
     * <p>Independent of {@code cdc.buffer-size}, 200 by default: a page asking for 500 is given
     * whatever the buffer holds, so this is a ceiling on the response rather than a claim about the
     * tail.
     */
    static final int MAX_LIMIT = 500;

    private final CdcTail tail;
    private final CdcContract contract;

    StreamingResource(CdcTail tail, CdcContract contract) {
        this.tail = tail;
        this.contract = contract;
    }

    /**
     * The latest mutations, newest first, with what the tail is doing.
     *
     * @param since poll with it to receive only what is new, without it for the latest window
     *     whatever has been seen before
     */
    @GET
    @Path("/cdc")
    // The limit is clamped where FastAPI declared `Query(50, ge=1, le=500)` and answered 422, which
    // is the one divergence on this route: it matches the other query parameters in this backend,
    // and a page polling a live tail is better served the nearest window than an error.
    public CdcStreamResponse stream(
            @QueryParam("limit") @DefaultValue("50") int limit, @QueryParam("since") Long since) {
        return new CdcStreamResponse(
                tail.status(), tail.records(Math.clamp(limit, 1, MAX_LIMIT), since));
    }

    /** The tail alone, for a caller that wants the counters and not the records. */
    @GET
    @Path("/cdc/status")
    public CdcStreamStatus status() {
        return tail.status();
    }

    /** The Avro schema the topic's records are written against, from the registry that holds it. */
    @GET
    @Path("/cdc/schema")
    public CdcSchemaView schema() {
        return contract.published();
    }
}
