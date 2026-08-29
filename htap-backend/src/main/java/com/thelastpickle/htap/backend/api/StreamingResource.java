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

    /** The largest window a page may ask for, which is also the largest the buffer can hold. */
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
