package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.IndexingStarted;
import com.thelastpickle.htap.backend.api.dto.LiveEmbeddingRequest;
import com.thelastpickle.htap.backend.api.dto.LiveEmbeddingStatus;
import com.thelastpickle.htap.backend.api.dto.VectorHit;
import com.thelastpickle.htap.backend.api.dto.VectorSearchRequest;
import com.thelastpickle.htap.backend.api.dto.VectorSearchResponse;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.read.CassandraReads;
import com.thelastpickle.htap.backend.read.FleetRow;
import com.thelastpickle.htap.backend.support.Messages;
import com.thelastpickle.htap.backend.support.Round;
import com.thelastpickle.htap.backend.vector.Embedder;
import com.thelastpickle.htap.backend.vector.EmbeddingFailed;
import com.thelastpickle.htap.backend.vector.Embeddings;
import com.thelastpickle.htap.backend.vector.Indexer;
import com.thelastpickle.htap.backend.vector.LiveEmbedder;
import com.thelastpickle.htap.backend.vector.Neighbour;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * Vector search over the embedding index, and the loop that keeps it current.
 *
 * <p>One search asks the analytical index for its nearest rows and then point-reads each one for
 * where the asset is now, so a single response is both paths answering about the same rows.
 */
@Path("/api/vector")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "vector")
public class VectorResource {

    private final CassandraPath cassandra;
    private final CassandraReads reads;
    private final Embeddings embeddings;
    private final Embedder embedder;
    private final Indexer indexer;
    private final LiveEmbedder live;

    VectorResource(
            CassandraPath cassandra,
            CassandraReads reads,
            Embeddings embeddings,
            Embedder embedder,
            Indexer indexer,
            LiveEmbedder live) {
        this.cassandra = cassandra;
        this.reads = reads;
        this.embeddings = embeddings;
        this.embedder = embedder;
        this.indexer = indexer;
        this.live = live;
    }

    @POST
    @Path("/search")
    @Consumes(MediaType.APPLICATION_JSON)
    public VectorSearchResponse search(VectorSearchRequest asked) {
        if (asked == null) {
            throw new ApiException(422, "Expected a body carrying the query to search for");
        }
        asked.outOfRange().ifPresent(reason -> {
            throw new ApiException(422, reason);
        });
        requireCassandra();

        float[] query = embed(asked.query());
        long started = System.nanoTime();
        List<VectorHit> hits;
        try {
            hits = withCurrentPositions(embeddings.nearest(query, asked.hits()));
        } catch (RuntimeException e) {
            // Nothing indexed reads as a failed search here, and the message says so: the index
            // exists from the schema onwards, so an empty one is the usual reason for a refusal.
            throw new ApiException(503, "Vector search failed: " + Messages.oneLine(e)
                    + ". Build the embeddings first — nothing is indexed until then.");
        }
        return new VectorSearchResponse(hits, Round.tenth((System.nanoTime() - started) / 1e6));
    }

    /** Embeds every asset's snippet. Answers at once and runs on. */
    @POST
    @Path("/index-all")
    public IndexingStarted indexAll() {
        requireCassandra();
        Thread.ofVirtual().name("vector-index-all").start(indexer::indexAll);
        return IndexingStarted.of(indexer.embedderKind());
    }

    /** What the live embedder is doing. Polled by the Explore page. */
    @GET
    @Path("/live")
    public LiveEmbeddingStatus liveStatus() {
        return live.status();
    }

    /** Turns live embedding on or off. Takes effect within one interval. */
    @POST
    @Path("/live")
    @Consumes(MediaType.APPLICATION_JSON)
    public LiveEmbeddingStatus setLive(LiveEmbeddingRequest asked) {
        if (asked == null || asked.enabled() == null) {
            throw new ApiException(422, "Expected a body saying whether to enable live embedding");
        }
        return live.enable(asked.enabled());
    }

    private float[] embed(String text) {
        try {
            return embedder.embed(text);
        } catch (EmbeddingFailed e) {
            throw new ApiException(500, e.getMessage());
        }
    }

    private List<VectorHit> withCurrentPositions(List<Neighbour> found) {
        List<VectorHit> hits = new ArrayList<>(found.size());
        for (Neighbour neighbour : found) {
            Optional<FleetRow> now = reads.drone(neighbour.entityId());
            hits.add(new VectorHit(
                    neighbour.entityId(),
                    neighbour.text(),
                    neighbour.similarity(),
                    now.map(FleetRow::observerId).orElse(null),
                    now.map(FleetRow::latitude).orElse(null),
                    now.map(FleetRow::longitude).orElse(null),
                    now.map(FleetRow::altitudeM).orElse(null),
                    now.map(FleetRow::isFlying).orElse(null)));
        }
        return hits;
    }

    private void requireCassandra() {
        if (!cassandra.connected()) {
            cassandra.connect();
        }
        if (!cassandra.connected()) {
            throw new ApiException(503, "Cassandra unavailable");
        }
    }
}
