package com.thelastpickle.htap.backend.vector;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;

/**
 * Every read and write of {@code demo.drone_text_embeddings}.
 *
 * <p>The index is a storage-attached index (SAI) over a {@code vector<float, 1536>} column, and
 * the search is one CQL statement: Cassandra orders by approximate nearest neighbour and scores
 * each row it returns. So the analytical index is queried over the request path, which is the
 * comparison the Explore page is making.
 */
@ApplicationScoped
public class Embeddings {

    private final CassandraPath cassandra;

    Embeddings(CassandraPath cassandra) {
        this.cassandra = cassandra;
    }

    /** Every asset's current prose, from the table the sink writes. */
    public List<Snippet> current() {
        return snippets("SELECT entity_id, text_payload FROM drone_latest_status");
    }

    /**
     * The prose already embedded, without the vectors beside it.
     *
     * <p>Two columns and not three: the third is 1536 floats per row, and it answers no part of
     * "has this snippet been embedded".
     */
    public List<Snippet> indexed() {
        return snippets("SELECT entity_id, text_payload FROM drone_text_embeddings");
    }

    /** The nearest rows to a query vector, each scored. */
    public List<Neighbour> nearest(float[] query, int limit) {
        CqlVector<Float> asked = vector(query);
        List<Neighbour> found = new ArrayList<>();
        for (Row row : cassandra.execute(SimpleStatement.newInstance(annCql(limit), asked, asked))) {
            Float similarity = row.get("similarity", Float.class);
            found.add(new Neighbour(
                    row.getString("entity_id"),
                    row.getString("text_payload"),
                    similarity == null ? null : Double.valueOf(similarity.doubleValue())));
        }
        return found;
    }

    /** The prose and its vector written together, so a stored vector always matches its text. */
    public void store(Snippet snippet, float[] embedding) {
        cassandra.execute(SimpleStatement.newInstance(
                "INSERT INTO drone_text_embeddings "
                        + "(entity_id, text_payload, payload_vector, updated_at) "
                        + "VALUES (?, ?, ?, toTimestamp(now()))",
                snippet.entityId(),
                snippet.text(),
                vector(embedding)));
    }

    /**
     * One nearest-neighbour lookup, for the latency probe to time.
     *
     * <p>Here rather than in the probe, because the probe would otherwise need the vector encoding
     * and the fixed probe vector, which are this package's. The rows are discarded: what is being
     * timed is the index, and a probe that read a payload would time 1536 floats coming back too.
     */
    public void probeAnn() {
        cassandra.execute(probeStatement());
    }

    /** One row and one column: what is being timed is the index, not a payload coming back. */
    static SimpleStatement probeStatement() {
        return SimpleStatement.newInstance(
                "SELECT entity_id FROM drone_text_embeddings ORDER BY payload_vector ANN OF ? "
                        + "LIMIT 1",
                vector(LocalEmbedder.probe()));
    }

    /**
     * The search, with its row count inlined.
     *
     * <p>The limit is a literal because the caller has already refused anything above {@link
     * com.thelastpickle.htap.backend.api.dto.VectorSearchRequest#MOST_HITS}; the two vector markers
     * are bound, and they are the same vector twice, once to order by and once to score with.
     */
    static String annCql(int limit) {
        return "SELECT entity_id, text_payload, similarity_cosine(payload_vector, ?) AS similarity "
                + "FROM drone_text_embeddings ORDER BY payload_vector ANN OF ? LIMIT " + limit;
    }

    /**
     * A vector the driver can encode.
     *
     * <p>The default codec registry reads the dimension count off the value, so a simple statement
     * carries a 1536-float vector without a prepared statement's type metadata.
     */
    static CqlVector<Float> vector(float[] values) {
        List<Float> boxed = new ArrayList<>(values.length);
        for (float value : values) {
            boxed.add(value);
        }
        return CqlVector.newInstance(boxed);
    }

    private List<Snippet> snippets(String cql) {
        List<Snippet> read = new ArrayList<>();
        for (Row row : cassandra.execute(SimpleStatement.newInstance(cql))) {
            read.add(new Snippet(row.getString("entity_id"), row.getString("text_payload")));
        }
        return read;
    }
}
