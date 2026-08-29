package com.thelastpickle.htap.backend.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import org.junit.jupiter.api.Test;

class EmbeddingsTest {

    /**
     * The search's own shape, which no unit test of the row mapping would cover: the ordering
     * clause is what makes this an approximate-nearest-neighbour query rather than a scan, and the
     * same vector has to reach both the ordering and the scoring function.
     */
    @Test
    void theSearchOrdersByTheIndexAndScoresWithTheSameVector() {
        assertEquals(
                "SELECT entity_id, text_payload, similarity_cosine(payload_vector, ?) AS similarity "
                        + "FROM drone_text_embeddings ORDER BY payload_vector ANN OF ? LIMIT 5",
                Embeddings.annCql(5));
    }

    /**
     * The latency probe's statement, which is timed rather than read: it names one column and one
     * row, so what the figure covers is the index and not a 1536-float payload coming back.
     */
    @Test
    void theProbeAsksTheIndexForOneRowAndOneColumn() {
        SimpleStatement probe = Embeddings.probeStatement();

        assertEquals(
                "SELECT entity_id FROM drone_text_embeddings ORDER BY payload_vector ANN OF ? "
                        + "LIMIT 1",
                probe.getQuery());
        assertEquals(1, probe.getPositionalValues().size());
        assertEquals(
                LocalEmbedder.DIMENSIONS,
                ((CqlVector<?>) probe.getPositionalValues().get(0)).size());
    }

    /** Fixed, so two probes a minute apart are timing the same query. */
    @Test
    void theProbeAsksForTheSameVectorEveryTime() {
        assertEquals(
                Embeddings.probeStatement().getPositionalValues(),
                Embeddings.probeStatement().getPositionalValues());
    }

    @Test
    void aVectorKeepsItsValuesAndItsOrder() {
        CqlVector<Float> vector = Embeddings.vector(new float[] {0.5f, -0.25f, 0.0f});

        assertEquals(3, vector.size());
        assertEquals(0.5f, vector.get(0));
        assertEquals(-0.25f, vector.get(1));
        assertEquals(0.0f, vector.get(2));
    }

    /**
     * A simple statement carries no type metadata, so the driver has to work the column type out
     * from the value. It does, from the element type and the count, which is why nothing here
     * prepares a statement to bind 1536 floats.
     */
    @Test
    void theDriverInfersTheColumnTypeFromTheVectorItself() {
        CqlVector<Float> vector = Embeddings.vector(new float[LocalEmbedder.DIMENSIONS]);

        assertEquals(
                DataTypes.vectorOf(DataTypes.FLOAT, LocalEmbedder.DIMENSIONS),
                CodecRegistry.DEFAULT.codecFor(vector).getCqlType());
    }
}
