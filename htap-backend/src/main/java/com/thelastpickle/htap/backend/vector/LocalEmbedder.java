package com.thelastpickle.htap.backend.vector;

import com.thelastpickle.htap.common.Blake2b;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The hashing trick: each token hashed to one dimension, the vector then normalised.
 *
 * <p>No network and no key, and still lexical: texts sharing vocabulary land near each other, so
 * the demo's cosine similarity ranks by something real rather than dressing noise up as a result.
 *
 * <p>The digest is BLAKE2b because the embeddings already in the table were written with it. A
 * per-process hash would be worse than a different one: Python's own {@code hash()} is salted per
 * interpreter, so a vector written by one run would not match a query embedded by the next, and
 * {@link String#hashCode} would tie this stack's index to one JDK's definition of it.
 */
public final class LocalEmbedder implements Embedder {

    /** The width of {@code payload_vector}, which the ingest sink declares. */
    public static final int DIMENSIONS = 1536;

    private static final Pattern TOKEN = Pattern.compile("[a-z0-9]+");

    /**
     * Tokens shorter than this are dropped.
     *
     * <p>Two-letter tokens are mostly function words, and each one lands in a dimension it then
     * shares with whatever else hashed there, so they add collisions without adding meaning.
     */
    private static final int SHORTEST_TOKEN = 3;

    @Override
    public String kind() {
        return "local";
    }

    @Override
    public float[] embed(String text) {
        double[] weights = new double[DIMENSIONS];
        Matcher tokens = TOKEN.matcher(text.toLowerCase(Locale.ROOT));
        while (tokens.find()) {
            String token = tokens.group();
            if (token.length() < SHORTEST_TOKEN) {
                continue;
            }
            byte[] digest = Blake2b.digest(token.getBytes(StandardCharsets.UTF_8), 8);
            weights[dimensionOf(digest)] += (digest[4] & 1) == 1 ? 1.0 : -1.0;
        }
        return unit(weights);
    }

    /**
     * A fixed unit vector, for a latency probe against the index.
     *
     * <p>Fixed so the probe times the index rather than the embedder, and so two probes are
     * comparable. It is also what an empty text embeds to, which keeps such a row at a constant
     * low similarity instead of at none.
     */
    public static float[] probe() {
        float[] vector = new float[DIMENSIONS];
        vector[0] = 1.0f;
        return vector;
    }

    /**
     * The vector scaled to length one, or the probe vector when it has no length at all.
     *
     * <p>Accumulated and normalised in {@code double} and narrowed once at the end, which is what
     * Python did before the driver narrowed it into a {@code float} column. Normalising in {@code
     * float} would answer differently in the last bits, and the stored vectors are the reference.
     */
    private static float[] unit(double[] weights) {
        double sumOfSquares = 0.0;
        for (double weight : weights) {
            sumOfSquares += weight * weight;
        }
        if (sumOfSquares == 0.0) {
            return probe();
        }
        double length = Math.sqrt(sumOfSquares);
        float[] vector = new float[DIMENSIONS];
        for (int i = 0; i < DIMENSIONS; i++) {
            vector[i] = (float) (weights[i] / length);
        }
        return vector;
    }

    /**
     * The dimension a token's digest names.
     *
     * <p>The digest's first four bytes big-endian, taken modulo the width, which is Python's {@code
     * int.from_bytes(digest[:4], "big") % 1536}. Widened to a {@code long} first, because that
     * integer is unsigned: half of all tokens have the high bit set, and a signed remainder would
     * send every one of them to a dimension 1024 away from the one holding its stored vector.
     */
    private static int dimensionOf(byte[] digest) {
        long value = ((long) (digest[0] & 0xFF) << 24) | ((digest[1] & 0xFF) << 16)
                | ((digest[2] & 0xFF) << 8) | (digest[3] & 0xFF);
        return (int) (value % DIMENSIONS);
    }
}
