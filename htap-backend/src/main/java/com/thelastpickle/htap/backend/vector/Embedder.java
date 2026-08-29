package com.thelastpickle.htap.backend.vector;

/** One text to one vector of {@link LocalEmbedder#DIMENSIONS} floats. */
public interface Embedder {

    /** {@code local} or {@code remote}, which is what the Explore page reports. */
    String kind();

    float[] embed(String text);
}
