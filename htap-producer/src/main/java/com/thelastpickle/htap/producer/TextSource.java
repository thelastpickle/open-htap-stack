package com.thelastpickle.htap.producer;

/**
 * Where an asset's text payload comes from.
 *
 * <p>An interface rather than a nullable sampler, so the no-corpus case is a value: the Python
 * branched on {@code text_sampler is not None} in the middle of the step, and the two branches
 * differed only in what the cache was filled with.
 */
interface TextSource extends AutoCloseable {

    /** No corpus: every asset reports an empty payload, and the refresh cadence still runs. */
    TextSource NONE = seed -> "";

    /**
     * A snippet chosen by {@code seed} alone, so the same seed gives the same text.
     *
     * <p>Deterministic on purpose. The seed carries the asset and its refresh count, which is
     * what makes a refresh yield new text: seeding on the asset alone gave every asset one fixed
     * snippet for the life of the process.
     */
    String sample(long seed);

    /** Nothing to release, for the source that holds nothing; the mapped one overrides it. */
    @Override
    default void close() {}
}
