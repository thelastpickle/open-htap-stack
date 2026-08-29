package com.thelastpickle.htap.backend.query;

import java.util.Optional;

/** Where the comparison's reference point read comes from, and what it reads. */
public interface OltpSampler {

    /**
     * An asset to point-read, or empty when Cassandra cannot be asked for one.
     *
     * <p>Empty is not a failure: the comparison runs without a probe and reports no impact, which
     * is what a stack whose sink has not created the table yet can honestly say.
     */
    Optional<String> subject();

    /** A probe reading that one asset, running until it is closed. */
    OltpProbe sample(String entityId);
}
