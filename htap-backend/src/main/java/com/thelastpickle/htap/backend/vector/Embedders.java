package com.thelastpickle.htap.backend.vector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.config.VectorSettings;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.jboss.logging.Logger;

/** Chooses between the two embedders once, at startup, on whether a key was configured. */
@ApplicationScoped
public class Embedders {

    private static final Logger LOG = Logger.getLogger(Embedders.class);

    /**
     * The application's one embedder.
     *
     * <p>Produced against the interface rather than injected as a class, because {@link
     * LocalEmbedder} and {@link RemoteEmbedder} are both final: a producer's declared type is what
     * gets the client proxy, so the implementations stay final and stay unit-testable.
     */
    @Produces
    @ApplicationScoped
    Embedder embedder(VectorSettings settings, ObjectMapper json) {
        LocalEmbedder local = new LocalEmbedder();
        if (!settings.remote()) {
            LOG.info("no embeddings key configured, embedding locally");
            return local;
        }
        LOG.infof("embedding through %s with model %s", settings.baseUrl(), settings.model());
        return new RemoteEmbedder(settings, json, local);
    }
}
