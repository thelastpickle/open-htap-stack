package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Optional;

/**
 * Where embeddings come from, and how hard the live embedder follows the writes.
 *
 * <p>The endpoint is optional, and that is the point: with no key the backend embeds locally, so
 * the vector demo runs with no account anywhere and no network egress. The three {@code live-}
 * settings govern the loop that keeps the index following the snippets the sink writes.
 */
@ConfigMapping(prefix = "vector")
public interface VectorSettings {

    /** An OpenAI-compatible embeddings endpoint. Absent means the local embedder. */
    Optional<String> apiKey();

    @WithDefault("https://api.openai.com/v1")
    String baseUrl();

    @WithDefault("text-embedding-3-small")
    String model();

    /**
     * The bound on one embedding request.
     *
     * <p>A failed request is answered by the local embedder rather than by an error, so a timeout
     * here costs the whole budget and then still returns a vector.
     */
    @WithDefault("20s")
    Duration timeout();

    /**
     * Whether the live embedder starts enabled.
     *
     * <p>Off, so that turning it on is what shows the point read staying where it was: a toggle
     * already on when the page opens demonstrates nothing.
     */
    @WithDefault("false")
    boolean liveEmbeddings();

    /** How long the loop sleeps between passes, whether or not it is enabled. */
    @WithName("live-interval-s")
    @WithDefault("5")
    double liveIntervalSeconds();

    /**
     * The most snippets one pass embeds.
     *
     * <p>Whatever a pass defers, the next one takes, so this bounds a pass rather than the work:
     * a fleet whose every snippet changed is embedded over several passes instead of one long one.
     */
    @WithDefault("64")
    int liveMaxPerCycle();

    /** Whether an endpoint was configured, which is what decides between the two embedders. */
    default boolean remote() {
        return apiKey().filter(key -> !key.isBlank()).isPresent();
    }
}
