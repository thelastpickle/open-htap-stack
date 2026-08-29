package com.thelastpickle.htap.backend.vector;

import com.thelastpickle.htap.backend.config.VectorSettings;
import java.time.Duration;
import java.util.Optional;

/**
 * The vector settings a test declares.
 *
 * <p>A record rather than seven overridden methods, and in this package because the live embedder
 * and both embedders each need settings of a test's own.
 */
public final class Vectors {

    private Vectors() {}

    /** No key, which is the local embedder and no network. */
    public static VectorSettings local() {
        return declared(Optional.empty(), "https://api.openai.com/v1", false, 5.0, 64);
    }

    /** A key and an endpoint a test's own server is listening on. */
    public static VectorSettings endpointAt(String baseUrl) {
        return declared(Optional.of("a-test-key"), baseUrl, false, 5.0, 64);
    }

    /** The local embedder, with the live loop's three settings chosen. */
    public static VectorSettings live(boolean enabled, double intervalSeconds, int maxPerCycle) {
        return declared(
                Optional.empty(),
                "https://api.openai.com/v1",
                enabled,
                intervalSeconds,
                maxPerCycle);
    }

    private static VectorSettings declared(
            Optional<String> apiKey,
            String baseUrl,
            boolean liveEmbeddings,
            double liveIntervalSeconds,
            int liveMaxPerCycle) {
        return new Declared(
                apiKey,
                baseUrl,
                "text-embedding-3-small",
                Duration.ofSeconds(5),
                liveEmbeddings,
                liveIntervalSeconds,
                liveMaxPerCycle);
    }

    private record Declared(
            Optional<String> apiKey,
            String baseUrl,
            String model,
            Duration timeout,
            boolean liveEmbeddings,
            double liveIntervalSeconds,
            int liveMaxPerCycle)
            implements VectorSettings {}
}
