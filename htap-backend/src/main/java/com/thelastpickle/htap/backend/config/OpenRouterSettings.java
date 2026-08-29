package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;

/**
 * An optional OpenAI-compatible chat endpoint, used to translate a question into SQL.
 *
 * <p>Optional is the point: with no key the natural-language page falls back to the rule-based
 * translator, so the demo runs with no account anywhere and no network egress.
 */
@ConfigMapping(prefix = "openrouter")
public interface OpenRouterSettings {

    Optional<String> apiKey();

    @WithDefault("openai/gpt-4o-mini")
    String model();

    @WithDefault("https://openrouter.ai/api/v1/chat/completions")
    String url();

    /**
     * The bound on one translation.
     *
     * <p>Generous, because a translation is one request a viewer waited for rather than a poll, and
     * the fallback is a worse answer rather than no answer: a timeout here spends the whole budget
     * and then still renders, having translated with the patterns.
     */
    @WithDefault("30s")
    Duration timeout();

    /** Whether a key was configured, which is what decides between the two translators. */
    default boolean configured() {
        return apiKey().filter(key -> !key.isBlank()).isPresent();
    }
}
