package com.thelastpickle.htap.backend.vector;

import com.thelastpickle.htap.common.Blake2b;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/** One asset's prose, as the sink wrote it or as the index holds it. */
public record Snippet(String entityId, String text) {

    /** Whether there is anything here to embed. */
    public boolean present() {
        return text != null && !text.isBlank();
    }

    /**
     * A short digest of the prose, which is how a changed snippet is told from an unchanged one.
     *
     * <p>The digest rather than the prose: the producer samples paragraphs of Wikipedia, and
     * holding one per asset for a fleet of two thousand would keep megabytes alive for a
     * comparison that eight bytes settles.
     */
    public String digest() {
        return HexFormat.of()
                .formatHex(Blake2b.digest(text.getBytes(StandardCharsets.UTF_8), 8));
    }
}
