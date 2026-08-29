package com.thelastpickle.htap.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** What a snippet is, and what happens when the corpus named is not in the image. */
class CorpusSamplerTest {

    private static final String CORPUS =
            "Urban planning is a technical process. It regulates land use in cities. "
                    + "Hydrology is the study of water. Rivers carry sediment to the sea! "
                    + "Is a lake still water? Glaciers move slowly downhill. "
                    + "Bridges span rivers and valleys. Tunnels pass beneath them. ";

    @TempDir
    Path directory;

    /** One to three sentences, and whitespace collapsed to what the page shows. */
    @Test
    void aSnippetIsOneToThreeSentences() throws IOException {
        try (CorpusSampler sampler = new CorpusSampler(corpus(CORPUS.repeat(20)))) {
            for (long seed = 0; seed < 40; seed++) {
                String snippet = sampler.sample(seed);
                long terminators = snippet.chars().filter(c -> c == '.' || c == '?' || c == '!').count();

                assertTrue(terminators >= 1 && terminators <= 3, "sentences in [" + snippet + "]");
                assertFalse(snippet.contains("  "), "whitespace was not collapsed: " + snippet);
                assertEquals(snippet.strip(), snippet);
            }
        }
    }

    /**
     * The same seed gives the same snippet, and a different one usually does not.
     *
     * <p>The first half is what makes a cached snippet stable between refreshes; the second is what
     * makes a refresh worth doing. Seeding on the asset alone gave every asset one fixed snippet for
     * the life of the process, which is the bug this arrangement replaced.
     */
    @Test
    void theSeedDecidesTheSnippet() throws IOException {
        try (CorpusSampler sampler = new CorpusSampler(corpus(CORPUS.repeat(20)))) {
            assertEquals(sampler.sample(7), sampler.sample(7));

            Set<String> snippets = new HashSet<>();
            for (long seed = 0; seed < 20; seed++) {
                snippets.add(sampler.sample(seed));
            }
            assertTrue(snippets.size() > 10, "twenty seeds gave only " + snippets.size() + " snippets");
        }
    }

    /** A corpus with nothing in it is no text rather than a failure. */
    @Test
    void aCorpusTooSmallToSampleGivesNoText() throws IOException {
        try (CorpusSampler sampler = new CorpusSampler(corpus("short."))) {
            assertEquals("", sampler.sample(1));
        }
    }

    /**
     * A missing {@code enwik9} names the build argument that fetches it.
     *
     * <p>It is a supported configuration and it is off by default, because the fetch is 322 MB from
     * one small host; a bare "no such file" would leave an operator with no way to know that.
     */
    @Test
    void aMissingEnwik9NamesTheBuildArgument() {
        IOException refused = assertThrows(
                IOException.class, () -> new CorpusSampler(directory.resolve("enwik9")));

        assertTrue(refused.getMessage().contains("FETCH_ENWIK9=1"), refused.getMessage());
        assertTrue(refused.getMessage().contains("/app/wikipedia.txt"), refused.getMessage());
    }

    /** Any other missing corpus says so plainly, with no advice that does not apply to it. */
    @Test
    void anyOtherMissingCorpusJustSaysSo() {
        IOException refused = assertThrows(
                IOException.class, () -> new CorpusSampler(directory.resolve("wikipedia.txt")));

        assertTrue(refused.getMessage().endsWith("is not in this image"), refused.getMessage());
    }

    /** A snippet may begin mid-sentence at a window edge, which is the price of a bounded scan. */
    @Test
    void aScanIsBoundedRatherThanExact() throws IOException {
        try (CorpusSampler sampler = new CorpusSampler(corpus(CORPUS))) {
            assertEquals("It regulates land use in cities.", sampler.extract(38, 1));
        }
    }

    private Path corpus(String text) throws IOException {
        Path path = directory.resolve("corpus-" + text.length() + ".txt");
        Files.write(path, text.getBytes(StandardCharsets.UTF_8));
        return path;
    }
}
