package com.thelastpickle.htap.backend.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class IndexerTest {

    private final FakeEmbeddings table = new FakeEmbeddings();
    private final Indexer indexer = new Indexer(new LocalEmbedder(), table);

    @Test
    void everySnippetIsEmbeddedAndStored() {
        Indexer.Indexed done = indexer.index(snippets(3), snippet -> {});

        assertEquals(new Indexer.Indexed(3, 0), done);
        assertEquals(Set.of("d0", "d1", "d2"), table.stored.keySet());
        assertEquals(LocalEmbedder.DIMENSIONS, table.stored.get("d0").length);
    }

    /**
     * A snippet whose write failed must be left for the next pass, so the caller is told about the
     * writes that landed and only those.
     */
    @Test
    void aFailedWriteIsCountedAndNotReported() {
        table.refuse.add("d1");
        List<String> reported = new CopyOnWriteArrayList<>();

        Indexer.Indexed done = indexer.index(snippets(3), snippet -> reported.add(snippet.entityId()));

        assertEquals(new Indexer.Indexed(2, 1), done);
        assertEquals(Set.of("d0", "d2"), Set.copyOf(reported));
        assertFalse(table.stored.containsKey("d1"));
    }

    /** The bound is the point: with a key configured each pair is a network round trip. */
    @Test
    void noMoreThanEightWritesRunAtOnce() {
        table.writeTakes = Duration.ofMillis(20);

        indexer.index(snippets(40), snippet -> {});

        assertEquals(40, table.stored.size());
        assertEquals(Indexer.CONCURRENCY, table.mostConcurrent.get());
    }

    @Test
    void indexAllEmbedsOnlyTheAssetsCarryingProse() {
        table.fleet.put("d0", "the drone climbed");
        table.fleet.put("d1", "");
        table.fleet.put("d2", "  ");
        table.fleet.put("d3", null);
        table.fleet.put("d4", "over the airport perimeter");

        indexer.indexAll();

        assertEquals(Set.of("d0", "d4"), table.stored.keySet());
    }

    @Test
    void indexAllReportsTheKindOfEmbedderInUse() {
        assertEquals("local", indexer.embedderKind());
    }

    /** A fleet whose whole snippet column is empty costs no writes and no failures. */
    @Test
    void indexAllOverAFleetWithNoProseDoesNothing() {
        table.fleet.put("d0", "");

        indexer.indexAll();

        assertTrue(table.stored.isEmpty());
    }

    private static List<Snippet> snippets(int count) {
        List<Snippet> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            batch.add(new Snippet("d" + i, "asset " + i + " over the airport perimeter"));
        }
        return batch;
    }
}
