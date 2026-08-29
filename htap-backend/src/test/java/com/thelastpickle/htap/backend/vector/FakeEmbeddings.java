package com.thelastpickle.htap.backend.vector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The embedding table, in memory.
 *
 * <p>A subclass rather than an interface, because the four methods are the whole of what the
 * indexer and the loop use and widening a seam for a test would be the larger change. The
 * superclass's CQL path is never reached, so it is constructed with none.
 */
final class FakeEmbeddings extends Embeddings {

    /** The prose the sink has written, which a pass reads. */
    final Map<String, String> fleet = new LinkedHashMap<>();

    /** What has been stored, in the order the writes completed. */
    final Map<String, float[]> stored = new ConcurrentHashMap<>();

    /** Assets whose write fails, so a retry can be tested. */
    final Set<String> refuse = ConcurrentHashMap.newKeySet();

    /** Concurrent writes at their highest, which is what bounds the indexer. */
    final AtomicInteger writing = new AtomicInteger();
    final AtomicInteger mostConcurrent = new AtomicInteger();

    private final List<String> alreadyIndexed = new ArrayList<>();
    final AtomicInteger indexedReads = new AtomicInteger();

    /** How long a write holds its permit, so that concurrent writes overlap observably. */
    volatile Duration writeTakes = Duration.ZERO;

    FakeEmbeddings() {
        super(null);
    }

    /** Declares an asset as already embedded, which is what a prime should find. */
    void alreadyIndexed(String entityId) {
        alreadyIndexed.add(entityId);
    }

    /** Set to make the fleet read fail, which is what a Cassandra that has gone away looks like. */
    volatile RuntimeException fleetReadFails;

    @Override
    public List<Snippet> current() {
        if (fleetReadFails != null) {
            throw fleetReadFails;
        }
        return fleet.entrySet().stream()
                .map(entry -> new Snippet(entry.getKey(), entry.getValue()))
                .toList();
    }

    @Override
    public List<Snippet> indexed() {
        indexedReads.incrementAndGet();
        return alreadyIndexed.stream()
                .map(entityId -> new Snippet(entityId, fleet.get(entityId)))
                .toList();
    }

    @Override
    public void store(Snippet snippet, float[] embedding) {
        mostConcurrent.accumulateAndGet(writing.incrementAndGet(), Math::max);
        try {
            if (!writeTakes.isZero()) {
                try {
                    Thread.sleep(writeTakes);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (refuse.contains(snippet.entityId())) {
                throw new IllegalStateException("write refused for " + snippet.entityId());
            }
            stored.put(snippet.entityId(), embedding);
        } finally {
            writing.decrementAndGet();
        }
    }
}
