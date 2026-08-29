package com.thelastpickle.htap.backend.vector;

import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/** Embeds snippets and writes them, a bounded number of pairs at a time. */
@ApplicationScoped
public class Indexer {

    private static final Logger LOG = Logger.getLogger(Indexer.class);

    /**
     * Concurrent embed-and-write pairs.
     *
     * <p>A bound rather than one task per asset: with a key configured each pair is a network
     * round trip, and a fleet of two thousand would open two thousand of them at once.
     */
    static final int CONCURRENCY = 8;

    /** What one call embedded, and what it could not. */
    public record Indexed(int stored, int failed) {}

    private final Embedder embedder;
    private final Embeddings embeddings;

    Indexer(Embedder embedder, Embeddings embeddings) {
        this.embedder = embedder;
        this.embeddings = embeddings;
    }

    /** Which embedder is in use, which both the Explore page and the bulk answer report. */
    public String embedderKind() {
        return embedder.kind();
    }

    /**
     * Embeds and stores each snippet, reporting each one that was written.
     *
     * <p>{@code onStored} runs after the write and not before, so a snippet whose write failed is
     * left for the caller to try again rather than recorded as done.
     */
    public Indexed index(List<Snippet> batch, Consumer<Snippet> onStored) {
        AtomicInteger stored = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        Semaphore permits = new Semaphore(CONCURRENCY);
        // Closing the executor waits for every task, so this returns with the batch finished.
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Snippet snippet : batch) {
                pool.execute(() -> {
                    permits.acquireUninterruptibly();
                    try {
                        embeddings.store(snippet, embedder.embed(snippet.text()));
                        onStored.accept(snippet);
                        stored.incrementAndGet();
                    } catch (RuntimeException e) {
                        failed.incrementAndGet();
                        LOG.warnf("embedding %s failed: %s",
                                snippet.entityId(), Messages.oneLine(e));
                    } finally {
                        permits.release();
                    }
                });
            }
        }
        return new Indexed(stored.get(), failed.get());
    }

    /** Embeds every asset's current snippet. Runs on after the route has answered. */
    public void indexAll() {
        List<Snippet> fleet;
        try {
            fleet = embeddings.current();
        } catch (RuntimeException e) {
            LOG.warnf("could not read the rows to index: %s", Messages.oneLine(e));
            return;
        }
        List<Snippet> pending = fleet.stream().filter(Snippet::present).toList();
        LOG.infof("indexing %d of %d assets (%d carry no text)",
                pending.size(), fleet.size(), fleet.size() - pending.size());
        if (pending.isEmpty()) {
            return;
        }
        Indexed done = index(pending, snippet -> {});
        LOG.infof("indexed %d of %d assets", done.stored(), pending.size());
    }
}
