package com.thelastpickle.htap.backend.vector;

import com.thelastpickle.htap.backend.api.dto.LiveEmbeddingStatus;
import com.thelastpickle.htap.backend.config.VectorSettings;
import com.thelastpickle.htap.backend.support.Messages;
import com.thelastpickle.htap.backend.support.Round;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;

/**
 * Keeps {@code drone_text_embeddings} following the snippets the sink writes.
 *
 * <p>The sink writes an asset's new prose and waits for nothing else; this loop reads the prose
 * afterwards and embeds what changed. So the cost of keeping a vector index current is paid beside
 * the write path rather than in it, which is the same separation the analytical paths claim for
 * scans, and the reason the toggle is worth having: turn it on and the point-read latency on the
 * Health page should not move.
 *
 * <p>The alternative was to embed in the ingest sink, on the write itself, and it was rejected
 * twice over. It would put an embedding call, and with a key a network round trip, in front of
 * every write, which is the coupling this demo exists to argue against. And it would make the sink
 * depend on this backend for its embedder, where today nothing in the data path depends on the
 * dashboard and either dashboard service can be stopped without touching ingest.
 */
@ApplicationScoped
public class LiveEmbedder {

    private static final Logger LOG = Logger.getLogger(LiveEmbedder.class);

    private final Embeddings embeddings;
    private final Indexer indexer;
    private final VectorSettings settings;
    private final LongSupplier nanoClock;

    /**
     * Asset to the digest of the prose already embedded for it.
     *
     * <p>In memory only: on a restart the first pass primes it from the table, which costs one
     * fleet-sized read of two columns and saves re-embedding the whole fleet.
     */
    private final Map<String, String> seen = new ConcurrentHashMap<>();

    private final AtomicInteger embedded = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final AtomicInteger passes = new AtomicInteger();

    private volatile boolean enabled;
    private volatile boolean primed;
    private volatile int lastEmbedded;
    private volatile double lastPassMs;
    private volatile int pending;
    private volatile Long lastPassAtNanos;
    private volatile String error;
    private volatile boolean running = true;
    private volatile Thread loop;

    @Inject
    LiveEmbedder(Embeddings embeddings, Indexer indexer, VectorSettings settings) {
        this(embeddings, indexer, settings, System::nanoTime);
    }

    LiveEmbedder(
            Embeddings embeddings,
            Indexer indexer,
            VectorSettings settings,
            LongSupplier nanoClock) {
        this.embeddings = embeddings;
        this.indexer = indexer;
        this.settings = settings;
        this.nanoClock = nanoClock;
        this.enabled = settings.liveEmbeddings();
    }

    void onStart(@Observes StartupEvent event) {
        loop = Thread.ofVirtual().name("live-embedder").start(this::run);
    }

    void onStop(@Observes ShutdownEvent event) {
        running = false;
        Thread current = loop;
        if (current != null) {
            current.interrupt();
        }
    }

    /** Turns the loop on or off. Its counts and its digests survive either. */
    public LiveEmbeddingStatus enable(boolean wanted) {
        enabled = wanted;
        if (!wanted) {
            error = null;
        }
        return status();
    }

    public LiveEmbeddingStatus status() {
        Long at = lastPassAtNanos;
        return new LiveEmbeddingStatus(
                enabled,
                indexer.embedderKind(),
                settings.liveIntervalSeconds(),
                embedded.get(),
                failed.get(),
                passes.get(),
                lastEmbedded,
                lastPassMs,
                pending,
                at == null ? null : Round.tenth((nanoClock.getAsLong() - at) / 1e9),
                seen.size(),
                error);
    }

    /**
     * The loop itself, started once and stopped at shutdown.
     *
     * <p>It runs whether or not the toggle is on, and does nothing while it is off: one thread for
     * the process's life is easier to reason about than one started and stopped on each toggle, and
     * an idle pass costs a sleep.
     */
    private void run() {
        while (running) {
            tick();
            if (!sleepOneInterval()) {
                return;
            }
        }
    }

    /** One turn of the loop, which is a pass when the toggle is on and nothing when it is off. */
    void tick() {
        if (!enabled) {
            return;
        }
        try {
            pass();
            error = null;
        } catch (RuntimeException e) {
            // Report rather than stop: Cassandra restarting under the loop is an expected state
            // in this stack, and the loop should resume when it comes back rather than needing
            // the toggle cycled.
            error = Messages.oneLine(e);
            LOG.warnf("live embedding pass failed: %s", error);
        }
    }

    /**
     * Embeds the snippets that changed since the last pass.
     *
     * <p>Nothing checks that Cassandra is up first: a read through the CQL path connects if the
     * gate allows it and raises if it does not, which is the reconnect and the report in one.
     */
    void pass() {
        long started = nanoClock.getAsLong();
        if (!primed) {
            prime();
        }
        List<Snippet> changed = embeddings.current().stream()
                .filter(Snippet::present)
                .filter(snippet -> !snippet.digest().equals(seen.get(snippet.entityId())))
                .toList();
        // Whatever this pass defers, the next one takes.
        List<Snippet> batch = changed.subList(
                0, Math.min(changed.size(), settings.liveMaxPerCycle()));
        pending = changed.size() - batch.size();

        Indexer.Indexed done =
                indexer.index(batch, snippet -> seen.put(snippet.entityId(), snippet.digest()));

        embedded.addAndGet(done.stored());
        failed.addAndGet(done.failed());
        lastEmbedded = done.stored();
        lastPassMs = Round.tenth((nanoClock.getAsLong() - started) / 1e6);
        lastPassAtNanos = nanoClock.getAsLong();
        passes.incrementAndGet();
    }

    /** Learns which snippets are already embedded, reading two columns and leaving the third. */
    private void prime() {
        for (Snippet snippet : embeddings.indexed()) {
            if (snippet.present()) {
                seen.put(snippet.entityId(), snippet.digest());
            }
        }
        primed = true;
        LOG.infof("live embedding primed with %d assets", seen.size());
    }

    /** Sleeps one interval, answering whether the loop should go round again. */
    private boolean sleepOneInterval() {
        try {
            Thread.sleep(Duration.ofNanos((long) (settings.liveIntervalSeconds() * 1e9)));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
