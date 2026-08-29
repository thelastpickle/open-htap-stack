package com.thelastpickle.htap.backend.demo;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.thelastpickle.htap.backend.api.dto.LatencyReport;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.engine.PrestoPath;
import com.thelastpickle.htap.backend.read.CassandraReads;
import com.thelastpickle.htap.backend.support.Round;
import com.thelastpickle.htap.backend.vector.Embeddings;
import com.thelastpickle.htap.common.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Iterator;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * One representative query per tier, timed on the tier's own path.
 *
 * <p>Each probe is split into choosing what to read and reading it, and only the second half is
 * timed. The point read is why: choosing which asset to read is itself a scan, and timing it would
 * report a scan's latency as a point read's, which is the claim the whole dashboard rests on.
 */
@ApplicationScoped
public class LatencyProbes {

    private final CassandraPath cassandra;
    private final PrestoPath presto;
    private final CassandraReads reads;
    private final Embeddings embeddings;
    private final Clock clock;
    private final LongSupplier nanoClock;

    /**
     * The asset the point read reads, held between calls.
     *
     * <p>Kept so that the scan which chose it is paid once rather than on every poll of the page.
     * Cleared when the asset turns out to be gone, a fleet resize being the usual reason.
     */
    private volatile String probeEntityId;

    @Inject
    LatencyProbes(
            CassandraPath cassandra,
            PrestoPath presto,
            CassandraReads reads,
            Embeddings embeddings) {
        this(cassandra, presto, reads, embeddings, Clock.systemUTC(), System::nanoTime);
    }

    LatencyProbes(
            CassandraPath cassandra,
            PrestoPath presto,
            CassandraReads reads,
            Embeddings embeddings,
            Clock clock,
            LongSupplier nanoClock) {
        this.cassandra = cassandra;
        this.presto = presto;
        this.reads = reads;
        this.embeddings = embeddings;
        this.clock = clock;
        this.nanoClock = nanoClock;
    }

    public LatencyReport measure() {
        return new LatencyReport(
                timed(this::cassandraPointRead),
                timed(this::prestoScan),
                timed(this::annLookup),
                Timestamps.isoOffset(clock.instant()));
    }

    /**
     * Times one query in milliseconds, or reports null if it cannot run.
     *
     * <p>{@code prepare} does whatever setup a probe needs and hands back the one call to time, so
     * setup never lands inside the measurement. A null from it, or any failure from either half,
     * is a tier that cannot answer; the page shows that as an em dash.
     */
    Double timed(Supplier<Runnable> prepare) {
        try {
            Runnable query = prepare.get();
            if (query == null) {
                return null;
            }
            long began = nanoClock.getAsLong();
            query.run();
            return Round.tenth((nanoClock.getAsLong() - began) / 1e6);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** A single-partition read: the transactional path the dashboard claims is fast. */
    private Runnable cassandraPointRead() {
        if (!cassandra.connected()) {
            return null;
        }
        String chosen = probeEntityId;
        if (chosen == null) {
            chosen = anyEntityId();
            if (chosen == null) {
                return null;
            }
            probeEntityId = chosen;
        }

        String entityId = chosen;
        return () -> {
            if (reads.drone(entityId).isEmpty()) {
                probeEntityId = null;
                throw new IllegalStateException("no such asset: " + entityId);
            }
        };
    }

    /** An aggregate over the same table: the analytical path, for contrast. */
    private Runnable prestoScan() {
        if (!presto.connected()) {
            return null;
        }
        return () -> presto.query("SELECT count(*) AS cnt FROM demo.drone_latest_status");
    }

    /** An approximate-nearest-neighbour lookup, which answers nothing until rows are indexed. */
    private Runnable annLookup() {
        if (!cassandra.connected()) {
            return null;
        }
        return embeddings::probeAnn;
    }

    private String anyEntityId() {
        Iterator<Row> rows = cassandra
                .execute(SimpleStatement.newInstance(
                        "SELECT entity_id FROM drone_latest_status LIMIT 1"))
                .iterator();
        return rows.hasNext() ? rows.next().getString("entity_id") : null;
    }
}
