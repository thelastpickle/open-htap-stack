package com.thelastpickle.htap.backend.engine;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * Connects every access path at startup, in order, so the first dashboard poll is not paying
 * for it.
 *
 * <p>Never fatally: the stack's services come up in their own time, and every endpoint
 * already reports a path it cannot reach.
 *
 * <p>The order is explicit and not the container's injection order, because one pair depends
 * on it. The cqlite reader takes each table's {@code CREATE TABLE} from the driver's schema
 * metadata, so the CQL path must have connected once before the reader can register anything,
 * and it is last here for that reason.
 *
 * <p>On a virtual thread rather than in the startup observer, which is the one difference
 * from the Python: FastAPI's lifespan ran before the port opened, and an HTTP port that opens
 * only once every engine has answered is a port that a cold stack leaves shut for the 36
 * seconds cassandra-sql takes to create its keyspaces. Quarkus reports itself started
 * immediately and each path reports itself down until its own connect returns.
 */
@ApplicationScoped
public class EngineStartup {

    private static final Logger LOG = Logger.getLogger(EngineStartup.class);

    private final List<EnginePath> paths;

    EngineStartup(
            CassandraPath cassandra,
            PrestoPath presto,
            SparkPath spark,
            SparkBulkPath sparkBulk,
            CqlitePath cqlite) {
        this.paths = List.of(cassandra, presto, spark, sparkBulk, cqlite);
    }

    void onStart(@Observes StartupEvent event) {
        Thread.ofVirtual().name("engine-startup").start(this::connectAll);
    }

    private void connectAll() {
        for (EnginePath path : paths) {
            try {
                path.connect();
            } catch (RuntimeException e) {
                // An unforced connect reports rather than raises, so reaching here means a
                // path threw from outside its own gate.
                LOG.warnf(e, "%s unavailable at startup", path.name());
            }
        }
    }
}
