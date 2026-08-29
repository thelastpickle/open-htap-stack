package com.thelastpickle.htap.backend.engine;

import com.thelastpickle.htap.backend.config.CassandraSettings;
import com.thelastpickle.htap.backend.config.SparkSettings;
import com.thelastpickle.htap.backend.query.Dialects;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * Spark SQL through the spark-cassandra-connector, which reads over the CQL request path.
 *
 * <p>Batch SQL that shares the coordinator with the OLTP traffic, so a scan here contends with
 * it. That is the comparison against {@link SparkBulkPath}, which reads the same rows off files
 * and cannot contend.
 */
@ApplicationScoped
public class SparkPath implements QueryPath {

    private static final Logger LOG = Logger.getLogger(SparkPath.class);

    /** As the Python's {@code RECONNECT_INTERVAL_S}. */
    static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

    /**
     * The tables both Spark paths register a view for.
     *
     * <p>Three rather than the console's seven: a view costs a table definition read through the
     * connector at registration, and these are the three the dashboard's Spark queries name.
     */
    public static final List<String> REGISTERED_TABLES =
            List.of("drone_latest_status", "drone_events_by_entity", "events");

    /** The connector registers each view under the table's own name, so nothing is prefixed. */
    static final String TABLE_PREFIX = "";

    private final CassandraSettings cassandra;
    private final ConnectionGate gate = new ConnectionGate("Spark", RETRY_INTERVAL);
    private final ThriftConnection thrift;

    @Inject
    SparkPath(CassandraSettings cassandra, SparkSettings spark) {
        this(cassandra, new ThriftConnection("connector", spark));
    }

    SparkPath(CassandraSettings cassandra, ThriftConnection thrift) {
        this.cassandra = cassandra;
        this.thrift = thrift;
    }

    @Override
    public String name() {
        return "spark";
    }

    @Override
    public void connect(boolean force) {
        gate.connect(force, () -> {
            thrift.open();
            registerViews();
        });
    }

    @Override
    public boolean connected() {
        return gate.connected();
    }

    @Override
    public boolean busy() {
        return thrift.busy();
    }

    @Override
    public boolean abort() {
        return thrift.abort();
    }

    @Override
    public String dialect(String sql, int limit) {
        return Dialects.sql(sql, TABLE_PREFIX, limit);
    }

    @Override
    public QueryRows query(String sql) {
        ready();
        try {
            return thrift.query(sql);
        } catch (EngineFailed e) {
            if (ThriftConnection.connectionIsGone(e)) {
                thrift.discard();
                gate.invalidate();
                throw e;
            }
            if (!ThriftConnection.viewsAreStale(e)) {
                throw e;
            }
            // Only a missing view is worth re-registering for. Retrying registration after any
            // other failure re-enters the code path that resolves a table definition, which is
            // where the failures come from in the first place.
            LOG.info("Spark views look stale; re-registering and retrying");
            registerViews();
            return thrift.query(sql);
        }
    }

    private void ready() {
        if (!gate.connected()) {
            connect();
        }
    }

    /**
     * Point Spark at the Cassandra tables through the CQL connector.
     *
     * <p>Best effort, per table: a connector or a keyspace that is not ready leaves the view
     * missing and the query says so, rather than the dashboard reporting Spark healthy and
     * returning nothing.
     */
    private void registerViews() {
        for (String table : REGISTERED_TABLES) {
            String ddl = "CREATE OR REPLACE TEMP VIEW %s USING org.apache.spark.sql.cassandra "
                    .formatted(table)
                    + "OPTIONS (keyspace '%s', table '%s')".formatted(cassandra.keyspace(), table);
            try {
                thrift.ddl(ddl);
                LOG.infof("Spark connector view registered: %s", table);
            } catch (RuntimeException e) {
                LOG.warnf("Spark view registration failed for %s: %s", table, e.getMessage());
            }
        }
    }

    @PreDestroy
    void shutdown() {
        gate.invalidate();
        thrift.discard();
    }
}
