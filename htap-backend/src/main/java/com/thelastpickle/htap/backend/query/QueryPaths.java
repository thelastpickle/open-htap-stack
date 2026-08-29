package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.backend.engine.CqlitePath;
import com.thelastpickle.htap.backend.engine.PrestoPath;
import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.engine.SparkBulkPath;
import com.thelastpickle.htap.backend.engine.SparkPath;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedMap;

/**
 * The five paths in the order the dashboard shows them and the comparison runs them.
 *
 * <p>Cassandra first, because it is the transactional path the other four are contrasted with, and
 * the cqlite reader last, because it is the newest and the one a viewer reads against the rest.
 *
 * <p>The order is declared here rather than left to the container, which injects in no order a
 * viewer could rely on.
 */
@ApplicationScoped
public class QueryPaths {

    private final SequencedMap<String, QueryPath> paths = new LinkedHashMap<>();

    @Inject
    QueryPaths(
            CassandraPath cassandra,
            PrestoPath presto,
            SparkPath spark,
            SparkBulkPath sparkBulk,
            CqlitePath cqlite) {
        for (QueryPath path : List.of(cassandra, presto, spark, sparkBulk, cqlite)) {
            paths.put(path.name(), path);
        }
    }

    /** Every path, in the declared order. */
    public List<QueryPath> all() {
        return List.copyOf(paths.values());
    }

    /** The path of that name, or empty: a caller may name anything. */
    public Optional<QueryPath> byName(String name) {
        return Optional.ofNullable(paths.get(name));
    }

    /** Which paths are connected, in order, for the engine selector. */
    public Map<String, Boolean> status() {
        SequencedMap<String, Boolean> status = new LinkedHashMap<>();
        paths.forEach((name, path) -> status.put(name, path.connected()));
        return status;
    }
}
