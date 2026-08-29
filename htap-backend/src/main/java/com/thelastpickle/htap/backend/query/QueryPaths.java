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
        this(List.of(cassandra, presto, spark, sparkBulk, cqlite));
    }

    /** The paths in the given order, which is how a test builds a set it can predict. */
    QueryPaths(List<QueryPath> declared) {
        declared.forEach(path -> paths.put(path.name(), path));
    }

    /** Every path, in the declared order. */
    public List<QueryPath> all() {
        return List.copyOf(paths.values());
    }

    /** The path of that name, or empty: a caller may name anything. */
    public Optional<QueryPath> byName(String name) {
        return Optional.ofNullable(paths.get(name));
    }

    /**
     * The paths a comparison asked for, or refuse.
     *
     * <p>Null means all of them. Otherwise the order is this class's own, so the columns do not move
     * about depending on the order a caller named them in; {@code keepOrder} takes the caller's
     * order instead, which the stream route wants, since there the order is the order paths answer in
     * and the dashboard sends its quickest path first so a viewer has something to read while the
     * slow ones work.
     *
     * <p>A duplicate is dropped either way: the same path twice would be timed twice and reported
     * once.
     */
    public List<String> chosen(List<String> names, boolean keepOrder) {
        if (names == null) {
            return List.copyOf(paths.keySet());
        }
        List<String> unknown = names.stream().filter(name -> !paths.containsKey(name)).toList();
        if (!unknown.isEmpty()) {
            throw new Unknown("Unknown engine(s): " + String.join(", ", unknown));
        }
        List<String> order = keepOrder ? names : List.copyOf(paths.keySet());
        List<String> chosen = order.stream().filter(names::contains).distinct().toList();
        if (chosen.isEmpty()) {
            throw new Unknown("Choose at least one engine to compare");
        }
        return chosen;
    }

    /** A comparison naming a path this backend does not have, which the route answers 400. */
    public static class Unknown extends RuntimeException {

        private static final long serialVersionUID = 1L;

        Unknown(String detail) {
            super(detail);
        }
    }

    /** Which paths are connected, in order, for the engine selector. */
    public Map<String, Boolean> status() {
        SequencedMap<String, Boolean> status = new LinkedHashMap<>();
        paths.forEach((name, path) -> status.put(name, path.connected()));
        return status;
    }
}
