package com.thelastpickle.htap.backend.api;

import com.thelastpickle.htap.backend.api.dto.QueryInFlight;
import com.thelastpickle.htap.backend.api.dto.RunningWork;
import com.thelastpickle.htap.backend.engine.PrestoQueries;
import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.engine.SparkUi;
import com.thelastpickle.htap.backend.query.QueryPaths;
import com.thelastpickle.htap.backend.query.SingleRunGate;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * What the engines are working on, and the comparison holding the gate.
 *
 * <p>Two of the five paths keep no list to read, so each answers with the reason instead of being
 * left out. An engine that could not be asked answers the same way: this page is what an operator
 * opens when the dashboard has gone slow, and a listing that goes missing under exactly those
 * conditions is the wrong shape of failure.
 */
@ApplicationScoped
public class WorkInFlight {

    private final SingleRunGate gate;
    private final QueryPaths paths;
    private final PrestoQueries presto;
    private final SparkUi sparkUi;

    @Inject
    WorkInFlight(SingleRunGate gate, QueryPaths paths, PrestoQueries presto, SparkUi sparkUi) {
        this.gate = gate;
        this.paths = paths;
        this.presto = presto;
        this.sparkUi = sparkUi;
    }

    /** Everything in flight, as the Health page reads it. */
    public RunningWork now() {
        List<QueryInFlight> queries = new ArrayList<>();
        Map<String, String> unreadable = new LinkedHashMap<>();
        list(unreadable, "presto", () -> presto.running().stream().map(QueryInFlight::of).toList())
                .forEach(queries::add);
        list(unreadable, "spark", () -> sparkUi.runningJobs().stream().map(QueryInFlight::of).toList())
                .forEach(queries::add);
        // A point read is milliseconds, so anything worth seeing here arrived through one of the two
        // above. Said rather than silently left out.
        unreadable.put("cassandra", "Cassandra keeps no list of running queries to read");
        // The reader runs in this process and gives a scan no handle, so there is nothing to list and
        // nothing to kill by id. A scan in flight is stopped with the comparison it belongs to.
        unreadable.put("cqlite", busy("cqlite")
                ? "a scan is running; stop it with the comparison"
                : "no scan is running");
        return new RunningWork(gate.running().orElse(null), queries, unreadable);
    }

    /**
     * One engine's listing, or the reason it could not be read.
     *
     * <p>The message is kept rather than logged and dropped: "the Spark UI could not be reached"
     * beside an empty list is the answer, where an empty list alone says the engine is idle.
     */
    private static List<QueryInFlight> list(
            Map<String, String> unreadable, String engine, Supplier<List<QueryInFlight>> listing) {
        try {
            return listing.get();
        } catch (RuntimeException e) {
            unreadable.put(engine, Messages.oneLine(e));
            return List.of();
        }
    }

    private boolean busy(String name) {
        return paths.byName(name).map(QueryPath::busy).orElse(false);
    }
}
