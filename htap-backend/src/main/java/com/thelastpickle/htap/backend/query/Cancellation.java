package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.config.PrestoSettings;
import com.thelastpickle.htap.backend.engine.PrestoQueries;
import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.engine.RunningQuery;
import com.thelastpickle.htap.backend.engine.SparkUi;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Stopping the comparison in flight, and saying what that took.
 *
 * <p>Four mechanisms, because the paths are four different kinds of client: the paths that have not
 * started are stopped by a flag on the run, a Presto query is cancelled by its coordinator, a Spark
 * statement has its connection taken away because a JDBC client cannot cancel one it is already
 * waiting on, and the cqlite reader is asked to stop its own scan, since it runs in this process.
 * Cassandra is absent on purpose: its legs are single-digit milliseconds, so there is never one to
 * stop.
 *
 * <p>Every mechanism reports in words rather than in a status, because which of them fired is what
 * makes a cancel readable: an operator who reads "took the connection away from spark_bulk" knows
 * something different from one who reads only that the run was cancelled. A mechanism that fails is
 * reported the same way and does not stop the others, since a Presto coordinator that cannot be
 * reached is no reason to leave a Spark job running.
 */
@ApplicationScoped
public class Cancellation {

    private final SingleRunGate gate;
    private final QueryPaths paths;
    private final PrestoQueries presto;
    private final PrestoSettings prestoSettings;
    private final SparkUi sparkUi;

    @Inject
    Cancellation(
            SingleRunGate gate,
            QueryPaths paths,
            PrestoQueries presto,
            PrestoSettings prestoSettings,
            SparkUi sparkUi) {
        this.gate = gate;
        this.paths = paths;
        this.presto = presto;
        this.prestoSettings = prestoSettings;
        this.sparkUi = sparkUi;
    }

    /**
     * Stop the run in flight, or report that there was none.
     *
     * <p>An empty list means nothing was running, which the route answers as a refusal rather than a
     * success: a control that reports having stopped nothing reads as though it had worked.
     *
     * <p>The run itself ends the moment its paths stop, and the request that started it gets an
     * ordinary response marked cancelled, if anything is still listening.
     */
    public List<String> cancel() {
        Run run = gate.inFlight().orElse(null);
        if (run == null) {
            return List.of();
        }
        run.cancel();
        List<String> actions = new ArrayList<>();
        actions.add("stopped the paths that had not started yet");
        List<String> engines = run.engines();
        if (engines.contains("presto")) {
            cancelPresto(actions);
        }
        for (String name : List.of("spark", "spark_bulk")) {
            if (engines.contains(name)) {
                takeConnectionAway(name, actions);
            }
        }
        if (engines.contains("cqlite")) {
            stopCqlite(actions);
        }
        if (!run.sparkStatements().isEmpty()) {
            killSparkJobs(run, actions);
        }
        return actions;
    }

    /**
     * Cancel this backend's own Presto queries and no others.
     *
     * <p>Filtered by the user the paths connect as, so a {@code presto-cli} session in the container
     * survives a cancel here. That is the same reason the Spark half matches by statement.
     */
    private void cancelPresto(List<String> actions) {
        try {
            List<String> killed = presto.running().stream()
                    .filter(query -> query.user().equals(prestoSettings.user()))
                    .map(RunningQuery::id)
                    .toList();
            for (String id : killed) {
                presto.kill(id);
            }
            if (!killed.isEmpty()) {
                actions.add("cancelled " + killed.size() + " Presto quer(y/ies): "
                        + String.join(", ", killed));
            }
        } catch (RuntimeException e) {
            actions.add("could not cancel the Presto query: " + Messages.oneLine(e));
        }
    }

    private void takeConnectionAway(String name, List<String> actions) {
        try {
            if (path(name).abort()) {
                actions.add("took the connection away from " + name);
            }
        } catch (RuntimeException e) {
            actions.add("could not stop " + name + ": " + Messages.oneLine(e));
        }
    }

    /**
     * No connection to take away, and none to rebuild afterwards: the scan is in this process and
     * stops at its next partition once it has been asked to.
     */
    private void stopCqlite(List<String> actions) {
        try {
            if (path("cqlite").abort()) {
                actions.add("stopped the cqlite scan");
            }
        } catch (RuntimeException e) {
            actions.add("could not stop cqlite: " + Messages.oneLine(e));
        }
    }

    /**
     * Both halves are needed.
     *
     * <p>The connection taken away above stops this backend waiting; this stops Spark working, which
     * it otherwise carries on doing for a session that has gone, keeping the cores the next
     * comparison would be timed against.
     */
    private void killSparkJobs(Run run, List<String> actions) {
        try {
            List<String> killed = sparkUi.killJobsFor(run.sparkStatements());
            if (!killed.isEmpty()) {
                actions.add("killed " + killed.size() + " Spark job(s): "
                        + String.join(", ", killed));
            }
        } catch (RuntimeException e) {
            actions.add("could not kill the Spark job(s): " + Messages.oneLine(e));
        }
    }

    private QueryPath path(String name) {
        return paths.byName(name).orElseThrow();
    }
}
