package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * One question down several paths, timed, with the transactional path sampled beside it.
 *
 * <p>Sequentially by default: a timing is then of one path rather than of several competing for one
 * host, and the point read sampled beside it is the price that path charged the request path. Asked
 * to run in parallel, the paths contend deliberately, every figure inflates, and the probe becomes
 * one measurement over the whole window, since while the paths overlap the cost belongs to all of
 * them and to none in particular.
 *
 * <p>A path's failure is a result rather than an exception, so the comparison still renders when a
 * path cannot answer. For CQL and an aggregate that refusal is the point of showing it.
 */
@ApplicationScoped
public class Comparison {

    /**
     * How long the reference read is sampled for before any path runs.
     *
     * <p>Long enough for a dozen readings at the probe's interval, and short enough that it is not
     * most of the wait on the questions that answer in a second.
     */
    static final Duration BASELINE_WINDOW = Duration.ofSeconds(3);

    private final QueryPaths paths;
    private final QueryRunner runner;
    private final OltpSampler sampler;
    private final SingleRunGate gate;
    private final Duration baselineWindow;

    @Inject
    Comparison(QueryPaths paths, QueryRunner runner, OltpSampler sampler, SingleRunGate gate) {
        this(paths, runner, sampler, gate, BASELINE_WINDOW);
    }

    Comparison(
            QueryPaths paths,
            QueryRunner runner,
            OltpSampler sampler,
            SingleRunGate gate,
            Duration baselineWindow) {
        this.paths = paths;
        this.runner = runner;
        this.sampler = sampler;
        this.gate = gate;
        this.baselineWindow = baselineWindow;
    }

    /**
     * Resolve the paths asked for and take the one-at-a-time gate.
     *
     * <p>Called by the route rather than inside a run, so a refusal is a status: a stream whose first
     * line has gone out can only report a failure in its body.
     *
     * <p>The paths run in the order this backend declares them, so the columns of a comparison do not
     * move about depending on the order a caller named them in.
     */
    public Run begin(String sql, List<String> engines, RunMode mode, int limit, boolean reuse) {
        return begin(new Asked(sql, paths.chosen(engines, false), mode, limit, reuse));
    }

    /**
     * The same, for the stream route: the caller's own order, and sequential whatever was asked.
     *
     * <p>The order matters here because it is the order the paths answer in, and the dashboard sends
     * its quickest path first so that a viewer has something to read while the slow ones work. Paths
     * that overlap have no individual timing to report as each finishes, which is why this mode is
     * the sequential one and the parallel run keeps the whole-body route.
     */
    public Run beginStreamed(String sql, List<String> engines, int limit, boolean reuse) {
        return begin(
                new Asked(sql, paths.chosen(engines, true), RunMode.SEQUENTIAL, limit, reuse));
    }

    private Run begin(Asked asked) {
        return gate.begin(asked, sparkStatements(asked));
    }

    public void end(Run run) {
        gate.end(run);
    }

    /**
     * Sample the point read on its own, and remember the asset the per-path probes will read.
     *
     * <p>Empty when there is no asset to read, in which case the run reports no impact at all rather
     * than zeros that would read as a path costing nothing.
     */
    public Optional<OltpImpact> baseline(Run run) {
        Optional<String> subject = sampler.subject();
        if (subject.isEmpty()) {
            return Optional.empty();
        }
        run.probing(subject.get());
        try (OltpProbe probe = sampler.sample(subject.get())) {
            sleep(baselineWindow);
            return Optional.of(probe.impact());
        }
    }

    /**
     * One path at a time, telling the caller as each answers.
     *
     * <p>A cancelled run stops before its next path; the path already working is stopped by the path
     * itself, which is what {@link com.thelastpickle.htap.backend.engine.QueryPath#abort} is for.
     */
    public void each(Run run, BiConsumer<String, PathResult> answered) {
        for (String engine : run.engines()) {
            if (run.cancelled()) {
                return;
            }
            PathResult result = probed(run, engine);
            answered.accept(engine, result);
        }
    }

    /**
     * Every path at once, contending on purpose, with one probe over the whole window.
     *
     * <p>Each path answers more slowly than it would alone, and that is the point: the comparison is
     * being used to show interference rather than to avoid it. Each path has its own connection,
     * including the two Spark paths, so they genuinely overlap instead of queueing behind one
     * session.
     *
     * <p>There is nothing here for the cancel flag to prevent, since every path has already started;
     * a cancel stops these by taking their connections away.
     */
    public Optional<OltpImpact> together(Run run) {
        Optional<String> subject = run.subject();
        if (subject.isEmpty()) {
            runTogether(run);
            return Optional.empty();
        }
        try (OltpProbe probe = sampler.sample(subject.get())) {
            runTogether(run);
            return Optional.of(probe.impact());
        }
    }

    private void runTogether(Run run) {
        List<Thread> legs = new ArrayList<>();
        for (String engine : run.engines()) {
            legs.add(Thread.ofPlatform()
                    .name("compare-" + engine)
                    .start(() -> run.answered(engine, run(run, engine))));
        }
        boolean interrupted = false;
        for (Thread leg : legs) {
            while (true) {
                try {
                    leg.join();
                    break;
                } catch (InterruptedException e) {
                    // Joined to the end even so, and the interrupt re-raised below. Returning here
                    // would have the route serialise its answer, and the gate admit the next
                    // comparison, while these legs were still writing results and still holding
                    // their engine connections, which is the contention the gate exists to prevent.
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** One path, with the probe running only while it works. */
    private PathResult probed(Run run, String engine) {
        Optional<String> subject = run.subject();
        if (subject.isEmpty()) {
            PathResult result = run(run, engine);
            run.answered(engine, result);
            return result;
        }
        PathResult result;
        try (OltpProbe probe = sampler.sample(subject.get())) {
            result = run(run, engine);
            run.answered(engine, result);
            run.probed(engine, probe.impact());
        }
        return result;
    }

    private PathResult run(Run run, String engine) {
        Asked asked = run.asked();
        QueryPath path = paths.byName(engine).orElseThrow();
        try {
            return runner.run(path, asked.sql(), asked.limit(), asked.reuseSnapshot());
        } catch (RuntimeException e) {
            // The runner reports a failure in its result rather than raising, so this is only
            // reached if it fails outright. Recorded, because a leg that died silently would drop
            // the column and the comparison would look as though the path was never asked.
            return PathResult.unavailable(engine, Messages.oneLine(e));
        }
    }

    /** What the two Spark paths will submit, which is what a cancel has to recognise. */
    private List<String> sparkStatements(Asked asked) {
        return asked.engines().stream()
                .filter(name -> name.startsWith("spark"))
                .map(name -> paths.byName(name).orElseThrow().dialect(asked.sql(), asked.limit()))
                .toList();
    }

    private static void sleep(Duration window) {
        try {
            Thread.sleep(window);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
