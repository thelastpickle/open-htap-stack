package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.support.Round;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * The comparison in flight: what was asked, what has answered, and whether it was stopped.
 *
 * <p>Results are filled in as paths answer rather than returned at the end, so a run can be watched
 * while it works: what the map holds is what has answered, and the paths a cancelled run never
 * reached stay absent. The Health page reads this, and the stream route reports from it.
 */
public final class Run {

    private final Asked asked;
    private final List<String> sparkStatements;
    private final LongSupplier nanoClock;
    private final long startedNanos;
    private final Map<String, PathResult> results = new ConcurrentHashMap<>();
    private final Map<String, OltpImpact> impacts = new ConcurrentHashMap<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private volatile String subject;

    Run(Asked asked, List<String> sparkStatements, LongSupplier nanoClock) {
        this.asked = asked;
        this.sparkStatements = List.copyOf(sparkStatements);
        this.nanoClock = nanoClock;
        this.startedNanos = nanoClock.getAsLong();
    }

    public Asked asked() {
        return asked;
    }

    public List<String> engines() {
        return asked.engines();
    }

    /**
     * What each Spark path will submit.
     *
     * <p>Worked out when the run begins, because a cancel has to recognise those jobs among
     * everything else the shared Thrift Server may be running, and by then the path is busy with
     * the statement rather than able to be asked for it.
     */
    public List<String> sparkStatements() {
        return sparkStatements;
    }

    /** The asset the probes read, once a baseline has chosen one. */
    Optional<String> subject() {
        return Optional.ofNullable(subject);
    }

    void probing(String entityId) {
        this.subject = entityId;
    }

    void answered(String engine, PathResult result) {
        results.put(engine, result);
    }

    void probed(String engine, OltpImpact impact) {
        impacts.put(engine, impact);
    }

    public Map<String, PathResult> results() {
        return Map.copyOf(results);
    }

    public Map<String, OltpImpact> impacts() {
        return Map.copyOf(impacts);
    }

    /** Ask the run to stop before its next path; a path already working is stopped by its own path. */
    public void cancel() {
        cancelled.set(true);
    }

    public boolean cancelled() {
        return cancelled.get();
    }

    public double runningForS() {
        return Round.tenth((nanoClock.getAsLong() - startedNanos) / 1_000_000_000.0);
    }

    /** What the Health page shows: the age, the question, and which paths have answered. */
    public ComparisonRun state() {
        return new ComparisonRun(
                runningForS(),
                asked.mode(),
                asked.engines(),
                asked.sql(),
                asked.engines().stream().filter(results::containsKey).toList());
    }
}
