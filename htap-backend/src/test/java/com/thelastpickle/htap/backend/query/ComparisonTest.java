package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.engine.EngineFailed;
import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.engine.QueryRows;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * One question down several paths: the order, the failures, and the probe beside each.
 *
 * <p>Over paths of this test's own, since the real five each hold a driver or a native reader and
 * nothing above them could be judged through one. The baseline window is a millisecond here, because
 * the three seconds the application samples for are a choice about readings rather than about this.
 */
class ComparisonTest {

    private final FakePath cassandra = new FakePath("cassandra");
    private final FakePath presto = new FakePath("presto");
    private final FakePath spark = new FakePath("spark");
    private final FakePath cqlite = new FakePath("cqlite");

    private final QueryPaths paths =
            new QueryPaths(List.<QueryPath>of(cassandra, presto, spark, cqlite));
    private final SingleRunGate gate = new SingleRunGate();
    private final ProbeReads reads = new ProbeReads();
    private final Comparison comparison = new Comparison(
            paths, new QueryRunner(), reads, gate, Duration.ofMillis(1));

    /** The columns of a comparison must not move about with the order a caller named the paths in. */
    @Test
    void aWholeBodyRunAsksInTheDeclaredOrder() {
        Run run = comparison.begin("SELECT 1", List.of("cqlite", "cassandra"), RunMode.SEQUENTIAL,
                10, false);

        assertEquals(List.of("cassandra", "cqlite"), run.engines());
    }

    /** The stream's order is the order paths answer in, and the page sends its quickest first. */
    @Test
    void aStreamedRunKeepsTheCallersOrderAndIsSequential() {
        Run run = comparison.beginStreamed("SELECT 1", List.of("cqlite", "cassandra"), 10, false);

        assertEquals(List.of("cqlite", "cassandra"), run.engines());
        assertEquals(RunMode.SEQUENTIAL, run.asked().mode());
    }

    /** Parallel is refused a stream because overlapping paths have no individual timing to report. */
    @Test
    void aStreamedRunIsSequentialWhateverWasAsked() {
        Run run = comparison.beginStreamed("SELECT 1", null, 10, false);

        assertEquals(RunMode.SEQUENTIAL, run.asked().mode());
        assertEquals(List.of("cassandra", "presto", "spark", "cqlite"), run.engines());
    }

    @Test
    void aPathThisBackendDoesNotHaveIsRefusedBeforeTheGateIsTaken() {
        assertThrows(QueryPaths.Unknown.class, () -> comparison.begin(
                "SELECT 1", List.of("duckdb"), RunMode.SEQUENTIAL, 10, false));

        assertTrue(gate.running().isEmpty());
    }

    @Test
    void eachPathAnswersInTurnAndTheResultsAreKeptOnTheRun() {
        Run run = comparison.begin("SELECT 1", List.of("cassandra", "cqlite"), RunMode.SEQUENTIAL,
                10, false);
        List<String> answered = new ArrayList<>();

        comparison.each(run, (engine, result) -> answered.add(engine));

        assertEquals(List.of("cassandra", "cqlite"), answered);
        assertEquals(List.of("cassandra", "cqlite"), List.copyOf(run.results().keySet()).stream()
                .sorted()
                .toList());
        assertEquals(List.of("SELECT 1 /* cassandra */ LIMIT 10"), cassandra.asked());
    }

    /**
     * The point read is sampled while each path works, so a figure belongs to one path. A run with
     * nothing to read reports no impact at all rather than zeros, which would read as no cost.
     */
    @Test
    void aProbeRunsBesideEachPathWhenThereIsAnAssetToRead() {
        reads.subject = Optional.of("drone-1");
        Run run = comparison.begin("SELECT 1", List.of("cassandra", "cqlite"), RunMode.SEQUENTIAL,
                10, false);

        assertNotNull(comparison.baseline(run).orElseThrow());
        comparison.each(run, (engine, result) -> {});

        assertEquals(List.of("cassandra", "cqlite"),
                run.impacts().keySet().stream().sorted().toList());
        assertEquals("drone-1", run.subject().orElseThrow());
        assertEquals(List.of("drone-1", "drone-1", "drone-1"), reads.sampled());
    }

    @Test
    void aRunWithNoAssetToReadReportsNoImpact() {
        Run run = comparison.begin("SELECT 1", List.of("cassandra"), RunMode.SEQUENTIAL, 10, false);

        assertTrue(comparison.baseline(run).isEmpty());
        comparison.each(run, (engine, result) -> {});

        assertEquals(Map.<String, OltpImpact>of(), run.impacts());
    }

    /** A refusal is what the compare page shows for CQL and an aggregate, so it is a result. */
    @Test
    void aPathThatRefusesIsAResultRatherThanAFailedRun() {
        cassandra.refusing(new EngineFailed("Group by is not supported on a non-key column"));
        Run run = comparison.begin("SELECT 1", List.of("cassandra", "cqlite"), RunMode.SEQUENTIAL,
                10, false);

        comparison.each(run, (engine, result) -> {});

        PathResult refused = run.results().get("cassandra");
        assertEquals("Group by is not supported on a non-key column", refused.error());
        assertTrue(refused.available());
        assertNotNull(refused.queryTimeMs());
        assertNull(run.results().get("cqlite").error());
    }

    /** Reported rather than dropped: a leg that died silently would look like a path never asked. */
    @Test
    void aPathThatCouldNotBeReachedIsUnavailable() {
        cassandra.unreachable();
        Run run = comparison.begin("SELECT 1", List.of("cassandra"), RunMode.SEQUENTIAL, 10, false);

        comparison.each(run, (engine, result) -> {});

        PathResult gone = run.results().get("cassandra");
        assertEquals(false, gone.available());
        assertTrue(gone.error().contains("cassandra connection failed"), gone.error());
        assertEquals(List.of(), cassandra.asked());
    }

    /** The paths a cancelled run never reached stay absent, so they are not read as failures. */
    @Test
    void aCancelStopsTheRunBeforeItsNextPath() {
        Run run = comparison.begin("SELECT 1", List.of("cassandra", "presto", "cqlite"),
                RunMode.SEQUENTIAL, 10, false);

        comparison.each(run, (engine, result) -> run.cancel());

        assertEquals(List.of("cassandra"), List.copyOf(run.results().keySet()));
        assertTrue(run.cancelled());
    }

    @Test
    void aRunCancelledBeforeItStartsAsksNoPathAtAll() {
        Run run = comparison.begin("SELECT 1", List.of("cassandra"), RunMode.SEQUENTIAL, 10, false);
        run.cancel();

        comparison.each(run, (engine, result) -> {});

        assertEquals(Map.<String, PathResult>of(), run.results());
    }

    /**
     * Every path at once, with one probe over the whole window: while they overlap the cost belongs
     * to all of them and to none in particular, so there is nothing to attribute per path.
     *
     * <p>The overlap is recorded from inside each leg, and it has to be: a latch read after {@code
     * together} has joined every leg has reached zero whether the legs ran together or in turn, so
     * a sequential implementation would pass such a check. Each leg here waits for the other two,
     * so only a run where all three are in flight sees the latch fall.
     */
    @Test
    @Timeout(20)
    void aParallelRunAsksEveryPathAndMeasuresOneCombinedImpact() {
        reads.subject = Optional.of("drone-1");
        CountDownLatch started = new CountDownLatch(3);
        List<Boolean> sawTheOthers = new CopyOnWriteArrayList<>();
        for (FakePath path : List.of(cassandra, presto, cqlite)) {
            path.answering(() -> {
                started.countDown();
                sawTheOthers.add(await(started));
                return new QueryRows(List.of("n"), List.of(List.of(1)));
            });
        }
        Run run = comparison.begin("SELECT 1", List.of("cassandra", "presto", "cqlite"),
                RunMode.PARALLEL, 10, false);
        comparison.baseline(run);

        OltpImpact combined = comparison.together(run).orElseThrow();

        assertEquals(List.of(true, true, true), sawTheOthers, "the three paths did not overlap");
        assertEquals(3, run.results().size());
        assertNotNull(combined);
        assertEquals(Map.<String, OltpImpact>of(), run.impacts());
    }

    /**
     * An interrupt does not abandon the legs it is waiting on.
     *
     * <p>Returning early would have the route answer, and the gate admit the next comparison, while
     * three legs were still writing into the run and still holding their engine connections. The
     * interrupt is re-raised once every leg has finished, so a caller that wanted to stop still
     * learns that it was asked to.
     */
    @Test
    @Timeout(20)
    void anInterruptWaitsForEveryLegAndIsRaisedAfterwards() {
        Thread caller = Thread.currentThread();
        CountDownLatch started = new CountDownLatch(3);
        for (FakePath path : List.of(cassandra, presto, cqlite)) {
            path.answering(() -> {
                started.countDown();
                await(started);
                if (path == cassandra) {
                    // The first leg joined, so the interrupt lands inside that join rather than
                    // after the last one, and it sleeps so the join is genuinely still waiting.
                    caller.interrupt();
                    sleep(300);
                }
                return new QueryRows(List.of("n"), List.of(List.of(1)));
            });
        }
        Run run = comparison.begin("SELECT 1", List.of("cassandra", "presto", "cqlite"),
                RunMode.PARALLEL, 10, false);

        comparison.together(run);

        assertEquals(3, run.results().size());
        assertTrue(Thread.interrupted(), "the interrupt was swallowed");
    }

    /** Worked out at the start, because by the time a cancel asks, the path is busy with it. */
    @Test
    void whatTheSparkPathsWillSubmitIsWorkedOutWhenTheRunBegins() {
        Run run = comparison.begin("SELECT 1", List.of("cassandra", "spark"), RunMode.SEQUENTIAL,
                25, false);

        assertEquals(List.of("SELECT 1 /* spark */ LIMIT 25"), run.sparkStatements());
    }

    @Test
    void aRunWithNoSparkPathHasNoStatementsToRecognise() {
        Run run = comparison.begin("SELECT 1", List.of("cassandra"), RunMode.SEQUENTIAL, 10, false);

        assertEquals(List.of(), run.sparkStatements());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** True when the latch reached zero, which is a caller's evidence that the others got here. */
    private static boolean await(CountDownLatch latch) {
        try {
            return latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** The probe's reads, counted, with a subject a test decides on. */
    private static final class ProbeReads implements OltpSampler {

        private final List<String> taken = new CopyOnWriteArrayList<>();
        private Optional<String> subject = Optional.empty();

        @Override
        public Optional<String> subject() {
            return subject;
        }

        List<String> sampled() {
            return List.copyOf(taken);
        }

        @Override
        public OltpProbe sample(String entityId) {
            taken.add(entityId);
            return OltpProbe.start(() -> {}, Duration.ofMillis(1), System::nanoTime);
        }
    }
}
